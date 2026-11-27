#include "kernel/program.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <utility>

namespace frontier {
namespace {

// Bounds the outbound buffer: messages accumulate only until the fixed capacity
// is reached, then the full chunk leaves the worker and the buffer restarts.
// Vertex degree therefore changes the number of chunks, never their size.
class SourceSink {
public:
    SourceSink(std::size_t capacity, const EmitChunk& emit) : capacity_(capacity), emit_(emit) {
        buffer_.reserve(capacity_);
    }

    void source(Vertex vertex) { source_ = vertex; }

    void add(Vertex target, double value) {
        buffer_.push_back({source_, target, value});
        ++emitted_;
        if (buffer_.size() == capacity_) {
            emit_(sequence_, false, buffer_);
            ++sequence_;
            buffer_.clear();
        }
    }

    // Emits the trailing chunk (even when empty) flagged as the stream end, so
    // every partition produces a closed, non-empty chunk stream.
    void finish() {
        emit_(sequence_, true, buffer_);
        ++sequence_;
        buffer_.clear();
    }

    [[nodiscard]] std::uint32_t chunks() const { return sequence_; }
    [[nodiscard]] std::uint64_t emitted() const { return emitted_; }

private:
    Vertex source_{};
    std::size_t capacity_;
    const EmitChunk& emit_;
    std::vector<Message> buffer_;
    std::uint32_t sequence_{};
    std::uint64_t emitted_{};
};

}  // namespace

const char* name(Algorithm algorithm) {
    switch (algorithm) {
    case Algorithm::bfs:
        return "bfs";
    case Algorithm::pagerank:
        return "pagerank";
    }
    throw std::invalid_argument("unknown graph algorithm");
}

void Program::validate(const Graph& graph) const {
    if (limit == 0 || limit > max_steps) {
        throw std::invalid_argument("step limit outside supported range");
    }
    if (!std::isfinite(damping) || !std::isfinite(tolerance) ||
        damping <= 0 || damping >= 1 || tolerance <= 0 || tolerance > 1) {
        throw std::invalid_argument("invalid PageRank damping or tolerance");
    }
    switch (algorithm) {
    case Algorithm::bfs:
        (void)graph.locate(source);
        if (damping != 0.85 || tolerance != 1e-10) {
            throw std::invalid_argument("BFS does not use PageRank options");
        }
        break;
    case Algorithm::pagerank:
        if (source != 0) {
            throw std::invalid_argument("PageRank does not use a BFS source");
        }
        break;
    default:
        throw std::invalid_argument("unsupported graph algorithm");
    }
}

std::vector<std::vector<VertexState>>
initial_state(const Graph& graph, const Shards& shards, const Program& program) {
    program.validate(graph);
    VertexState initial;
    if (program.algorithm == Algorithm::pagerank) {
        initial.rank = 1.0 / static_cast<double>(graph.size());
    }
    std::vector<std::vector<VertexState>> states(shards.count());
    for (Partition part = 0; part < shards.count(); ++part) {
        states[part].assign(shards.vertices(part).size(), initial);
    }
    return states;
}

Mailbox initial_mailbox(const Graph& graph, const Shards& shards, const Program& program) {
    Mailbox initial{0, 0, std::vector<std::vector<InboxEntry>>(shards.count())};
    if (program.algorithm == Algorithm::bfs) {
        const auto source = graph.locate(program.source);
        initial.inboxes[shards.shard(source)].push_back({source, 0.0});
        initial.raw_count = 1;
    }
    return initial;
}

double initial_dangling(const Graph& graph, const Program& program) {
    if (program.algorithm == Algorithm::bfs) {
        return 0.0;
    }
    std::size_t empty{};
    for (Vertex vertex = 0; vertex < graph.size(); ++vertex) {
        empty += graph.outgoing(vertex).empty() ? 1U : 0U;
    }
    return static_cast<double>(empty) / static_cast<double>(graph.size());
}

void validate_state(Algorithm algorithm, const VertexState& state, std::size_t vertices) {
    if (algorithm == Algorithm::bfs) {
        if (state.rank != 0.0 || std::signbit(state.rank) ||
            (state.distance != unreachable && state.distance >= vertices)) {
            throw std::invalid_argument("invalid BFS vertex state");
        }
        return;
    }
    if (state.distance != unreachable || !valid_message(Algorithm::pagerank, state.rank) ||
        state.rank == 0.0) {
        throw std::invalid_argument("invalid PageRank vertex state");
    }
}

void validate_step_input(const Graph& graph, const Shards& shards,
                         const Program& program, const StepInput& input) {
    const auto members = shards.vertices(input.partition);
    if (input.states.size() != members.size() || input.inbox.size() > members.size() ||
        input.epoch > static_cast<Epoch>(max_steps) + 1 || !valid_message(Algorithm::pagerank, input.dangling) ||
        (program.algorithm == Algorithm::bfs && input.dangling != 0.0)) {
        throw std::invalid_argument("partition work has invalid bounds");
    }
    for (const auto& state : input.states) {
        validate_state(program.algorithm, state, graph.size());
    }

    auto member = members.begin();
    for (const auto& entry : input.inbox) {
        member = std::lower_bound(member, members.end(), entry.target);
        if (member == members.end() || *member != entry.target ||
            !valid_message(program.algorithm, entry.value) ||
            (program.algorithm == Algorithm::bfs && entry.value != static_cast<double>(input.epoch))) {
            throw std::invalid_argument("partition work has an invalid inbox entry");
        }
        ++member;
    }
    if (program.algorithm == Algorithm::pagerank && input.epoch == 0 && !input.inbox.empty()) {
        throw std::invalid_argument("PageRank epoch zero starts with an empty inbox");
    }
}

StreamedPartition settle_partition(const Graph& graph, const Shards& shards,
                                   const Program& program, const WorkerAttempt& attempt,
                                   const StepInput& input) {
    (void)attempt;
    validate_step_input(graph, shards, program, input);
    StreamedPartition streamed;
    streamed.states = input.states;
    streamed.stats.consumed = input.inbox.size();
    const auto members = shards.vertices(input.partition);

    // Settle every vertex state and the step statistics. No scatter buffer is
    // allocated here, so this phase cannot grow with a vertex degree.
    switch (program.algorithm) {
    case Algorithm::bfs:
        for (const auto& message : input.inbox) {
            const auto member = std::lower_bound(members.begin(), members.end(), message.target);
            auto& state = streamed.states[static_cast<std::size_t>(member - members.begin())];
            const auto distance = static_cast<std::uint32_t>(message.value);
            if (state.distance <= distance) {
                continue;
            }
            state.distance = distance;
            ++streamed.stats.active;
        }
        break;
    case Algorithm::pagerank: {
        const auto count = static_cast<double>(graph.size());
        const auto teleport = (1.0 - program.damping) / count;
        const auto redistributed = input.dangling / count;
        auto next_message = input.inbox.begin();
        Sum dangling;
        Sum residual;
        Sum rank_mass;
        for (std::size_t index = 0; index < members.size(); ++index) {
            const auto vertex = members[index];
            const auto previous = streamed.states[index].rank;
            double rank = previous;
            if (input.epoch > 0) {
                double received{};
                if (next_message != input.inbox.end() && next_message->target == vertex) {
                    received = next_message->value;
                    ++next_message;
                }
                rank = teleport + program.damping * (received + redistributed);
                residual.add(std::abs(rank - previous));
            }
            streamed.states[index].rank = rank;
            rank_mass.add(rank);
            if (graph.outgoing(vertex).empty()) {
                dangling.add(rank);
            }
        }
        streamed.stats.active = members.size();
        streamed.stats.dangling = dangling.value();
        streamed.stats.residual = residual.value();
        streamed.stats.rank_mass = rank_mass.value();
        break;
    }
    default:
        throw std::invalid_argument("unsupported graph algorithm");
    }
    return streamed;
}

StreamedPartition scatter_partition(const Graph& graph, const Shards& shards,
                                    const Program& program, const StepInput& input,
                                    std::span<const VertexState> settled_states,
                                    std::size_t chunk_capacity, const EmitChunk& emit) {
    if (chunk_capacity == 0) {
        throw std::invalid_argument("message chunk capacity must be positive");
    }
    if (settled_states.size() != shards.vertices(input.partition).size()) {
        throw std::invalid_argument("scatter received a mismatched settled state image");
    }
    const auto members = shards.vertices(input.partition);
    StreamedPartition streamed;
    streamed.stats.consumed = input.inbox.size();

    // Walk the settled vertices once and push every legal edge through the
    // fixed-capacity sink. The buffered chunk is the only outbound storage, so
    // peak buffer use stays at chunk_capacity even for a million-edge source.
    SourceSink sink(chunk_capacity, emit);
    switch (program.algorithm) {
    case Algorithm::bfs:
        for (const auto& message : input.inbox) {
            const auto member = std::lower_bound(members.begin(), members.end(), message.target);
            const auto index = static_cast<std::size_t>(member - members.begin());
            const auto distance = settled_states[index].distance;
            // A state only changes once per step (first, smallest inbox value),
            // which is exactly when the original kernel scattered.
            if (distance != static_cast<std::uint32_t>(message.value)) {
                continue;
            }
            const auto next = static_cast<double>(distance) + 1.0;
            sink.source(message.target);
            for (const auto target : graph.outgoing(message.target)) {
                sink.add(target, next);
            }
        }
        break;
    case Algorithm::pagerank:
        for (std::size_t index = 0; index < members.size(); ++index) {
            const auto vertex = members[index];
            const auto degree = graph.outgoing(vertex).size();
            if (degree == 0) {
                continue;
            }
            const auto share = settled_states[index].rank / static_cast<double>(degree);
            sink.source(vertex);
            for (const auto target : graph.outgoing(vertex)) {
                sink.add(target, share);
            }
        }
        break;
    default:
        throw std::invalid_argument("unsupported graph algorithm");
    }

    sink.finish();
    streamed.chunks = sink.chunks();
    streamed.stats.emitted = sink.emitted();
    streamed.stats.active = members.size();
    return streamed;
}

StreamedPartition run_partition_streamed(const Graph& graph, const Shards& shards,
                                         const Program& program, const WorkerAttempt& attempt,
                                         const StepInput& input, std::size_t chunk_capacity,
                                         const EmitChunk& emit) {
    auto streamed = settle_partition(graph, shards, program, attempt, input);
    const auto scattered = scatter_partition(graph, shards, program, input, streamed.states,
                                             chunk_capacity, emit);
    streamed.chunks = scattered.chunks;
    streamed.stats.emitted = scattered.stats.emitted;
    return streamed;
}

PartitionResult run_partition(const Graph& graph, const Shards& shards, const Program& program,
                              const WorkerAttempt& attempt, const StepInput& input) {
    PartitionResult result;
    result.batch = {input.epoch, attempt, input.partition, {}};
    auto& messages = result.batch.messages;
    const auto streamed = run_partition_streamed(
        graph, shards, program, attempt, input, message_chunk_capacity,
        [&](std::uint32_t, bool, MessageChunk chunk) {
            messages.insert(messages.end(), chunk.begin(), chunk.end());
        });
    result.states = std::move(streamed.states);
    result.stats = std::move(streamed.stats);
    result.stats.emitted = messages.size();
    return result;
}

}  // namespace frontier
