#include "kernel/program.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace frontier {
namespace {

void add_messages(const Graph& graph, Vertex source, double value, MessageBatch& batch) {
    for (const auto target : graph.outgoing(source)) {
        batch.messages.push_back({source, target, value});
    }
}

void run_bfs(const Graph& graph, std::span<const Vertex> members, const StepInput& input,
             PartitionResult& result) {
    for (const auto& message : input.inbox) {
        const auto member = std::lower_bound(members.begin(), members.end(), message.target);
        auto& state = result.states[static_cast<std::size_t>(member - members.begin())];
        const auto distance = static_cast<std::uint32_t>(message.value);
        if (state.distance <= distance) {
            continue;
        }
        state.distance = distance;
        ++result.stats.active;
        add_messages(graph, message.target, static_cast<double>(distance) + 1.0, result.batch);
    }
}

void run_rank(const Graph& graph, std::span<const Vertex> members, const Program& program,
              const StepInput& input, PartitionResult& result) {
    const auto count = static_cast<double>(graph.size());
    const auto teleport = (1.0 - program.damping) / count;
    const auto redistributed = input.dangling / count;
    auto next_message = input.inbox.begin();
    Sum dangling;
    Sum residual;
    Sum rank_mass;
    for (std::size_t index = 0; index < members.size(); ++index) {
        const auto source = members[index];
        const auto previous = result.states[index].rank;
        double rank = previous;
        if (input.epoch > 0) {
            double received{};
            if (next_message != input.inbox.end() && next_message->target == source) {
                received = next_message->value;
                ++next_message;
            }
            rank = teleport + program.damping * (received + redistributed);
            residual.add(std::abs(rank - previous));
        }

        result.states[index].rank = rank;
        rank_mass.add(rank);
        const auto degree = graph.outgoing(source).size();
        if (degree == 0) {
            dangling.add(rank);
        } else {
            add_messages(graph, source, rank / static_cast<double>(degree), result.batch);
        }
    }
    result.stats.active = members.size();
    result.stats.dangling = dangling.value();
    result.stats.residual = residual.value();
    result.stats.rank_mass = rank_mass.value();
}

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

PartitionResult run_partition(const Graph& graph, const Shards& shards, const Program& program,
                              const WorkerAttempt& attempt, const StepInput& input) {
    validate_step_input(graph, shards, program, input);
    PartitionResult result;
    result.states = input.states;
    result.batch = {input.epoch, attempt, input.partition, {}};
    result.stats.consumed = input.inbox.size();
    const auto members = shards.vertices(input.partition);
    switch (program.algorithm) {
    case Algorithm::bfs:
        run_bfs(graph, members, input, result);
        break;
    case Algorithm::pagerank:
        run_rank(graph, members, program, input, result);
        break;
    }
    result.stats.emitted = result.batch.messages.size();
    return result;
}

}  // namespace frontier
