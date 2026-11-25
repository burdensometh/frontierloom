#include "kernel/contract.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace frontier {
namespace {

class Emissions {
public:
    explicit Emissions(std::span<const Message> messages) : remaining_(messages) {}

    void expect(Vertex source, std::span<const Vertex> neighbors, double value) {
        if (neighbors.size() > remaining_.size()) {
            throw std::invalid_argument("partition result omitted part of a vertex scatter");
        }
        for (std::size_t index = 0; index < neighbors.size(); ++index) {
            const auto& message = remaining_[index];
            if (message.source != source || message.target != neighbors[index] || message.value != value) {
                throw std::invalid_argument("partition message does not match the resulting vertex state");
            }
        }
        remaining_ = remaining_.subspan(neighbors.size());
    }

    void finish() const {
        if (!remaining_.empty()) {
            throw std::invalid_argument("partition sent extra messages");
        }
    }

private:
    std::span<const Message> remaining_;
};

class InboxCursor {
public:
    explicit InboxCursor(std::span<const InboxEntry> entries) : remaining_(entries) {}

    const InboxEntry* consume(Vertex vertex) {
        if (!remaining_.empty() && remaining_.front().target == vertex) {
            const auto* entry = remaining_.data();
            remaining_ = remaining_.subspan(1);
            return entry;
        }
        return nullptr;
    }

    void finish() const {
        if (!remaining_.empty()) {
            throw std::invalid_argument("partition output did not account for every inbox entry");
        }
    }

private:
    std::span<const InboxEntry> remaining_;
};

StepStats inspect_bfs(const Graph& graph, std::span<const Vertex> vertices, const StepInput& input,
                      const PartitionResult& output, Emissions& emissions) {
    InboxCursor inbox(input.inbox);
    StepStats measured;
    for (std::size_t index = 0; index < vertices.size(); ++index) {
        const auto vertex = vertices[index];
        const auto previous = input.states[index].distance;
        const auto* incoming = inbox.consume(vertex);
        auto expected = previous;
        if (incoming != nullptr) {
            ++measured.consumed;
            expected = std::min(expected, static_cast<std::uint32_t>(incoming->value));
        }
        const auto& after = output.states[index];
        validate_state(Algorithm::bfs, after, graph.size());
        if (after.distance != expected) {
            throw std::invalid_argument("BFS output did not commit the minimum incoming distance");
        }
        if (after.distance < previous) {
            ++measured.active;
            const auto neighbors = graph.outgoing(vertex);
            emissions.expect(vertex, neighbors, static_cast<double>(after.distance) + 1);
            measured.emitted += neighbors.size();
        }
    }
    inbox.finish();
    return measured;
}

StepStats inspect_rank(const Graph& graph, std::span<const Vertex> vertices, const StepInput& input,
                       const PartitionResult& output, Emissions& emissions) {
    StepStats measured;
    measured.active = vertices.size();
    measured.consumed = input.inbox.size();
    Sum residual;
    Sum mass;
    Sum dangling;
    for (std::size_t index = 0; index < vertices.size(); ++index) {
        const auto vertex = vertices[index];
        const auto& state = output.states[index];
        const auto previous = input.states[index].rank;
        validate_state(Algorithm::pagerank, state, graph.size());
        if (input.epoch == 0 && previous != state.rank) {
            throw std::invalid_argument("PageRank scatter changed epoch-zero state");
        }
        residual.add(std::abs(state.rank - previous));
        mass.add(state.rank);
        const auto neighbors = graph.outgoing(vertex);
        if (neighbors.empty()) {
            dangling.add(state.rank);
        } else {
            emissions.expect(vertex, neighbors, state.rank / static_cast<double>(neighbors.size()));
            measured.emitted += neighbors.size();
        }
    }
    measured.dangling = dangling.value();
    measured.rank_mass = mass.value();
    measured.residual = residual.value();
    return measured;
}

}  // namespace

void validate_partition_output(const Graph& graph, const Shards& shards,
                               const Program& program, const StepInput& input,
                               const PartitionResult& output) {
    const auto vertices = shards.vertices(input.partition);
    if (input.states.size() != vertices.size() || output.states.size() != vertices.size() ||
        input.partition != output.batch.source_partition || input.epoch != output.batch.epoch) {
        throw std::invalid_argument("worker result shape does not match its input");
    }
    Emissions emissions(output.batch.messages);
    const auto stats = program.algorithm == Algorithm::bfs ?
        inspect_bfs(graph, vertices, input, output, emissions) :
        inspect_rank(graph, vertices, input, output, emissions);

    emissions.finish();
    if (stats != output.stats) {
        throw std::invalid_argument("partition reduction does not describe its state and messages");
    }
}

}  // namespace frontier
