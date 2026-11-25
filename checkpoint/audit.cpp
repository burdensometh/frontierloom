#include "checkpoint/audit.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <unordered_set>

namespace frontier {
namespace {

std::vector<VertexState> dense_image(const Graph& graph, const Shards& shards, const Checkpoint& image) {
    std::vector<VertexState> dense(graph.size());
    for (Partition part = 0; part < shards.count(); ++part) {
        const auto members = shards.vertices(part);
        for (std::size_t index = 0; index < members.size(); ++index) {
            dense[members[index]] = image.states[part][index];
        }
    }
    return dense;
}

std::vector<std::optional<double>> pending_by_vertex(const Graph& graph, const Mailbox& mailbox) {
    std::vector<std::optional<double>> pending(graph.size());
    for (const auto& inbox : mailbox.inboxes) {
        for (const auto& entry : inbox) {
            pending[entry.target] = entry.value;
        }
    }
    return pending;
}

void audit_message(Audit& audit, const Graph& graph, Vertex target,
                   std::optional<double> persisted, std::optional<double> rebuilt) {
    if (persisted.has_value() != rebuilt.has_value()) {
        throw std::invalid_argument("audit found a frontier membership mismatch at vertex " +
                                    std::to_string(graph.external(target)));
    }
    if (!persisted) {
        return;
    }
    ++audit.combined;
    const auto error = std::abs(*persisted - *rebuilt);
    audit.max_mailbox_error = std::max(audit.max_mailbox_error, error);
    if (error != 0) {
        throw std::invalid_argument("audit found a frontier value mismatch at vertex " +
                                    std::to_string(graph.external(target)));
    }
}

void audit_bfs(Audit& audit, const Graph& graph, const Checkpoint& image,
               std::span<const VertexState> states, std::span<const std::optional<double>> pending) {
    const auto epoch = image.last->epoch;
    const auto root = graph.locate(image.program.source);
    // One merged message exists per unique (emitting layer, destination).
    // Cumulative statistics must match even when several parents share a child.
    std::unordered_set<std::uint64_t> transfers;
    std::uint64_t emitted{};
    std::uint64_t consumed{1};
    std::uint64_t previous_consumed{};
    for (Vertex target = 0; target < graph.size(); ++target) {
        const auto outgoing = graph.outgoing(target);
        if (states[target].distance == epoch) {
            ++audit.previous_sources;
            audit.raw += outgoing.size();
        }
        if (states[target].distance != unreachable) {
            emitted += outgoing.size();
        }

        const auto incoming = graph.incoming(target);
        audit.checked_edges += incoming.size();
        std::uint32_t candidate = target == root ? 0 : unreachable;
        bool final_message = false;
        bool consumed_here = false;
        for (const auto source : incoming) {
            const auto distance = states[source].distance;
            if (distance == unreachable) {
                continue;
            }
            const auto next = static_cast<std::uint64_t>(distance) + 1;
            const auto key = (next << 32) | target;
            if (transfers.insert(key).second && next < image.next_epoch) {
                ++consumed;
            }
            final_message = final_message || distance == epoch;
            consumed_here = consumed_here || next == epoch;
            if (next < image.next_epoch) {
                candidate = std::min(candidate, static_cast<std::uint32_t>(next));
            }
        }
        if (candidate != states[target].distance) {
            throw std::invalid_argument("audit found a BFS distance without a committed path at vertex " +
                                        std::to_string(graph.external(target)));
        }
        if (consumed_here || (epoch == 0 && target == root)) {
            ++previous_consumed;
        }
        audit_message(audit, graph, target, pending[target],
                      final_message ? std::optional<double>(static_cast<double>(epoch + 1)) : std::nullopt);
    }

    if (image.totals.emitted != emitted || image.totals.combined != transfers.size() ||
        image.totals.consumed != consumed || image.last->stats.active != audit.previous_sources ||
        image.last->stats.consumed != previous_consumed) {
        throw std::invalid_argument("audit found inconsistent BFS barrier or cumulative counts");
    }
}

void audit_rank(Audit& audit, const Graph& graph, const Checkpoint& image,
                std::span<const VertexState> states, std::span<const std::optional<double>> pending) {
    std::vector<std::size_t> degrees(graph.size());
    for (Vertex vertex = 0; vertex < graph.size(); ++vertex) {
        degrees[vertex] = graph.outgoing(vertex).size();
        if (degrees[vertex] != 0) {
            ++audit.previous_sources;
        }
    }
    for (Vertex target = 0; target < graph.size(); ++target) {
        const auto incoming = graph.incoming(target);
        audit.raw += incoming.size();
        audit.checked_edges += incoming.size();
        Sum contributions;
        for (const auto source : incoming) {
            contributions.add(states[source].rank / static_cast<double>(degrees[source]));
        }
        audit_message(audit, graph, target, pending[target],
                      incoming.empty() ? std::nullopt : std::optional<double>(contributions.value()));
    }
    const auto steps = image.next_epoch;
    if (image.totals.active != steps * graph.size() || image.totals.emitted != steps * audit.raw ||
        image.totals.consumed != (steps - 1) * audit.combined ||
        image.totals.combined != steps * audit.combined ||
        image.last->stats.consumed != (steps == 1 ? 0 : audit.combined)) {
        throw std::invalid_argument("audit found inconsistent PageRank barrier or cumulative counts");
    }
}

}  // namespace

Audit audit_checkpoint(const Graph& graph, const Checkpoint& checkpoint) {
    const Shards shards(graph, static_cast<Partition>(checkpoint.placement.owners.size()));
    checkpoint.validate(graph, shards);
    Audit audit;
    audit.states = graph.size();
    if (!checkpoint.last) {
        audit.raw = checkpoint.mailbox.raw_count;
        audit.combined = checkpoint.mailbox.combined_count();
        return audit;
    }

    const auto states = dense_image(graph, shards, checkpoint);
    const auto pending = pending_by_vertex(graph, checkpoint.mailbox);
    if (checkpoint.program.algorithm == Algorithm::bfs) {
        audit_bfs(audit, graph, checkpoint, states, pending);
    } else {
        audit_rank(audit, graph, checkpoint, states, pending);
    }
    if (audit.raw != checkpoint.mailbox.raw_count || audit.combined != checkpoint.mailbox.combined_count()) {
        throw std::invalid_argument("audit found a frontier count mismatch");
    }
    return audit;
}

}  // namespace frontier
