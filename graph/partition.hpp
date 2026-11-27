#pragma once

#include "graph/graph.hpp"

#include <optional>

namespace frontier {

inline constexpr Partition max_partitions = 128;
inline constexpr Worker max_workers = 32;

class Shards {
public:
    Shards(const Graph& graph, Partition count);

    [[nodiscard]] Partition count() const { return static_cast<Partition>(vertices_.size()); }
    [[nodiscard]] Partition shard(Vertex vertex) const { return partitions_.at(vertex); }
    [[nodiscard]] std::size_t position(Vertex vertex) const { return positions_.at(vertex); }
    [[nodiscard]] std::span<const Vertex> vertices(Partition shard) const { return vertices_.at(shard); }

private:
    std::vector<Partition> partitions_;
    std::vector<std::size_t> positions_;
    std::vector<std::vector<Vertex>> vertices_;
};

struct PartitionMap {
    Generation generation{};
    Worker workers{};
    std::vector<Worker> owners;

    static PartitionMap initial(Partition partitions, Worker workers);
    void validate(Partition partitions) const;
    [[nodiscard]] std::vector<Partition> assigned(Worker worker) const;
    [[nodiscard]] Worker owner(Partition partition) const { return owners.at(partition); }
    bool operator==(const PartitionMap&) const = default;
};

struct PartitionMove {
    Partition partition{};
    Worker previous{};
    Worker next{};
    bool operator==(const PartitionMove&) const = default;
};

struct ScalePlan {
    PartitionMap target;
    std::vector<PartitionMove> moves;
};

// Plans a deterministic worker count transition. Every partition keeps a single
// owner; ownership is only moved when the target placement cannot be satisfied
// inside the quota ladder.
ScalePlan plan_scale(const PartitionMap& current, Worker target_workers);

// Plans a transition to exactly `target_workers` such that every worker owns at
// most `edge_budget` outgoing edges. A worker's predicted workload is the sum of
// the outgoing edges of the shards it owns: each such edge is scanned and
// scattered once per superstep, whether the message stays local or crosses a
// worker. Ownership moves only when the budget or quotas require it, and equal
// scores resolve by partition then worker index so the placement is stable for
// the same input. Returns std::nullopt when no placement at the target worker
// count satisfies the quotas and budget; callers must then refuse to install a
// plan instead of falling back to an over-budget placement.
[[nodiscard]] std::optional<ScalePlan>
plan_scale_budgeted(const Shards& shards, const Graph& graph, const PartitionMap& current,
                    Worker target_workers, std::uint64_t edge_budget);

// Sums each shard's outgoing edges onto the worker that owns it. The result has
// one entry per worker and is the predicted per-worker scatter workload the
// budget planner bounds.
[[nodiscard]] std::vector<std::uint64_t>
worker_edge_loads(const Shards& shards, const Graph& graph, const PartitionMap& placement);

Generation next_generation(Generation current);

}  // namespace frontier
