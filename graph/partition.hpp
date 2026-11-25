#pragma once

#include "graph/graph.hpp"

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

ScalePlan plan_scale(const PartitionMap& current, Worker target_workers);
Generation next_generation(Generation current);

}  // namespace frontier
