#pragma once

#include "graph/partition.hpp"

namespace frontier {

struct PartitionLoad {
    std::uint64_t vertices{};
    std::uint64_t outgoing{};
    std::uint64_t incoming{};
    std::uint64_t sent_remote{};
    std::uint64_t received_remote{};
    std::uint64_t dangling{};
    std::uint64_t max_out_degree{};
};

struct WorkerTraffic {
    Worker from{};
    Worker to{};
    std::uint64_t edges{};
};

struct Distribution {
    std::vector<PartitionLoad> partitions;
    std::vector<PartitionLoad> workers;
    std::vector<WorkerTraffic> traffic;
    std::uint64_t cross_partition{};
    std::uint64_t cross_worker{};
};

[[nodiscard]] Distribution measure_distribution(const Graph& graph, const Shards& shards,
                                                const PartitionMap& placement);

}  // namespace frontier
