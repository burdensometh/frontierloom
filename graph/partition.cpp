#include "graph/partition.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace frontier {
namespace {

std::uint64_t placement_hash(VertexId id) {
    auto value = id + 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

void validate_counts(std::size_t partitions, Worker workers) {
    if (partitions == 0 || partitions > max_partitions ||
        workers == 0 || workers > max_workers || workers > partitions) {
        throw std::invalid_argument("invalid partition or worker count");
    }
}

std::vector<std::size_t> quotas(std::size_t partitions, Worker workers) {
    std::vector<std::size_t> result(workers, partitions / workers);
    for (std::size_t index = 0; index < partitions % workers; ++index) {
        ++result[index];
    }
    return result;
}

}  // namespace

Shards::Shards(const Graph& graph, Partition count) {
    validate_counts(count, 1);
    partitions_.reserve(graph.size());
    positions_.reserve(graph.size());
    vertices_.resize(count);
    for (Vertex vertex = 0; vertex < graph.size(); ++vertex) {
        const auto partition = static_cast<Partition>(placement_hash(graph.external(vertex)) % count);
        auto& members = vertices_[partition];
        partitions_.push_back(partition);
        positions_.push_back(members.size());
        members.push_back(vertex);
    }
}

PartitionMap PartitionMap::initial(Partition partitions, Worker workers) {
    validate_counts(partitions, workers);
    PartitionMap result{1, workers, {}};
    result.owners.reserve(partitions);
    for (Partition part = 0; part < partitions; ++part) {
        result.owners.push_back(part % workers);
    }
    return result;
}

void PartitionMap::validate(Partition partitions) const {
    validate_counts(partitions, workers);
    if (generation == 0 || owners.size() != partitions) {
        throw std::invalid_argument("partition map has an invalid generation or size");
    }

    std::vector<std::size_t> counts(workers);
    for (const auto owner : owners) {
        if (owner >= workers) {
            throw std::invalid_argument("partition map names an unknown worker");
        }
        ++counts[owner];
    }
    if (counts != quotas(partitions, workers)) {
        throw std::invalid_argument("partition map does not satisfy the worker quotas");
    }
}

std::vector<Partition> PartitionMap::assigned(Worker worker) const {
    if (worker >= workers) {
        throw std::invalid_argument("assignment requested for an unknown worker");
    }
    std::vector<Partition> result;
    for (Partition part = 0; part < owners.size(); ++part) {
        if (owners[part] == worker) {
            result.push_back(part);
        }
    }
    return result;
}

ScalePlan plan_scale(const PartitionMap& current, Worker target_workers) {
    const auto count = static_cast<Partition>(current.owners.size());
    current.validate(count);
    validate_counts(count, target_workers);
    if (current.workers == target_workers) {
        return {current, {}};
    }

    constexpr auto unassigned = std::numeric_limits<Worker>::max();
    ScalePlan plan;
    plan.target = {next_generation(current.generation), target_workers,
                   std::vector<Worker>(count, unassigned)};
    auto remaining = quotas(count, target_workers);

    for (Partition part = 0; part < count; ++part) {
        const auto former = current.owner(part);
        if (former < target_workers && remaining[former] > 0) {
            plan.target.owners[part] = former;
            --remaining[former];
        }
    }

    Worker recipient = 0;
    for (Partition part = 0; part < count; ++part) {
        if (plan.target.owner(part) != unassigned) {
            continue;
        }
        while (recipient < target_workers && remaining[recipient] == 0) {
            ++recipient;
        }
        if (recipient == target_workers) {
            throw std::logic_error("placement quotas ran out before every partition was assigned");
        }
        plan.target.owners[part] = recipient;
        --remaining[recipient];
        plan.moves.push_back({part, current.owner(part), recipient});
    }
    plan.target.validate(count);
    return plan;
}

Generation next_generation(Generation current) {
    if (current == std::numeric_limits<Generation>::max()) {
        throw std::overflow_error("generation exhausted");
    }
    return current + 1;
}

}  // namespace frontier
