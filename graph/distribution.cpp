#include "graph/distribution.hpp"

#include <algorithm>
#include <numeric>
#include <stdexcept>

namespace frontier {
namespace {

void accumulate(PartitionLoad& total, const PartitionLoad& part) {
    total.vertices += part.vertices;
    total.outgoing += part.outgoing;
    total.incoming += part.incoming;
    total.sent_remote += part.sent_remote;
    total.received_remote += part.received_remote;
    total.dangling += part.dangling;
    total.max_out_degree = std::max(total.max_out_degree, part.max_out_degree);
}

void validate_distribution(const Graph& graph, const Distribution& report) {
    const auto totals = std::accumulate(report.workers.begin(), report.workers.end(), PartitionLoad{},
        [](PartitionLoad combined, const PartitionLoad& item) {
            accumulate(combined, item);
            return combined;
        });
    if (totals.vertices != graph.size() || totals.outgoing != graph.edge_count() ||
        totals.incoming != graph.edge_count() || totals.sent_remote != report.cross_worker ||
        totals.received_remote != report.cross_worker || report.cross_worker > report.cross_partition) {
        throw std::logic_error("partition distribution does not account for every vertex and edge");
    }
}

}  // namespace

Distribution measure_distribution(const Graph& graph, const Shards& shards,
                                  const PartitionMap& placement) {
    placement.validate(shards.count());
    Distribution report;
    report.partitions.resize(shards.count());
    report.workers.resize(placement.workers);
    const auto workers = static_cast<std::size_t>(placement.workers);
    std::vector<std::uint64_t> matrix(workers * workers);

    for (Partition part = 0; part < shards.count(); ++part) {
        const auto owner = placement.owner(part);
        auto& source_load = report.partitions[part];
        const auto vertices = shards.vertices(part);
        source_load.vertices = vertices.size();
        for (const auto vertex : vertices) {
            const auto outgoing = graph.outgoing(vertex);
            source_load.outgoing += outgoing.size();
            source_load.max_out_degree = std::max<std::uint64_t>(source_load.max_out_degree, outgoing.size());
            if (outgoing.empty()) {
                ++source_load.dangling;
            }
            for (const auto target : outgoing) {
                const auto destination = shards.shard(target);
                const auto recipient = placement.owner(destination);
                auto& target_load = report.partitions[destination];
                ++target_load.incoming;
                if (part != destination) {
                    ++report.cross_partition;
                }
                if (owner != recipient) {
                    ++report.cross_worker;
                    ++source_load.sent_remote;
                    ++target_load.received_remote;
                    ++matrix[owner * workers + recipient];
                }
            }
        }
    }

    for (Partition part = 0; part < shards.count(); ++part) {
        accumulate(report.workers[placement.owner(part)], report.partitions[part]);
    }
    for (Worker source = 0; source < placement.workers; ++source) {
        for (Worker target = 0; target < placement.workers; ++target) {
            if (const auto edges = matrix[source * workers + target]; edges != 0) {
                report.traffic.push_back({source, target, edges});
            }
        }
    }
    validate_distribution(graph, report);
    return report;
}

}  // namespace frontier
