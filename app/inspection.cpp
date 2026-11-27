#include "app/inspection.hpp"
#include "checkpoint/audit.hpp"
#include "graph/distribution.hpp"

#include <algorithm>
#include <iomanip>
#include <ostream>

namespace frontier {
namespace {

void print_load(std::ostream& out, const PartitionLoad& load) {
    out << " vertices=" << load.vertices << " incoming=" << load.incoming
        << " outgoing=" << load.outgoing << " dangling=" << load.dangling
        << " sent_remote=" << load.sent_remote << " received_remote=" << load.received_remote
        << " max_out_degree=" << load.max_out_degree;
}

void print_traffic(std::ostream& out, const Distribution& distribution, Generation topology) {
    for (const auto& row : distribution.traffic) {
        out << "traffic topology=" << topology << " from=" << row.from
            << " to=" << row.to << " edges=" << row.edges << '\n';
    }
}

bool active(const VertexState& state, Algorithm algorithm, Epoch next_epoch) {
    if (algorithm == Algorithm::bfs) {
        return next_epoch > 0 && state.distance == next_epoch - 1;
    }
    return next_epoch > 0;
}

struct StateSummary {
    std::size_t reached{};
    std::size_t recent{};
    double rank_mass{};
};

StateSummary summarize(std::span<const VertexState> states, Algorithm algorithm, Epoch next_epoch) {
    StateSummary result;
    Sum mass;
    for (const auto& state : states) {
        if (state.distance != unreachable) {
            ++result.reached;
        }
        if (active(state, algorithm, next_epoch)) {
            ++result.recent;
        }
        mass.add(state.rank);
    }
    result.rank_mass = mass.value();
    return result;
}

void print_worker(std::ostream& out, Worker worker, const PartitionMap& placement,
                  const Checkpoint& checkpoint, const Distribution& distribution) {
    const auto partitions = placement.assigned(worker);
    std::uint64_t pending{};
    for (const auto part : partitions) {
        pending += checkpoint.mailbox.inboxes[part].size();
    }
    out << "worker=" << worker << " topology=" << placement.generation
        << " partitions=" << partitions.size() << " pending=" << pending;
    print_load(out, distribution.workers[worker]);
    out << '\n';
}

}  // namespace

void print_checkpoint_detail(std::ostream& out, const Graph& graph, const Checkpoint& checkpoint) {
    const auto& placement = checkpoint.placement;
    const Shards shards(graph, static_cast<Partition>(placement.owners.size()));
    const auto distribution = measure_distribution(graph, shards, placement);
    out << std::setprecision(17) << "placement topology=" << placement.generation
        << " graph_revision=" << graph.revision() << " graph_identity=" << graph.identity()
        << " cross_partition=" << distribution.cross_partition
        << " cross_worker=" << distribution.cross_worker << '\n';

    const auto dangling = initial_dangling(graph, checkpoint.program);
    for (Partition part = 0; part < shards.count(); ++part) {
        const auto summary = summarize(checkpoint.states[part], checkpoint.program.algorithm, checkpoint.next_epoch);
        const auto image = checkpoint.work(part, dangling);
        out << "partition=" << part << " owner=" << placement.owner(part)
            << " reached=" << summary.reached << " active_last=" << summary.recent
            << " rank_mass=" << summary.rank_mass << " pending=" << image.inbox.size()
            << " image=" << work_digest(image);
        print_load(out, distribution.partitions[part]);
        out << '\n';
    }
    for (Worker worker = 0; worker < placement.workers; ++worker) {
        print_worker(out, worker, placement, checkpoint, distribution);
    }
    print_traffic(out, distribution, placement.generation);
}

void print_scale_plan(std::ostream& out, const Graph& graph, const Checkpoint& checkpoint, Worker target_id) {
    const auto& current = checkpoint.placement;
    const Shards shards(graph, static_cast<Partition>(current.owners.size()));
    const auto plan = plan_scale(current, target_id);
    const auto previous = measure_distribution(graph, shards, current);
    const auto next = measure_distribution(graph, shards, plan.target);
    const auto dangling = initial_dangling(graph, checkpoint.program);
    out << "plan epoch=" << checkpoint.next_epoch
        << " from_topology=" << current.generation << " to_topology=" << plan.target.generation
        << " from_workers=" << current.workers << " to_workers=" << plan.target.workers
        << " moved=" << plan.moves.size() << " edges_from_cross_worker=" << previous.cross_worker
        << " edges_to_cross_worker=" << next.cross_worker << '\n';
    for (const auto& move : plan.moves) {
        const auto image = checkpoint.work(move.partition, dangling);
        out << "move partition=" << move.partition << " from=" << move.previous << " to=" << move.next
            << " states=" << image.states.size() << " pending=" << image.inbox.size()
            << " image=" << work_digest(image) << '\n';
    }
    for (Worker worker = 0; worker < plan.target.workers; ++worker) {
        print_worker(out, worker, plan.target, checkpoint, next);
    }
    print_traffic(out, next, plan.target.generation);
}

void print_scale_plan(std::ostream& out, const Graph& graph, const Checkpoint& checkpoint,
                      Worker target_id, std::uint64_t edge_budget) {
    const auto& current = checkpoint.placement;
    const Shards shards(graph, static_cast<Partition>(current.owners.size()));
    const auto previous = measure_distribution(graph, shards, current);
    const auto loads_before = worker_edge_loads(shards, graph, current);
    const auto plan = plan_scale_budgeted(shards, graph, current, target_id, edge_budget);
    if (!plan) {
        out << "plan epoch=" << checkpoint.next_epoch
            << " from_topology=" << current.generation << " from_workers=" << current.workers
            << " to_workers=" << target_id << " edge_budget=" << edge_budget
            << " peak_from=" << *std::max_element(loads_before.begin(), loads_before.end())
            << " status=infeasible reason=edge-budget" << '\n';
        return;
    }
    const auto next = measure_distribution(graph, shards, plan->target);
    const auto dangling = initial_dangling(graph, checkpoint.program);
    const auto loads_after = worker_edge_loads(shards, graph, plan->target);
    out << "plan epoch=" << checkpoint.next_epoch
        << " from_topology=" << current.generation << " to_topology=" << plan->target.generation
        << " from_workers=" << current.workers << " to_workers=" << plan->target.workers
        << " edge_budget=" << edge_budget
        << " peak_from=" << *std::max_element(loads_before.begin(), loads_before.end())
        << " peak_to=" << *std::max_element(loads_after.begin(), loads_after.end())
        << " moved=" << plan->moves.size() << " edges_from_cross_worker=" << previous.cross_worker
        << " edges_to_cross_worker=" << next.cross_worker << " status=feasible" << '\n';
    for (const auto& move : plan->moves) {
        const auto image = checkpoint.work(move.partition, dangling);
        out << "move partition=" << move.partition << " from=" << move.previous << " to=" << move.next
            << " states=" << image.states.size() << " pending=" << image.inbox.size()
            << " image=" << work_digest(image) << '\n';
    }
    for (Worker worker = 0; worker < plan->target.workers; ++worker) {
        print_worker(out, worker, plan->target, checkpoint, next);
    }
    print_traffic(out, next, plan->target.generation);
}

void print_audit(std::ostream& out, const Graph& graph, const Checkpoint& checkpoint) {
    const auto audit = audit_checkpoint(graph, checkpoint);
    out << std::setprecision(17) << "audit=pass next_epoch=" << checkpoint.next_epoch
        << " states=" << audit.states << " sources=" << audit.previous_sources
        << " checked_edges=" << audit.checked_edges << " raw=" << audit.raw
        << " combined=" << audit.combined << " max_mailbox_error=" << audit.max_mailbox_error << '\n';
}

}  // namespace frontier
