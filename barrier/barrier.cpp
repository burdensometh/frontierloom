#include "barrier/barrier.hpp"

#include <cmath>
#include <stdexcept>

namespace frontier {
namespace {

constexpr double mass_epsilon = 1e-9;

void validate_stats(Algorithm algorithm, const StepStats& stats,
                    std::size_t vertices, std::size_t edges) {
    if (stats.active > vertices || stats.consumed > vertices || stats.emitted > edges ||
        !valid_message(Algorithm::pagerank, stats.dangling) ||
        !valid_message(Algorithm::pagerank, stats.rank_mass) ||
        !std::isfinite(stats.residual) || stats.residual < 0 || stats.residual > 2.0 + mass_epsilon ||
        stats.dangling > stats.rank_mass) {
        throw std::invalid_argument("barrier statistics outside supported bounds");
    }
    if (algorithm == Algorithm::bfs &&
        (stats.active > stats.consumed || stats.dangling != 0 || stats.rank_mass != 0 ||
         stats.residual != 0 || (stats.active == 0 && stats.emitted != 0))) {
        throw std::invalid_argument("BFS receipt contains incompatible statistics");
    }
    if (algorithm == Algorithm::pagerank && stats.active != vertices) {
        throw std::invalid_argument("PageRank receipt did not cover all vertices");
    }
}

}  // namespace

Barrier::Barrier(const Graph& graph, const Shards& shards, const Program& program,
                 const PartitionMap& placement, std::span<const WorkerAttempt> attempts,
                 const Mailbox& input)
    : graph_(graph), shards_(shards), program_(program), placement_(placement),
      attempts_(attempts), input_(input), receipts_(shards.count()) {
    if (attempts.size() != placement.workers) {
        throw std::invalid_argument("barrier attempt count does not match placement");
    }
}

void Barrier::acknowledge(const BarrierReceipt& receipt, Digest staged) {
    const auto partition = receipt.partition;
    const auto owner = placement_.owner(partition);
    if (receipt.epoch != input_.epoch || receipt.attempt.worker != owner ||
        receipt.attempt.topology != placement_.generation || receipt.attempt != attempts_[owner] ||
        receipt.attempt.incarnation == 0 || receipt.batch != staged) {
        throw std::invalid_argument("barrier receipt does not match the open epoch, lease or batch");
    }
    validate_stats(program_.algorithm, receipt.stats, shards_.vertices(partition).size(),
                   graph_.edge_count());
    if (receipt.stats.consumed != input_.inboxes.at(partition).size()) {
        throw std::invalid_argument("barrier receipt has an incorrect consumption waterline");
    }
    auto& stored = receipts_.at(partition);
    if (stored && *stored != receipt) {
        throw std::invalid_argument("conflicting acknowledgement for a partition");
    }
    stored = receipt;
}

StepSummary Barrier::finish(const Mailbox& next) const {
    StepSummary summary;
    summary.epoch = input_.epoch;
    summary.topology = placement_.generation;
    Sum dangling;
    Sum residual;
    Sum rank_mass;
    for (const auto& receipt : receipts_) {
        if (!receipt) {
            throw std::logic_error("barrier cannot commit with missing receipts");
        }
        ++summary.receipts;
        summary.stats.active += receipt->stats.active;
        summary.stats.consumed += receipt->stats.consumed;
        summary.stats.emitted += receipt->stats.emitted;
        dangling.add(receipt->stats.dangling);
        residual.add(receipt->stats.residual);
        rank_mass.add(receipt->stats.rank_mass);
    }
    summary.stats.dangling = dangling.value();
    summary.stats.residual = residual.value();
    summary.stats.rank_mass = rank_mass.value();
    summary.combined = next.combined_count();
    summary.mailbox = mailbox_digest(next);
    if (next.epoch != input_.epoch + 1 || next.raw_count != summary.stats.emitted ||
        summary.stats.consumed != input_.combined_count()) {
        throw std::invalid_argument("mailbox waterline disagrees with barrier receipts");
    }
    summary.stop = stop_after(program_, summary.epoch, summary.stats, summary.combined);
    validate_progress(program_, graph_, summary);
    return summary;
}

Stop stop_after(const Program& program, Epoch epoch, const StepStats& stats, std::size_t combined) {
    if (program.algorithm == Algorithm::bfs) {
        if (combined == 0) {
            return Stop::quiescent;
        }
        return epoch + 1 >= program.limit ? Stop::limit : Stop::running;
    }
    if (epoch == 0) {
        return Stop::running;
    }
    if (stats.residual <= program.tolerance) {
        return Stop::converged;
    }
    return epoch >= program.limit ? Stop::limit : Stop::running;
}

void validate_progress(const Program& program, const Graph& graph, const StepSummary& summary) {
    validate_stats(program.algorithm, summary.stats, graph.size(), graph.edge_count());
    if (summary.topology == 0 || summary.receipts == 0 || summary.receipts > max_partitions ||
        summary.combined > summary.stats.emitted || summary.combined > graph.size() ||
        summary.epoch > program.limit || summary.stop !=
            stop_after(program, summary.epoch, summary.stats, summary.combined)) {
        throw std::invalid_argument("superstep summary has invalid progress");
    }
    if (program.algorithm == Algorithm::pagerank &&
        (summary.stats.emitted != graph.edge_count() ||
         std::abs(summary.stats.rank_mass - 1.0) > mass_epsilon ||
         (summary.epoch == 0 && (summary.stats.residual != 0 || summary.stats.consumed != 0)))) {
        throw std::invalid_argument("PageRank barrier violates mass conservation");
    }
    if (program.algorithm == Algorithm::bfs && summary.epoch >= program.limit) {
        throw std::invalid_argument("BFS barrier is beyond its configured step limit");
    }
}

}  // namespace frontier
