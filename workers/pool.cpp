#include "workers/pool.hpp"
#include "kernel/contract.hpp"
#include "mailbox/exchange.hpp"

#include <numeric>

namespace frontier {
namespace {

using Clock = std::chrono::steady_clock;

std::uint64_t elapsed(Clock::time_point start) {
    const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start);
    return static_cast<std::uint64_t>(duration.count());
}

template<class Operation>
decltype(auto) contact(const WorkerProcess& process, std::string_view phase, Operation operation) {
    try {
        return operation();
    } catch (const wire::Failure& error) {
        const auto attempt = process.attempt();
        throw wire::Failure("worker " + std::to_string(attempt.worker) + " incarnation " +
                            std::to_string(attempt.incarnation) + " during " +
                            std::string(phase) + ": " + error.what());
    }
}

}  // namespace

WorkerPool::WorkerPool(const Graph& graph, const Shards& shards, const Checkpoint& image,
                       std::span<const PartitionMove> moves, std::chrono::milliseconds timeout)
    : graph_(graph), shards_(shards), placement_(image.placement) {
    processes_.reserve(placement_.workers);
    attempts_.reserve(placement_.workers);
    for (Worker worker = 0; worker < placement_.workers; ++worker) {
        auto process = std::make_unique<WorkerProcess>(graph, shards, image, worker, moves, timeout);
        attempts_.push_back(process->attempt());
        processes_.push_back(std::move(process));
    }
}

PreparedStep WorkerPool::step(const Checkpoint& image, double original_dangling,
                              const StagedHook& on_staged) const {
    if (placement_ != image.placement || attempts_.front().incarnation != image.incarnation) {
        throw std::logic_error("worker pool lease does not match committed control state");
    }
    Exchange exchange(graph_, shards_, image.program.algorithm, image.next_epoch, placement_, attempts_);
    Barrier barrier(graph_, shards_, image.program, placement_, attempts_, image.mailbox);

    // Adaptive dispatch: only BFS shards whose inbox holds the pending frontier
    // skip the kernel. PageRank recomputes every vertex each epoch (its barrier
    // requires full vertex coverage), so every PageRank shard stays active even
    // when an epoch starts with an empty inbox (e.g. the epoch-zero scatter).
    const bool sparse_dispatch = image.program.algorithm == Algorithm::bfs;
    std::vector<std::vector<Partition>> active(placement_.workers);
    std::vector<std::vector<Partition>> idle(placement_.workers);
    for (Partition part = 0; part < shards_.count(); ++part) {
        const auto owner = placement_.owner(part);
        if (sparse_dispatch && image.mailbox.inboxes.at(part).empty()) {
            idle[owner].push_back(part);
        } else {
            active[owner].push_back(part);
        }
    }

    StepTiming timing;
    timing.dispatched_shards = shards_.count() - static_cast<std::uint32_t>(
        std::accumulate(idle.begin(), idle.end(), std::size_t{},
                        [](std::size_t total, const auto& parts) { return total + parts.size(); }));
    timing.idle_shards = shards_.count() - timing.dispatched_shards;
    for (Worker worker = 0; worker < placement_.workers; ++worker) {
        if (active[worker].empty()) {
            ++timing.idle_workers;
        } else {
            ++timing.dispatched_workers;
        }
    }

    const auto previous_traffic = traffic();
    auto phase = Clock::now();
    // Dispatch phase: commands only. Every worker receives the shards holding
    // pending frontier work as compute, and its remaining leased shards as an
    // idle settlement. Both belong to the same epoch; replies are read later in
    // command order so socket frames never cross.
    for (const auto& process : processes_) {
        const auto worker = process->attempt().worker;
        contact(*process, "dispatch", [&] {
            if (!active[worker].empty()) {
                process->submit(image, original_dangling, active[worker]);
            }
            if (!idle[worker].empty()) {
                process->idle(image.next_epoch, idle[worker]);
            }
        });
    }
    timing.dispatch_us = elapsed(phase);

    phase = Clock::now();
    // Idle shards keep their committed vertex image; active shards are replaced
    // by the kernel results below, so every partition still yields a full state.
    std::vector<std::vector<VertexState>> next_states = image.states;
    std::vector<std::vector<StagedBatch>> staged(placement_.workers);
    std::vector<std::vector<BarrierReceipt>> idle_receipts(placement_.workers);
    for (const auto& process : processes_) {
        const auto worker = process->attempt().worker;
        if (!active[worker].empty()) {
            auto outputs = contact(*process, "stage", [&] {
                return process->receive(graph_, shards_, image.next_epoch);
            });
            auto& collected = staged[worker];
            collected.reserve(outputs.size() + idle[worker].size());
            for (auto& output : outputs) {
                const auto part = output.batch.source_partition;
                try {
                    validate_partition_output(graph_, shards_, image.program,
                                              image.work(part, original_dangling), output);
                } catch (const std::invalid_argument& error) {
                    throw wire::Failure("invalid output from worker " + std::to_string(worker) +
                                        " for partition " + std::to_string(part) + ": " + error.what());
                }
                const auto digest = exchange.stage(std::move(output.batch));
                collected.push_back({part, digest, output.stats});
                next_states[part] = std::move(output.states);
            }
        }
        if (!idle[worker].empty()) {
            idle_receipts[worker] = contact(*process, "idle receipt", [&] {
                return process->idle_receipts(image.next_epoch, idle[worker]);
            });
            // Idle shards use empty batches but take the identical stage path,
            // so every partition occupies an exchange slot before confirmation.
            for (const auto& receipt : idle_receipts[worker]) {
                const auto digest = exchange.stage(
                    MessageBatch{image.next_epoch, attempts_[worker], receipt.partition, {}});
                if (digest != receipt.batch || receipt.stats != StepStats{}) {
                    throw wire::Failure("idle worker receipt does not bind an empty batch");
                }
                staged[worker].push_back({receipt.partition, digest, StepStats{}});
            }
        }
        // Fault boundaries cover idle settlements too: zero staged messages are
        // reported as an acknowledged round, never as a missing worker task.
        if (on_staged) {
            on_staged(worker, exchange.staged_messages());
        }
    }
    timing.staging_us = elapsed(phase);

    phase = Clock::now();
    for (const auto& process : processes_) {
        const auto worker = process->attempt().worker;
        // Kernel results and idle receipts were appended separately; reorder so
        // the acknowledgement matches the worker's partition-sorted pending set.
        std::sort(staged[worker].begin(), staged[worker].end(),
                  [](const StagedBatch& left, const StagedBatch& right) {
                      return left.partition < right.partition;
                  });
        contact(*process, "confirm", [&] {
            process->confirm(image.next_epoch, staged[worker]);
        });
    }
    for (const auto& process : processes_) {
        const auto worker = process->attempt().worker;
        const auto receipts = contact(*process, "receipt", [&] {
            return process->receipts(image.next_epoch);
        });
        if (receipts.size() != staged[worker].size()) {
            throw wire::Failure("worker receipt count disagrees with its settled shards");
        }
        for (std::size_t index = 0; index < receipts.size(); ++index) {
            const auto& receipt = receipts[index];
            const auto& expected = staged[worker][index];
            if (receipt.partition != expected.partition || receipt.stats != expected.stats) {
                throw wire::Failure("worker receipt disagrees with its staged partition output");
            }
            barrier.acknowledge(receipt, expected.digest);
            exchange.confirm(receipt.partition, receipt.attempt, receipt.batch);
        }
    }
    timing.confirm_us = elapsed(phase);

    phase = Clock::now();
    auto mailbox = exchange.seal();
    const auto summary = barrier.finish(mailbox);
    timing.merge_us = elapsed(phase);
    const auto finished_traffic = traffic();
    timing.wire_sent = finished_traffic.sent - previous_traffic.sent;
    timing.wire_received = finished_traffic.received - previous_traffic.received;
    return {summary, std::move(next_states), std::move(mailbox), timing};
}

void WorkerPool::transfer_to(WorkerPool& target, const Checkpoint& image, double original_dangling,
                               std::span<const PartitionMove> moves) const {
    if (&graph_ != &target.graph_ || &shards_ != &target.shards_ ||
        placement_ != image.placement || target.placement_.generation != next_generation(placement_.generation)) {
        throw std::logic_error("partition handoff does not cross adjacent topologies on the same graph");
    }

    auto move = moves.begin();
    for (Partition part = 0; part < shards_.count(); ++part) {
        const auto former = placement_.owner(part);
        const auto next = target.placement_.owner(part);
        if (former == next) {
            continue;
        }
        const PartitionMove expected{part, former, next};
        if (move == moves.end() || *move != expected) {
            throw std::logic_error("partition migration plan does not bind all changed owners");
        }
        const auto work = image.work(part, original_dangling);
        const auto exported = contact(*processes_[former], "partition export", [&] {
            return processes_[former]->export_shard(work, graph_, shards_);
        });
        contact(*target.processes_[next], "partition import", [&] {
            target.processes_[next]->import_shard(exported);
        });
        ++move;
    }
    if (move != moves.end()) {
        throw std::logic_error("partition migration plan contains unexpected handoffs");
    }
}

std::vector<pid_t> WorkerPool::pids() const {
    std::vector<pid_t> result;
    result.reserve(processes_.size());
    for (const auto& process : processes_) {
        result.push_back(process->pid());
    }
    return result;
}

wire::TrafficCount WorkerPool::traffic() const {
    wire::TrafficCount total;
    for (const auto& process : processes_) {
        const auto count = process->traffic();
        total.sent += count.sent;
        total.received += count.received;
    }
    return total;
}

std::uint32_t WorkerPool::retire(Epoch epoch) noexcept {
    std::uint32_t graceful{};
    for (const auto& process : processes_) {
        if (process->retire(epoch)) {
            ++graceful;
        }
    }
    return graceful;
}

void WorkerPool::kill(Worker worker) const {
    processes_.at(worker)->terminate();
}

}  // namespace frontier
