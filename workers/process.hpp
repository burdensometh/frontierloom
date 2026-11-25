#pragma once

#include "workers/wire.hpp"

#include <sys/types.h>

namespace frontier {

struct StagedBatch {
    Partition partition{};
    Digest digest{};
    StepStats stats;
};

class WorkerProcess {
public:
    WorkerProcess(const Graph& graph, const Shards& shards, const Checkpoint& checkpoint,
                  Worker worker, std::span<const PartitionMove> moves,
                  std::chrono::milliseconds timeout);
    ~WorkerProcess();

    WorkerProcess(const WorkerProcess&) = delete;
    WorkerProcess& operator=(const WorkerProcess&) = delete;

    [[nodiscard]] WorkerAttempt attempt() const { return attempt_; }
    [[nodiscard]] pid_t pid() const { return pid_; }
    [[nodiscard]] std::span<const Partition> assigned() const { return assigned_; }
    [[nodiscard]] wire::TrafficCount traffic() const { return channel_.traffic(); }

    void submit(const Checkpoint& image, double original_dangling) const;
    [[nodiscard]] std::vector<PartitionResult> receive(const Graph& graph, const Shards& shards,
                                                       Epoch epoch) const;
    void confirm(Epoch epoch, std::span<const StagedBatch> batches) const;
    [[nodiscard]] std::vector<BarrierReceipt> receipts(Epoch epoch) const;
    [[nodiscard]] StepInput export_shard(const StepInput& work, const Graph& graph,
                                         const Shards& shards) const;
    void import_shard(const StepInput& work) const;
    [[nodiscard]] bool retire(Epoch epoch) noexcept;
    void terminate() noexcept;

private:
    [[nodiscard]] wire::Channel::Deadline deadline() const;
    [[nodiscard]] wire::Decoder reply_header(const wire::Frame& frame, Epoch epoch) const;

    WorkerAttempt attempt_;
    std::vector<Partition> assigned_;
    std::chrono::milliseconds timeout_;
    wire::Channel channel_{-1};
    pid_t pid_ = -1;
};

}  // namespace frontier
