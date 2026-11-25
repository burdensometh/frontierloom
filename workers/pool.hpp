#pragma once

#include "workers/process.hpp"
#include "workers/metrics.hpp"

#include <functional>
#include <memory>

namespace frontier {

struct PreparedStep {
    StepSummary step;
    std::vector<std::vector<VertexState>> states;
    Mailbox inbox;
    StepTiming timing;
};

class WorkerPool {
public:
    using StagedHook = std::function<void(Worker worker, std::uint64_t staged_messages)>;

    WorkerPool(const Graph& graph, const Shards& shards, const Checkpoint& image,
               std::span<const PartitionMove> moves, std::chrono::milliseconds timeout);

    [[nodiscard]] PreparedStep step(const Checkpoint& image, double original_dangling,
                                    const StagedHook& on_staged) const;
    void transfer_to(WorkerPool& target, const Checkpoint& image, double original_dangling,
                     std::span<const PartitionMove> moves) const;
    [[nodiscard]] std::vector<pid_t> pids() const;
    [[nodiscard]] std::uint32_t retire(Epoch epoch) noexcept;
    void kill(Worker worker) const;

private:
    const Graph& graph_;
    const Shards& shards_;
    PartitionMap placement_;
    std::vector<std::unique_ptr<WorkerProcess>> processes_;
    std::vector<WorkerAttempt> attempts_;

    [[nodiscard]] wire::TrafficCount traffic() const;
};

}  // namespace frontier
