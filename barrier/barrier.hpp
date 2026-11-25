#pragma once

#include "kernel/reference.hpp"

#include <optional>

namespace frontier {

struct BarrierReceipt {
    Epoch epoch{};
    WorkerAttempt attempt;
    Partition partition{};
    Digest batch{};
    StepStats stats;

    bool operator==(const BarrierReceipt&) const = default;
};

struct StepSummary {
    Epoch epoch{};
    Generation topology{};
    std::uint32_t receipts{};
    StepStats stats;
    std::uint64_t combined{};
    Digest mailbox{};
    Stop stop = Stop::running;

    bool operator==(const StepSummary&) const = default;
};

class Barrier {
public:
    Barrier(const Graph& graph, const Shards& shards, const Program& program,
            const PartitionMap& placement, std::span<const WorkerAttempt> attempts,
            const Mailbox& input);

    void acknowledge(const BarrierReceipt& receipt, Digest staged);
    [[nodiscard]] StepSummary finish(const Mailbox& next) const;

private:
    const Graph& graph_;
    const Shards& shards_;
    const Program& program_;
    const PartitionMap& placement_;
    std::span<const WorkerAttempt> attempts_;
    const Mailbox& input_;
    std::vector<std::optional<BarrierReceipt>> receipts_;
};

[[nodiscard]] Stop stop_after(const Program& program, Epoch epoch,
                              const StepStats& stats, std::size_t combined);
void validate_progress(const Program& program, const Graph& graph, const StepSummary& summary);

}  // namespace frontier
