#pragma once

#include "mailbox/batch.hpp"

#include <optional>

namespace frontier {

class Exchange {
public:
    Exchange(const Graph& graph, const Shards& shards, Algorithm algorithm, Epoch epoch,
             const PartitionMap& placement, std::span<const WorkerAttempt> attempts);

    Digest stage(MessageBatch batch);
    void confirm(Partition partition, WorkerAttempt attempt, Digest digest);
    [[nodiscard]] Mailbox seal();
    [[nodiscard]] std::uint64_t staged_messages() const { return raw_count_; }

private:
    struct Slot {
        MessageBatch batch;
        Digest digest{};
        bool confirmed{};
    };

    void verify_source(Partition partition, const WorkerAttempt& attempt) const;
    void validate_messages(const MessageBatch& batch) const;
    [[nodiscard]] Mailbox combine_bfs() const;
    [[nodiscard]] Mailbox combine_rank() const;

    const Graph& graph_;
    const Shards& shards_;
    Algorithm algorithm_;
    Epoch epoch_;
    const PartitionMap& placement_;
    std::span<const WorkerAttempt> attempts_;
    std::vector<std::optional<Slot>> slots_;
    std::uint64_t raw_count_{};
    bool sealed_{};
};

}  // namespace frontier
