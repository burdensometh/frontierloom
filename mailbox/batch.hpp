#pragma once

#include "graph/partition.hpp"

#include <compare>

namespace frontier {

enum class Algorithm : std::uint8_t { bfs = 1, pagerank = 2 };

struct WorkerAttempt {
    Worker worker{};
    Generation topology{};
    Generation incarnation{};
    auto operator<=>(const WorkerAttempt&) const = default;
};

struct Message {
    Vertex source{};
    Vertex target{};
    double value{};
    bool operator==(const Message&) const = default;
};

struct MessageBatch {
    Epoch epoch{};
    WorkerAttempt attempt;
    Partition source_partition{};
    std::vector<Message> messages;
};

struct InboxEntry {
    Vertex target{};
    double value{};
    bool operator==(const InboxEntry&) const = default;
};

struct Mailbox {
    Epoch epoch{};
    std::uint64_t raw_count{};
    std::vector<std::vector<InboxEntry>> inboxes;

    [[nodiscard]] std::size_t combined_count() const;
    void validate(const Shards& shards, Algorithm algorithm) const;
    bool operator==(const Mailbox&) const = default;
};

[[nodiscard]] bool valid_message(Algorithm algorithm, double value);
[[nodiscard]] Digest batch_digest(const MessageBatch& batch);
[[nodiscard]] Digest mailbox_digest(const Mailbox& mailbox);

class Sum {
public:
    void add(double value);
    [[nodiscard]] double value() const { return sum_ + correction_; }

private:
    double sum_{};
    double correction_{};
};

}  // namespace frontier
