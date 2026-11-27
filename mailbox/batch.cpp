#include "mailbox/batch.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <stdexcept>

namespace frontier {

bool valid_message(Algorithm algorithm, double value) {
    if (!std::isfinite(value) || value < 0.0 || std::signbit(value)) {
        return false;
    }
    switch (algorithm) {
    case Algorithm::bfs:
        return value <= static_cast<double>(max_vertices) && std::trunc(value) == value;
    case Algorithm::pagerank:
        return value <= 1.0 + 1e-10;
    }
    return false;
}

std::size_t Mailbox::combined_count() const {
    std::size_t total{};
    for (const auto& inbox : inboxes) {
        total += inbox.size();
    }
    return total;
}

void Mailbox::validate(const Shards& shards, Algorithm algorithm) const {
    if (inboxes.size() != shards.count() || raw_count > max_edges ||
        combined_count() > raw_count) {
        throw std::invalid_argument("mailbox count is invalid");
    }
    for (Partition part = 0; part < shards.count(); ++part) {
        const auto members = shards.vertices(part);
        auto member = members.begin();
        for (const auto& entry : inboxes[part]) {
            member = std::lower_bound(member, members.end(), entry.target);
            if (member == members.end() || *member != entry.target ||
                !valid_message(algorithm, entry.value)) {
                throw std::invalid_argument("invalid, unordered or misrouted inbox entry");
            }
            ++member;
        }
    }
}

Digest batch_digest(const MessageBatch& batch) {
    Hash hash;
    hash.bytes("FrontierLoom batch");
    hash.number(batch.epoch);
    hash.number(batch.attempt.worker);
    hash.number(batch.attempt.topology);
    hash.number(batch.attempt.incarnation);
    hash.number(batch.source_partition);
    hash.number(batch.messages.size());
    for (const auto& message : batch.messages) {
        hash.number(message.source);
        hash.number(message.target);
        hash.number(std::bit_cast<std::uint64_t>(message.value));
    }
    return hash.value();
}

Digest streaming_batch_digest(const MessageBatch& batch) {
    BatchDigester digester(batch.epoch, batch.attempt, batch.source_partition);
    digester.add(batch.messages);
    return digester.finish(batch.messages.size());
}

BatchDigester::BatchDigester(Epoch epoch, const WorkerAttempt& attempt, Partition partition) {
    hash_.bytes("FrontierLoom streaming batch");
    hash_.number(epoch);
    hash_.number(attempt.worker);
    hash_.number(attempt.topology);
    hash_.number(attempt.incarnation);
    hash_.number(partition);
}

void BatchDigester::add(const Message& message) {
    hash_.number(message.source);
    hash_.number(message.target);
    hash_.number(std::bit_cast<std::uint64_t>(message.value));
}

void BatchDigester::add(std::span<const Message> messages) {
    for (const auto& message : messages) {
        add(message);
    }
}

Digest BatchDigester::finish(std::uint64_t total) const {
    Hash hash = hash_;
    hash.number(total);
    return hash.value();
}

Digest mailbox_digest(const Mailbox& mailbox) {
    Hash hash;
    hash.bytes("FrontierLoom mailbox");
    hash.number(mailbox.epoch);
    hash.number(mailbox.raw_count);
    for (const auto& inbox : mailbox.inboxes) {
        hash.number(inbox.size());
        for (const auto& entry : inbox) {
            hash.number(entry.target);
            hash.number(std::bit_cast<std::uint64_t>(entry.value));
        }
    }
    return hash.value();
}

void Sum::add(double value) {
    const auto combined = sum_ + value;
    if (std::abs(sum_) >= std::abs(value)) {
        correction_ += (sum_ - combined) + value;
    } else {
        correction_ += (value - combined) + sum_;
    }
    sum_ = combined;
}

}  // namespace frontier
