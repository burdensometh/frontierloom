#include "mailbox/exchange.hpp"

#include <algorithm>
#include <queue>
#include <stdexcept>
#include <unordered_map>

namespace frontier {

Exchange::Exchange(const Graph& graph, const Shards& shards, Algorithm algorithm, Epoch epoch,
                   const PartitionMap& placement, std::span<const WorkerAttempt> attempts)
    : graph_(graph), shards_(shards), algorithm_(algorithm), epoch_(epoch),
      placement_(placement), attempts_(attempts), slots_(shards.count()) {
    placement_.validate(shards_.count());
    if (attempts_.size() != placement_.workers) {
        throw std::invalid_argument("exchange attempt count does not match placement");
    }
}

void Exchange::verify_source(Partition partition, const WorkerAttempt& attempt) const {
    const auto owner = placement_.owner(partition);
    if (attempt != attempts_[owner] || attempt.worker != owner ||
        attempt.topology != placement_.generation || attempt.incarnation == 0) {
        throw std::invalid_argument("message batch or receipt uses a stale ownership lease");
    }
}

void Exchange::validate_messages(const MessageBatch& batch) const {
    const auto members = shards_.vertices(batch.source_partition);
    auto member = members.begin();
    Vertex current_source = invalid_vertex;
    std::span<const Vertex> outgoing;
    std::size_t edge_index{};
    double source_value{};
    for (const auto& message : batch.messages) {
        if (message.source != current_source) {
            if (edge_index != outgoing.size()) {
                throw std::invalid_argument("message batch omitted edges from an active source");
            }
            member = std::lower_bound(member, members.end(), message.source);
            if (member == members.end() || *member != message.source) {
                throw std::invalid_argument("message batch source order or ownership is invalid");
            }
            ++member;
            current_source = message.source;
            outgoing = graph_.outgoing(current_source);
            edge_index = 0;
            source_value = message.value;
            if (!valid_message(algorithm_, source_value) ||
                (algorithm_ == Algorithm::bfs && source_value != static_cast<double>(epoch_ + 1))) {
                throw std::invalid_argument("message batch contains an invalid contribution");
            }
        }
        if (edge_index >= outgoing.size() || outgoing[edge_index] != message.target ||
            message.value != source_value || !valid_message(algorithm_, message.value)) {
            throw std::invalid_argument("message batch does not match the ordered adjacency list");
        }
        ++edge_index;
    }
    if (edge_index != outgoing.size()) {
        throw std::invalid_argument("message batch ended before its last source was complete");
    }
}

Digest Exchange::stage(MessageBatch batch) {
    if (sealed_ || batch.epoch != epoch_) {
        throw std::invalid_argument("message batch does not belong to the open superstep");
    }
    verify_source(batch.source_partition, batch.attempt);
    validate_messages(batch);
    const auto digest = batch_digest(batch);
    auto& slot = slots_.at(batch.source_partition);
    if (slot) {
        if (slot->digest == digest && slot->batch.messages == batch.messages) {
            return digest;
        }
        throw std::invalid_argument("conflicting second batch for a partition");
    }
    if (batch.messages.size() > graph_.edge_count() - raw_count_) {
        throw std::invalid_argument("total emitted messages exceed edge count");
    }
    raw_count_ += batch.messages.size();
    slot.emplace(Slot{std::move(batch), digest, false});
    return digest;
}

void Exchange::confirm(Partition partition, WorkerAttempt attempt, Digest digest) {
    if (sealed_) {
        throw std::logic_error("message exchange already sealed");
    }
    verify_source(partition, attempt);
    auto& slot = slots_.at(partition);
    if (!slot || slot->digest != digest) {
        throw std::invalid_argument("receipt does not acknowledge the staged batch");
    }
    slot->confirmed = true;
}

Mailbox Exchange::seal() {
    if (sealed_) {
        throw std::logic_error("message exchange already sealed");
    }
    for (const auto& slot : slots_) {
        if (!slot || !slot->confirmed) {
            throw std::logic_error("message exchange cannot seal with unconfirmed partitions");
        }
    }
    auto next = algorithm_ == Algorithm::bfs ? combine_bfs() : combine_rank();
    next.validate(shards_, algorithm_);
    sealed_ = true;
    return next;
}

Mailbox Exchange::combine_bfs() const {
    Mailbox next{epoch_ + 1, raw_count_, std::vector<std::vector<InboxEntry>>(shards_.count())};
    std::unordered_map<Vertex, double> minimum;
    minimum.reserve(static_cast<std::size_t>(std::min<std::uint64_t>(raw_count_, graph_.size())));
    for (const auto& slot : slots_) {
        for (const auto& message : slot->batch.messages) {
            const auto [position, inserted] = minimum.try_emplace(message.target, message.value);
            if (!inserted) {
                position->second = std::min(position->second, message.value);
            }
        }
    }
    for (const auto& [target, value] : minimum) {
        next.inboxes[shards_.shard(target)].push_back({target, value});
    }
    for (auto& inbox : next.inboxes) {
        std::sort(inbox.begin(), inbox.end(), [](const InboxEntry& left, const InboxEntry& right) {
            return left.target < right.target;
        });
    }
    return next;
}

Mailbox Exchange::combine_rank() const {
    // A partition emits in source order. The heap merges those streams by source
    // so floating-point contributions reach each target in global vertex order.
    struct Cursor {
        const Message* current;
        const Message* end;
    };
    const auto earlier = [](const Cursor& left, const Cursor& right) {
        return left.current->source > right.current->source;
    };
    std::priority_queue<Cursor, std::vector<Cursor>, decltype(earlier)> heap(earlier);
    for (const auto& slot : slots_) {
        const auto& messages = slot->batch.messages;
        if (!messages.empty()) {
            heap.push({messages.data(), messages.data() + messages.size()});
        }
    }

    std::vector<Sum> sums(graph_.size());
    std::vector<bool> seen(graph_.size());
    while (!heap.empty()) {
        auto cursor = heap.top();
        heap.pop();
        const auto source = cursor.current->source;
        do {
            const auto& message = *cursor.current;
            sums[message.target].add(message.value);
            seen[message.target] = true;
            ++cursor.current;
        } while (cursor.current != cursor.end && cursor.current->source == source);
        if (cursor.current != cursor.end) {
            heap.push(cursor);
        }
    }

    Mailbox next{epoch_ + 1, raw_count_, std::vector<std::vector<InboxEntry>>(shards_.count())};
    for (Vertex vertex = 0; vertex < graph_.size(); ++vertex) {
        if (seen[vertex]) {
            next.inboxes[shards_.shard(vertex)].push_back({vertex, sums[vertex].value()});
        }
    }
    return next;
}

}  // namespace frontier
