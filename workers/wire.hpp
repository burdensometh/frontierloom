#pragma once

#include "checkpoint/snapshot.hpp"

#include <chrono>
#include <cstddef>
#include <stdexcept>
#include <string_view>

namespace frontier::wire {

using Bytes = std::vector<std::byte>;
inline constexpr std::size_t max_frame = 96U * 1024U * 1024U;

enum class Kind : std::uint32_t {
    hello = 1,
    compute,
    batches,
    confirm,
    receipts,
    export_shard,
    import_shard,
    exported,
    imported,
    close,
    closed,
    error
};

struct TrafficCount {
    std::uint64_t sent{};
    std::uint64_t received{};
};

class Failure : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class Encoder {
public:
    void u32(std::uint32_t value);
    void u64(std::uint64_t value);
    void real(double value);
    void text(std::string_view value);
    [[nodiscard]] Bytes take() && { return std::move(bytes_); }

private:
    Bytes bytes_;
};

class Decoder {
public:
    explicit Decoder(std::span<const std::byte> bytes) : bytes_(bytes) {}
    std::uint32_t u32();
    std::uint64_t u64();
    double real();
    std::string text();
    void finish() const;

    [[nodiscard]] std::size_t remaining() const { return bytes_.size() - offset_; }

private:
    std::uint64_t number(unsigned bytes);

    std::span<const std::byte> bytes_;
    std::size_t offset_{};
};

struct Frame {
    Kind kind;
    Bytes body;
};

class Channel {
public:
    using Clock = std::chrono::steady_clock;
    using Deadline = Clock::time_point;

    explicit Channel(int descriptor);
    ~Channel();

    Channel(Channel&& other) noexcept;
    Channel& operator=(Channel&& other) noexcept;

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    void send(Kind kind, std::span<const std::byte> bytes, Deadline deadline) const;
    [[nodiscard]] Frame receive(Deadline deadline) const;
    [[nodiscard]] Frame expect(Kind kind, Deadline deadline) const;
    [[nodiscard]] TrafficCount traffic() const { return traffic_; }
    void close() noexcept;

private:
    void wait(short mask, Deadline deadline) const;
    void write(std::span<const std::byte> bytes, Deadline deadline) const;
    void read(std::span<std::byte> bytes, Deadline deadline) const;

    int descriptor_;
    mutable TrafficCount traffic_;
};

void encode(Encoder& out, const WorkerAttempt& attempt);
void encode(Encoder& out, const StepInput& work);
void encode(Encoder& out, const PartitionResult& result);
void encode(Encoder& out, const BarrierReceipt& receipt);

WorkerAttempt decode_attempt(Decoder& in);
StepInput decode_work(Decoder& in, const Graph& graph, const Shards& shards);
PartitionResult decode_result(Decoder& in, const Graph& graph, const Shards& shards);
BarrierReceipt decode_receipt(Decoder& in);

}  // namespace frontier::wire
