#include "workers/wire.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <climits>
#include <cstring>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace frontier::wire {
namespace {

constexpr std::uint32_t magic = 0x314d4c46U;

// Hard ceiling on a single outbound buffer. It deliberately stays far below the
// 96 MiB frame limit so no vertex degree can inflate one frame or one allocation.
constexpr std::uint32_t chunk_message_limit = 4096U;

[[noreturn]] void fail(std::string_view action) {
    const auto error = errno;
    throw Failure(std::string(action) + ": " + std::strerror(error));
}

void validate_count(std::size_t count, std::size_t limit, std::size_t record_size,
                    const Decoder& decoder) {
    if (count > limit || count > decoder.remaining() / record_size) {
        throw Failure("wire collection exceeds its context or frame");
    }
}

void encode_states(Encoder& out, std::span<const VertexState> states) {
    out.u32(static_cast<std::uint32_t>(states.size()));
    for (const auto& state : states) {
        out.u32(state.distance);
        out.real(state.rank);
    }
}

std::vector<VertexState> decode_states(Decoder& in, std::size_t vertices) {
    const auto count = in.u32();
    validate_count(count, vertices, 12, in);
    if (count != vertices) {
        throw Failure("wire image is missing vertex states");
    }
    std::vector<VertexState> states;
    states.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        states.push_back({in.u32(), in.real()});
    }
    return states;
}

void encode_stats(Encoder& out, const StepStats& stats) {
    out.u64(stats.active);
    out.u64(stats.consumed);
    out.u64(stats.emitted);
    out.real(stats.dangling);
    out.real(stats.residual);
    out.real(stats.rank_mass);
}

StepStats decode_stats(Decoder& in) {
    return {in.u64(), in.u64(), in.u64(), in.real(), in.real(), in.real()};
}

void encode_messages(Encoder& out, std::span<const Message> messages) {
    out.u32(static_cast<std::uint32_t>(messages.size()));
    for (const auto& message : messages) {
        out.u32(message.source);
        out.u32(message.target);
        out.real(message.value);
    }
}

std::vector<Message> decode_messages(Decoder& in, const Graph& graph) {
    const auto count = in.u32();
    validate_count(count, chunk_message_limit, 16, in);
    if (count > graph.edge_count()) {
        throw Failure("message chunk exceeds the graph edge count");
    }
    std::vector<Message> messages;
    messages.reserve(count);
    for (std::uint32_t index = 0; index < count; ++index) {
        messages.push_back({in.u32(), in.u32(), in.real()});
    }
    return messages;
}

std::size_t vertex_count(const Shards& shards, Partition part) {
    if (part >= shards.count()) {
        throw Failure("unknown partition in worker frame");
    }
    return shards.vertices(part).size();
}

}  // namespace

void Encoder::u32(std::uint32_t value) {
    for (unsigned i = 0; i < 4; ++i) {
        bytes_.push_back(static_cast<std::byte>(value & 0xffU));
        value >>= 8;
    }
}

void Encoder::u64(std::uint64_t value) {
    for (unsigned i = 0; i < 8; ++i) {
        bytes_.push_back(static_cast<std::byte>(value & 0xffU));
        value >>= 8;
    }
}

void Encoder::real(double value) {
    u64(std::bit_cast<std::uint64_t>(value));
}

void Encoder::text(std::string_view value) {
    if (value.size() > 4096) {
        throw Failure("wire error text exceeds limit");
    }
    u32(static_cast<std::uint32_t>(value.size()));
    for (const auto character : value) {
        bytes_.push_back(static_cast<std::byte>(static_cast<unsigned char>(character)));
    }
}

std::uint64_t Decoder::number(unsigned bytes) {
    if (bytes > remaining()) {
        throw Failure("truncated worker frame");
    }
    std::uint64_t value{};
    for (unsigned i = 0; i < bytes; ++i) {
        value |= static_cast<std::uint64_t>(std::to_integer<unsigned char>(bytes_[offset_++])) << (8U * i);
    }
    return value;
}

std::uint32_t Decoder::u32() {
    return static_cast<std::uint32_t>(number(4));
}

std::uint64_t Decoder::u64() {
    return number(8);
}

double Decoder::real() {
    return std::bit_cast<double>(u64());
}

std::string Decoder::text() {
    const auto length = u32();
    validate_count(length, 4096, 1, *this);
    const auto* start = reinterpret_cast<const char*>(bytes_.data() + offset_);
    offset_ += length;
    return std::string(start, length);
}

void Decoder::finish() const {
    if (remaining() != 0) {
        throw Failure("trailing bytes in worker frame");
    }
}

Channel::Channel(int descriptor) : descriptor_(descriptor) {}

Channel::~Channel() {
    close();
}

Channel::Channel(Channel&& other) noexcept
    : descriptor_(std::exchange(other.descriptor_, -1)), traffic_(other.traffic_) {}

Channel& Channel::operator=(Channel&& other) noexcept {
    if (this != &other) {
        close();
        descriptor_ = std::exchange(other.descriptor_, -1);
        traffic_ = other.traffic_;
    }
    return *this;
}

void Channel::close() noexcept {
    if (descriptor_ >= 0) {
        (void)::close(std::exchange(descriptor_, -1));
    }
}

void Channel::wait(short mask, Deadline deadline) const {
    if (descriptor_ < 0) {
        throw Failure("worker channel is closed");
    }
    for (;;) {
        const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now());
        if (remaining.count() <= 0) {
            throw Failure("worker channel deadline elapsed");
        }
        const auto timeout = static_cast<int>(std::min<std::int64_t>(remaining.count(), INT_MAX));
        pollfd event{descriptor_, mask, 0};
        const auto status = ::poll(&event, 1, timeout);
        if (status == 0 || (status < 0 && errno == EINTR)) {
            continue;
        }
        if (status < 0) {
            fail("polling worker channel");
        }
        if ((event.revents & mask) != 0) {
            return;
        }
        if ((event.revents & (POLLHUP | POLLERR | POLLNVAL)) != 0) {
            throw Failure("worker channel disconnected");
        }
    }
}

void Channel::write(std::span<const std::byte> bytes, Deadline deadline) const {
    while (!bytes.empty()) {
        wait(POLLOUT, deadline);
        const auto sent = ::send(descriptor_, bytes.data(), bytes.size(), MSG_NOSIGNAL);
        if (sent < 0 && (errno == EAGAIN || errno == EINTR)) {
            continue;
        }
        if (sent < 0) {
            fail("writing worker channel");
        }
        if (sent == 0) {
            throw Failure("worker channel made no write progress");
        }
        traffic_.sent += static_cast<std::uint64_t>(sent);
        bytes = bytes.subspan(static_cast<std::size_t>(sent));
    }
}

void Channel::read(std::span<std::byte> bytes, Deadline deadline) const {
    while (!bytes.empty()) {
        wait(POLLIN, deadline);
        const auto count = ::recv(descriptor_, bytes.data(), bytes.size(), 0);
        if (count < 0 && (errno == EAGAIN || errno == EINTR)) {
            continue;
        }
        if (count < 0) {
            fail("reading worker channel");
        }
        if (count == 0) {
            throw Failure("worker closed channel before completing a frame");
        }
        traffic_.received += static_cast<std::uint64_t>(count);
        bytes = bytes.subspan(static_cast<std::size_t>(count));
    }
}

void Channel::send(Kind kind, std::span<const std::byte> bytes, Deadline deadline) const {
    if (bytes.size() > max_frame) {
        throw Failure("worker frame exceeds the byte limit");
    }
    Encoder header;
    header.u32(magic);
    header.u32(static_cast<std::uint32_t>(kind));
    header.u32(static_cast<std::uint32_t>(bytes.size()));
    write(std::move(header).take(), deadline);
    write(bytes, deadline);
}

Frame Channel::receive(Deadline deadline) const {
    std::array<std::byte, 12> header{};
    read(header, deadline);
    Decoder decoded(header);
    const auto marker = decoded.u32();
    const auto kind = decoded.u32();
    const auto size = decoded.u32();
    if (marker != magic || kind < static_cast<std::uint32_t>(Kind::hello) ||
        kind > static_cast<std::uint32_t>(Kind::error) || size > max_frame) {
        throw Failure("worker frame header is invalid");
    }
    Frame frame{static_cast<Kind>(kind), Bytes(size)};
    read(frame.body, deadline);
    return frame;
}

Frame Channel::expect(Kind kind, Deadline deadline) const {
    Frame frame = receive(deadline);
    if (frame.kind == Kind::error) {
        Decoder in(frame.body);
        auto message = in.text();
        in.finish();
        throw Failure("worker rejected command: " + message);
    }
    if (frame.kind != kind) {
        throw Failure("worker returned an unexpected frame kind");
    }
    return frame;
}

void encode(Encoder& out, const WorkerAttempt& attempt) {
    out.u32(attempt.worker);
    out.u64(attempt.topology);
    out.u64(attempt.incarnation);
}

WorkerAttempt decode_attempt(Decoder& in) {
    return {in.u32(), in.u64(), in.u64()};
}

void encode(Encoder& out, const StepInput& work) {
    out.u64(work.epoch);
    out.u32(work.partition);
    out.real(work.dangling);
    encode_states(out, work.states);
    out.u32(static_cast<std::uint32_t>(work.inbox.size()));
    for (const auto& message : work.inbox) {
        out.u32(message.target);
        out.real(message.value);
    }
}

StepInput decode_work(Decoder& in, const Graph&, const Shards& shards) {
    StepInput work;
    work.epoch = in.u64();
    work.partition = in.u32();
    work.dangling = in.real();
    const auto vertices = vertex_count(shards, work.partition);
    work.states = decode_states(in, vertices);
    const auto entries = in.u32();
    validate_count(entries, vertices, 12, in);
    work.inbox.reserve(entries);
    for (std::uint32_t i = 0; i < entries; ++i) {
        work.inbox.push_back({in.u32(), in.real()});
    }
    return work;
}

void encode(Encoder& out, const BatchChunk& chunk) {
    if (chunk.messages.size() > chunk_message_limit) {
        throw Failure("message chunk exceeds the outbound buffer limit");
    }
    out.u64(chunk.epoch);
    encode(out, chunk.attempt);
    out.u32(chunk.source_partition);
    out.u32(chunk.sequence);
    out.u32(chunk.last ? 1U : 0U);
    out.u32(static_cast<std::uint32_t>(chunk.states.size()));
    if (chunk.last) {
        // A partition may own no vertices, in which case an empty state image
        // is the correct and complete payload for its single closing chunk.
        encode_states(out, chunk.states);
        encode_stats(out, chunk.stats);
    } else if (!chunk.states.empty()) {
        throw Failure("only the closing message chunk may carry partition state");
    }
    encode_messages(out, chunk.messages);
}

DecodedChunk decode_chunk(Decoder& in, const Graph& graph, const Shards& shards) {
    DecodedChunk chunk;
    chunk.epoch = in.u64();
    chunk.attempt = decode_attempt(in);
    chunk.source_partition = in.u32();
    chunk.sequence = in.u32();
    chunk.last = in.u32() != 0U;
    const auto state_count = in.u32();
    const auto vertices = vertex_count(shards, chunk.source_partition);
    if (chunk.last) {
        if (state_count != vertices) {
            throw Failure("closing message chunk is missing partition state");
        }
        chunk.states = decode_states(in, vertices);
        chunk.stats = decode_stats(in);
        chunk.has_payload = true;
    } else if (state_count != 0) {
        throw Failure("only the closing message chunk may carry partition state");
    }    chunk.messages = decode_messages(in, graph);
    return chunk;
}

void encode(Encoder& out, const BarrierReceipt& receipt) {
    out.u64(receipt.epoch);
    encode(out, receipt.attempt);
    out.u32(receipt.partition);
    out.u64(receipt.batch);
    encode_stats(out, receipt.stats);
}

BarrierReceipt decode_receipt(Decoder& in) {
    return {in.u64(), decode_attempt(in), in.u32(), in.u64(), decode_stats(in)};
}

}  // namespace frontier::wire
