#pragma once

#include "mailbox/batch.hpp"

#include <cstddef>
#include <functional>

namespace frontier {

inline constexpr std::uint32_t unreachable = std::numeric_limits<std::uint32_t>::max();
inline constexpr std::uint32_t max_steps = 1'000'001;

// Fixed scatter capacity. A single outbound buffer never holds more than this
// many messages, so its size is independent of any vertex degree or partition
// edge count; larger scatters leave the worker as a sequence of full chunks.
inline constexpr std::size_t message_chunk_capacity = 256;

struct Program {
    Algorithm algorithm = Algorithm::bfs;
    VertexId source{};
    double damping = 0.85;
    double tolerance = 1e-10;
    std::uint32_t limit = max_steps;

    void validate(const Graph& graph) const;
    bool operator==(const Program&) const = default;
};

struct VertexState {
    std::uint32_t distance = unreachable;
    double rank{};
    bool operator==(const VertexState&) const = default;
};

struct StepInput {
    Epoch epoch{};
    Partition partition{};
    double dangling{};
    std::vector<VertexState> states;
    std::vector<InboxEntry> inbox;
    bool operator==(const StepInput&) const = default;
};

struct StepStats {
    std::uint64_t active{};
    std::uint64_t consumed{};
    std::uint64_t emitted{};
    double dangling{};
    double residual{};
    double rank_mass{};
    bool operator==(const StepStats&) const = default;
};

struct PartitionResult {
    std::vector<VertexState> states;
    MessageBatch batch;
    StepStats stats;
};

[[nodiscard]] const char* name(Algorithm algorithm);
[[nodiscard]] std::vector<std::vector<VertexState>>
initial_state(const Graph& graph, const Shards& shards, const Program& program);

[[nodiscard]] Mailbox initial_mailbox(const Graph& graph, const Shards& shards,
                                      const Program& program);
[[nodiscard]] double initial_dangling(const Graph& graph, const Program& program);
void validate_state(Algorithm algorithm, const VertexState& state, std::size_t vertices);
void validate_step_input(const Graph& graph, const Shards& shards,
                         const Program& program, const StepInput& input);

[[nodiscard]] PartitionResult run_partition(const Graph& graph, const Shards& shards,
                                            const Program& program, const WorkerAttempt& attempt,
                                            const StepInput& input);

// Boundary of one bounded outbound buffer. The chunk always carries a contiguous
// slice of the partition's ordered message stream; sequence numbers start at
// zero and the final flush is flagged through last.
using MessageChunk = std::span<const Message>;
using EmitChunk = std::function<void(std::uint32_t sequence, bool last, MessageChunk chunk)>;

struct StreamedPartition {
    std::vector<VertexState> states;
    StepStats stats;
    std::uint32_t chunks{};
};

// Pass one: settle every vertex state and the step statistics. No scatter
// buffer is allocated, so this phase never grows with a vertex degree.
[[nodiscard]] StreamedPartition settle_partition(
    const Graph& graph, const Shards& shards, const Program& program,
    const WorkerAttempt& attempt, const StepInput& input);

// Pass two: walk the settled vertices once and push every legal edge through
// fixed-capacity chunks. Only one chunk is ever resident, so a million-edge
// source produces many bounded chunks instead of one degree-sized buffer.
// Returns the number of emitted chunks and the total message count.
[[nodiscard]] StreamedPartition scatter_partition(
    const Graph& graph, const Shards& shards, const Program& program,
    const StepInput& input, std::span<const VertexState> settled_states,
    std::size_t chunk_capacity, const EmitChunk& emit);

// Convenience wrapper used by the serial reference path and unit tests: settle
// then scatter, with the same single edge-walk cost as the original kernel.
[[nodiscard]] StreamedPartition run_partition_streamed(
    const Graph& graph, const Shards& shards, const Program& program,
    const WorkerAttempt& attempt, const StepInput& input, std::size_t chunk_capacity,
    const EmitChunk& emit);

}  // namespace frontier
