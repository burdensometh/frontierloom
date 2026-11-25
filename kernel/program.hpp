#pragma once

#include "mailbox/batch.hpp"

namespace frontier {

inline constexpr std::uint32_t unreachable = std::numeric_limits<std::uint32_t>::max();
inline constexpr std::uint32_t max_steps = 1'000'001;

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

}  // namespace frontier
