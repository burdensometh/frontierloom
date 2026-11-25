#pragma once

#include "kernel/program.hpp"

namespace frontier {

enum class Stop : std::uint8_t { running = 0, quiescent, converged, limit };

struct ReferenceResult {
    std::vector<VertexState> states;
    std::uint32_t steps{};
    Stop stop{};
    double residual{};
};

[[nodiscard]] const char* name(Stop stop);
[[nodiscard]] ReferenceResult run_reference(const Graph& graph, const Program& program);

}  // namespace frontier
