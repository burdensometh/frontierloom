#pragma once

#include "kernel/program.hpp"

namespace frontier {

void validate_partition_output(const Graph& graph, const Shards& shards,
                               const Program& program, const StepInput& input,
                               const PartitionResult& output);

}  // namespace frontier
