#pragma once

#include "checkpoint/snapshot.hpp"

namespace frontier {

void print_checkpoint_detail(std::ostream& out, const Graph& graph, const Checkpoint& checkpoint);
void print_scale_plan(std::ostream& out, const Graph& graph, const Checkpoint& checkpoint,
                      Worker target);
void print_scale_plan(std::ostream& out, const Graph& graph, const Checkpoint& checkpoint,
                      Worker target, std::uint64_t edge_budget);
void print_audit(std::ostream& out, const Graph& graph, const Checkpoint& checkpoint);

}  // namespace frontier
