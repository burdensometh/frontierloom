#pragma once

#include "checkpoint/snapshot.hpp"

namespace frontier {

struct Audit {
    std::uint64_t states{};
    std::uint64_t previous_sources{};
    std::uint64_t checked_edges{};
    std::uint64_t raw{};
    std::uint64_t combined{};
    double max_mailbox_error{};
};

Audit audit_checkpoint(const Graph& graph, const Checkpoint& checkpoint);

}  // namespace frontier
