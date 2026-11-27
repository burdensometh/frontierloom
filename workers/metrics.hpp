#pragma once

#include "checkpoint/snapshot.hpp"

#include <functional>
#include <iosfwd>
#include <sys/types.h>

namespace frontier {

enum class FaultPoint : std::uint8_t { after_send, before_commit, before_replace, after_replace };

[[nodiscard]] const char* name(FaultPoint point);

struct Fault {
    FaultPoint point;
    Epoch epoch;
    Worker worker{};
};

struct StepTiming {
    std::uint64_t dispatch_us{};
    std::uint64_t staging_us{};
    std::uint64_t confirm_us{};
    std::uint64_t merge_us{};
    std::uint64_t publish_us{};
    std::uint64_t wire_sent{};
    std::uint64_t wire_received{};
};

struct Event {
    enum class Kind { started, committed, retry, fault, scaled, retired };
    Kind kind;
    Epoch epoch{};
    Generation topology{};
    Generation incarnation{};
    std::vector<pid_t> pids;
    std::optional<StepSummary> step;
    std::optional<StepTiming> timing;
    std::optional<Fault> fault;
    std::string detail;
    std::uint64_t staged{};
    std::uint64_t moved{};
    std::uint64_t peak_edges{};
    std::uint32_t graceful{};
};

using Observer = std::function<void(const Event&)>;

void print_event(std::ostream& output, const Event& event);
void print_totals(std::ostream& output, const Checkpoint& checkpoint);

}  // namespace frontier
