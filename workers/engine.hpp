#pragma once

#include "checkpoint/store.hpp"
#include "workers/metrics.hpp"

#include <chrono>

namespace frontier {

struct ScaleAt {
    Epoch epoch{};
    Worker workers{};
};

struct Runtime {
    std::chrono::milliseconds timeout{15000};
    std::uint32_t retry_limit{4};
    std::optional<std::uint32_t> stop_after;
    std::vector<ScaleAt> scaling;
    std::vector<Fault> faults;
};

struct Start {
    Program program;
    Partition partitions = 8;
    Worker workers = 2;
};

Checkpoint compute(const Graph& graph, Store& store, const Start& start,
                   const Runtime& runtime, const Observer& observer = {});
Checkpoint resume(const Graph& graph, Store& store, const Runtime& runtime,
                  const Observer& observer = {});
Checkpoint rescale(const Graph& graph, Store& store, Worker workers, const Runtime& runtime,
                   const Observer& observer = {});

}  // namespace frontier
