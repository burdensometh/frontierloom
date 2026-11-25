#pragma once

#include "workers/engine.hpp"

#include <filesystem>

namespace frontier {

struct Settings {
    Partition partitions = 8;
    Worker workers = 2;
    std::chrono::milliseconds timeout{15000};
    std::uint32_t retry_limit = 4;
    double damping = 0.85;
    double tolerance = 1e-10;
    std::uint32_t rank_limit = 100;
    std::uint32_t bfs_limit = max_steps;
};

Settings read_settings(const std::filesystem::path& file);
Fault parse_fault(std::string_view value);
ScaleAt parse_scale(std::string_view value);

}  // namespace frontier
