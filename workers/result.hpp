#pragma once

#include "checkpoint/snapshot.hpp"

#include <filesystem>

namespace frontier {

struct Comparison {
    std::size_t vertices{};
    std::size_t mismatches{};
    double max_abs{};
    double l1{};
    bool ok{};
};

std::vector<VertexState> result_vector(const Graph& graph, const Checkpoint& checkpoint);
Comparison verify_result(const Graph& graph, const Checkpoint& checkpoint);
Digest result_digest(const Graph& graph, Algorithm algorithm, std::span<const VertexState> states);
void print_vector(std::ostream& output, const Graph& graph, Algorithm algorithm,
                   std::span<const VertexState> states);
void create_vector(const std::filesystem::path& file, const Graph& graph, Algorithm algorithm,
                   std::span<const VertexState> states);
void print_comparison(std::ostream& output, const Comparison& comparison);

}  // namespace frontier
