#pragma once

#include "graph/graph.hpp"

#include <filesystem>
#include <iosfwd>

namespace frontier {

Graph read_graph_v1(std::istream& input);
void write_graph_v1(std::ostream& output, const Graph& graph);
Graph load_graph(const std::filesystem::path& file);
void save_graph(const std::filesystem::path& file, const Graph& graph);

}  // namespace frontier
