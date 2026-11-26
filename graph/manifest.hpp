#pragma once

#include "graph/graph.hpp"

#include <filesystem>
#include <iosfwd>

namespace frontier {

Graph read_graph_v1(std::istream& input);
void write_graph_v1(std::ostream& output, const Graph& graph);

// Format 2: declarable external-vertex dictionary + compact label endpoints.
Graph read_graph_v2(std::istream& input);
Graph read_graph_v2(std::istream& input, const VertexDictionary& external_dictionary);
void write_graph_v2(std::ostream& output, const Graph& graph);

// Sniff the leading format version and dispatch to the matching reader.
Graph read_graph(std::istream& input);
Graph read_graph_auto(std::istream& input);
Graph read_graph(const std::filesystem::path& file);

// Standalone external dictionary declarations.
VertexDictionary read_dictionary(std::istream& input);
VertexDictionary load_dictionary(const std::filesystem::path& file);

Graph load_graph(const std::filesystem::path& file);
Graph load_graph(const std::filesystem::path& file,
                 const std::filesystem::path& dictionary_file);
void save_graph(const std::filesystem::path& file, const Graph& graph);
void save_graph_v2(const std::filesystem::path& file, const Graph& graph);

// Naming aliases for the compact dictionary manifest readers/writers.
inline Graph read_dictionary_graph(std::istream& input) { return read_graph_v2(input); }
inline Graph read_compact_graph(std::istream& input) { return read_graph_v2(input); }
inline Graph parse_graph(std::istream& input) { return read_graph_auto(input); }
inline void write_dictionary_graph(std::ostream& output, const Graph& graph) {
    write_graph_v2(output, graph);
}
inline void write_compact_graph(std::ostream& output, const Graph& graph) {
    write_graph_v2(output, graph);
}

}  // namespace frontier
