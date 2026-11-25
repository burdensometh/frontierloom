#include "graph/graph.hpp"

#include <algorithm>
#include <stdexcept>

namespace frontier {

void Hash::byte(unsigned char value) {
    value_ ^= value;
    value_ *= 1099511628211ULL;
}

void Hash::number(std::uint64_t value) {
    for (unsigned index = 0; index < 8; ++index) {
        byte(static_cast<unsigned char>(value & 0xffU));
        value >>= 8;
    }
}

void Hash::bytes(std::string_view value) {
    for (const char character : value) {
        byte(static_cast<unsigned char>(character));
    }
}

std::span<const Vertex> Graph::Adjacency::at(Vertex vertex) const {
    const auto start = offsets.at(vertex);
    const auto end = offsets.at(static_cast<std::size_t>(vertex) + 1);
    return std::span<const Vertex>(values).subspan(start, end - start);
}

Graph::Adjacency Graph::Adjacency::build(std::size_t vertices,
                                        std::span<const std::pair<Vertex, Vertex>> edges) {
    Adjacency result;
    result.offsets.resize(vertices + 1);
    result.values.reserve(edges.size());
    for (const auto& [source, target] : edges) {
        ++result.offsets.at(static_cast<std::size_t>(source) + 1);
        result.values.push_back(target);
    }
    for (std::size_t index = 1; index < result.offsets.size(); ++index) {
        result.offsets[index] += result.offsets[index - 1];
    }
    return result;
}

Graph::Graph(GraphInput input)
    : revision_(input.revision), ids_(std::move(input.vertices)) {
    if (revision_ == 0 || ids_.empty() || ids_.size() > max_vertices ||
        input.edges.size() > max_edges) {
        throw std::invalid_argument("graph revision, vertex count or edge count is invalid");
    }
    std::sort(ids_.begin(), ids_.end());
    if (std::adjacent_find(ids_.begin(), ids_.end()) != ids_.end()) {
        throw std::invalid_argument("graph has duplicate vertex IDs");
    }

    std::sort(input.edges.begin(), input.edges.end());
    if (std::adjacent_find(input.edges.begin(), input.edges.end()) != input.edges.end()) {
        throw std::invalid_argument("graph has duplicate directed edges");
    }

    Hash hash;
    hash.bytes("FrontierLoom graph v1");
    hash.number(revision_);
    hash.number(ids_.size());
    for (auto id : ids_) {
        hash.number(id);
    }
    hash.number(input.edges.size());
    std::vector<std::pair<Vertex, Vertex>> forward;
    forward.reserve(input.edges.size());
    for (const auto& [source, target] : input.edges) {
        hash.number(source);
        hash.number(target);
        forward.emplace_back(locate(source), locate(target));
    }
    identity_ = hash.value();
    outgoing_ = Adjacency::build(ids_.size(), forward);

    for (auto& edge : forward) {
        std::swap(edge.first, edge.second);
    }
    std::sort(forward.begin(), forward.end());
    incoming_ = Adjacency::build(ids_.size(), forward);
}

Vertex Graph::locate(VertexId id) const {
    const auto found = std::lower_bound(ids_.begin(), ids_.end(), id);
    if (found == ids_.end() || *found != id) {
        throw std::invalid_argument("unknown vertex ID " + std::to_string(id));
    }
    return static_cast<Vertex>(found - ids_.begin());
}

VertexId Graph::external(Vertex vertex) const {
    return ids_.at(vertex);
}

std::span<const Vertex> Graph::outgoing(Vertex vertex) const {
    return outgoing_.at(vertex);
}

std::span<const Vertex> Graph::incoming(Vertex vertex) const {
    return incoming_.at(vertex);
}

}  // namespace frontier
