#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace frontier {

using VertexId = std::uint64_t;
using Vertex = std::uint32_t;
using Partition = std::uint32_t;
using Worker = std::uint32_t;
using Epoch = std::uint64_t;
using Generation = std::uint64_t;
using Digest = std::uint64_t;

inline constexpr auto invalid_vertex = std::numeric_limits<Vertex>::max();
inline constexpr std::size_t max_vertices = 1'000'000;
inline constexpr std::size_t max_edges = 4'000'000;

class Hash {
public:
    void number(std::uint64_t value);
    void bytes(std::string_view value);
    [[nodiscard]] Digest value() const { return value_; }

private:
    void byte(unsigned char value);
    Digest value_ = 14695981039346656037ULL;
};

struct GraphInput {
    std::uint64_t revision{};
    std::vector<VertexId> vertices;
    std::vector<std::pair<VertexId, VertexId>> edges;
};

class Graph {
public:
    explicit Graph(GraphInput input);

    [[nodiscard]] std::uint64_t revision() const { return revision_; }
    [[nodiscard]] Digest identity() const { return identity_; }
    [[nodiscard]] std::size_t size() const { return ids_.size(); }
    [[nodiscard]] std::size_t edge_count() const { return outgoing_.values.size(); }
    [[nodiscard]] Vertex locate(VertexId id) const;
    [[nodiscard]] VertexId external(Vertex vertex) const;
    [[nodiscard]] std::span<const VertexId> ids() const { return ids_; }
    [[nodiscard]] std::span<const Vertex> outgoing(Vertex vertex) const;
    [[nodiscard]] std::span<const Vertex> incoming(Vertex vertex) const;

private:
    struct Adjacency {
        std::vector<std::size_t> offsets;
        std::vector<Vertex> values;

        [[nodiscard]] std::span<const Vertex> at(Vertex vertex) const;
        static Adjacency build(std::size_t vertices, std::span<const std::pair<Vertex, Vertex>> edges);
    };

    std::uint64_t revision_;
    std::vector<VertexId> ids_;
    Adjacency outgoing_;
    Adjacency incoming_;
    Digest identity_{};
};

}  // namespace frontier
