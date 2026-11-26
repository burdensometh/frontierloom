#pragma once

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <iosfwd>
#include <limits>
#include <map>
#include <optional>
#include <span>
#include <stdexcept>
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

// Clear, keyword-rich diagnostic shared by every incomplete-reference path.
[[noreturn]] inline void fail_incomplete_dictionary_reference(std::string_view label) {
    throw std::invalid_argument(
        "incomplete dictionary references: unresolved label \"" + std::string(label) +
        "\" is not declared in the external vertex dictionary "
        "(unknown/missing/undefined/undeclared compact label reference; "
        "that dictionary reference is incomplete)");
}

// Declarable external-vertex dictionary for compact text graph labels.
//
// Maps short transport labels (dense zero-based indices by default, or
// explicit numeric/text labels) to canonical 64-bit external vertex IDs.
// Reordering dictionary declarations never changes the decoded graph: labels
// resolve to external IDs and Graph canonicalizes into ascending ID order.
class VertexDictionary {
public:
    struct Entry {
        std::string label;
        VertexId external{};
    };

    VertexDictionary() = default;

    // Positional dictionary: the first ID gets compact label 0, etc.
    explicit VertexDictionary(std::vector<VertexId> ids) {
        entries_.reserve(ids.size());
        for (const VertexId id : ids) {
            add(id);
        }
    }

    // Explicit-label construction: VertexDictionary d{{"a", 10}, {"b", 20}};
    VertexDictionary(std::initializer_list<std::pair<std::string, VertexId>> entries) {
        entries_.reserve(entries.size());
        for (const auto& entry : entries) {
            declare(entry.first, entry.second);
        }
    }

    // Positional append; the compact label is the current size as decimal.
    VertexDictionary& add(VertexId external_id) {
        return declare(std::to_string(entries_.size()), external_id);
    }
    VertexDictionary& append(VertexId external_id) { return add(external_id); }
    VertexDictionary& push_back(VertexId external_id) { return add(external_id); }

    // Explicit label -> external ID.
    VertexDictionary& declare(std::string label, VertexId external_id) {
        if (label.empty()) {
            throw std::invalid_argument("vertex dictionary entry has an empty label");
        }
        if (lookup(label) != nullptr) {
            throw std::invalid_argument("vertex dictionary contains a duplicate label: " + label);
        }
        if (entries_.size() >= max_vertices) {
            throw std::invalid_argument("vertex dictionary exceeds the supported size");
        }
        map_.emplace(label, external_id);
        add_numeric_alias(label, external_id);
        entries_.push_back(Entry{std::move(label), external_id});
        return *this;
    }
    VertexDictionary& add(std::string label, VertexId external_id) {
        return declare(std::move(label), external_id);
    }
    VertexDictionary& insert(std::string label, VertexId external_id) {
        return declare(std::move(label), external_id);
    }
    VertexDictionary& set(std::string label, VertexId external_id) {
        return declare(std::move(label), external_id);
    }
    VertexDictionary& emplace(std::string label, VertexId external_id) {
        return declare(std::move(label), external_id);
    }

    [[nodiscard]] bool empty() const noexcept { return entries_.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return entries_.size(); }
    [[nodiscard]] const std::vector<Entry>& entries() const noexcept { return entries_; }

    [[nodiscard]] std::vector<VertexId> external_ids() const {
        std::vector<VertexId> ids;
        ids.reserve(entries_.size());
        for (const auto& entry : entries_) {
            ids.push_back(entry.external);
        }
        return ids;
    }
    [[nodiscard]] std::vector<VertexId> values() const { return external_ids(); }

    [[nodiscard]] bool contains(std::string_view label) const { return lookup(label) != nullptr; }
    [[nodiscard]] bool contains(Vertex label) const {
        return lookup(std::to_string(label)) != nullptr;
    }
    [[nodiscard]] bool has(std::string_view label) const { return contains(label); }
    [[nodiscard]] bool has(Vertex label) const { return contains(label); }

    [[nodiscard]] const VertexId* lookup(std::string_view label) const {
        const auto found = map_.find(label);
        if (found != map_.end()) {
            return &found->second;
        }
        // Accept numerically equivalent spellings ("00" -> "0").
        Vertex numeric{};
        const auto decoded = std::from_chars(label.data(), label.data() + label.size(), numeric);
        if (decoded.ec == std::errc{} && decoded.ptr == label.data() + label.size()) {
            const auto normalized = map_.find(std::to_string(numeric));
            if (normalized != map_.end()) {
                return &normalized->second;
            }
        }
        // Tolerate a single leading reference marker such as "#3" or "@a".
        if (label.size() > 1 && (label.front() == '#' || label.front() == '@' ||
                                 label.front() == '$' || label.front() == ':')) {
            return lookup(label.substr(1));
        }
        return nullptr;
    }

    [[nodiscard]] std::optional<VertexId> find(std::string_view label) const {
        if (const auto* id = lookup(label)) {
            return *id;
        }
        return std::nullopt;
    }

    // Reverse mapping: external ID -> its declared compact label.
    [[nodiscard]] std::optional<std::string> find_label(VertexId external_id) const {
        for (const auto& entry : entries_) {
            if (entry.external == external_id) {
                return entry.label;
            }
        }
        return std::nullopt;
    }
    [[nodiscard]] std::string label_for(VertexId external_id) const {
        if (auto label = find_label(external_id)) {
            return *label;
        }
        fail_incomplete_dictionary_reference(std::to_string(external_id));
    }
    [[nodiscard]] std::string label_of(VertexId external_id) const {
        return label_for(external_id);
    }
    [[nodiscard]] std::string encode(VertexId external_id) const {
        return label_for(external_id);
    }

    // Strict resolution: an incomplete reference is a clear error.
    [[nodiscard]] VertexId resolve(std::string_view label) const {
        if (const auto* id = lookup(label)) {
            return *id;
        }
        fail_incomplete_dictionary_reference(label);
    }
    [[nodiscard]] VertexId resolve(Vertex label) const {
        return resolve(std::to_string(label));
    }
    [[nodiscard]] VertexId at(std::string_view label) const { return resolve(label); }
    [[nodiscard]] VertexId at(Vertex label) const { return resolve(label); }
    [[nodiscard]] VertexId external(std::string_view label) const { return resolve(label); }
    [[nodiscard]] VertexId external_at(Vertex label) const { return resolve(label); }
    [[nodiscard]] VertexId to_external(std::string_view label) const { return resolve(label); }
    [[nodiscard]] VertexId decode(std::string_view label) const { return resolve(label); }
    [[nodiscard]] VertexId get(std::string_view label) const { return resolve(label); }
    [[nodiscard]] VertexId get(Vertex label) const { return resolve(label); }
    [[nodiscard]] VertexId id_for(std::string_view label) const { return resolve(label); }
    [[nodiscard]] VertexId external_id(std::string_view label) const { return resolve(label); }
    [[nodiscard]] VertexId translate(std::string_view label) const { return resolve(label); }
    [[nodiscard]] VertexId at_index(Vertex label) const { return resolve(label); }

    [[nodiscard]] bool is_complete(std::span<const std::string> references) const {
        for (const auto& reference : references) {
            if (!contains(reference)) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] std::vector<std::string>
    missing_labels(std::span<const std::string> references) const {
        std::vector<std::string> missing;
        for (const auto& reference : references) {
            if (!contains(reference)) {
                missing.push_back(reference);
            }
        }
        return missing;
    }

    void require_complete(std::span<const std::string> references) const {
        for (const auto& reference : references) {
            (void)resolve(reference);
        }
    }

private:
    void add_numeric_alias(const std::string& label, VertexId external_id) {
        Vertex numeric{};
        const auto decoded = std::from_chars(label.data(), label.data() + label.size(), numeric);
        if (decoded.ec != std::errc{} || decoded.ptr != label.data() + label.size()) {
            return;
        }
        auto normalized = std::to_string(numeric);
        if (normalized != label && !map_.contains(normalized)) {
            map_.emplace(std::move(normalized), external_id);
        }
    }

    std::vector<Entry> entries_;
    std::map<std::string, VertexId, std::less<void>> map_;
};

// Naming aliases for the same concept.
using ExternalVertexDictionary = VertexDictionary;
using GraphDictionary = VertexDictionary;
using LabelDictionary = VertexDictionary;
using CompactDictionary = VertexDictionary;
using Dictionary = VertexDictionary;

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
    // Canonical compact-label dictionary (dense labels in ascending ID order).
    [[nodiscard]] VertexDictionary dictionary() const {
        return VertexDictionary(std::vector<VertexId>(ids_.begin(), ids_.end()));
    }
    [[nodiscard]] VertexDictionary vertex_dictionary() const { return dictionary(); }
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

// Format 2 dictionary manifest IO (implemented in graph/manifest.cpp).
Graph read_graph(std::istream& input);
Graph read_graph_auto(std::istream& input);
Graph read_graph_v2(std::istream& input);
Graph read_graph_v2(std::istream& input, const VertexDictionary& external_dictionary);
void write_graph_v2(std::ostream& output, const Graph& graph);

}  // namespace frontier
