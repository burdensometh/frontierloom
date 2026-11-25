#include "workers/result.hpp"
#include "graph/text.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <iomanip>
#include <locale>
#include <sstream>

namespace frontier {
namespace {

constexpr double rank_error_limit = 1e-8;

void check_size(const Graph& graph, std::span<const VertexState> states) {
    if (graph.size() != states.size()) {
        throw std::invalid_argument("result vector has incorrect size");
    }
}

}  // namespace

std::vector<VertexState> result_vector(const Graph& graph, const Checkpoint& checkpoint) {
    const Shards shards(graph, static_cast<Partition>(checkpoint.placement.owners.size()));
    std::vector<VertexState> result(graph.size());
    for (Partition part = 0; part < shards.count(); ++part) {
        const auto members = shards.vertices(part);
        for (std::size_t index = 0; index < members.size(); ++index) {
            result[members[index]] = checkpoint.states.at(part).at(index);
        }
    }
    return result;
}

Comparison verify_result(const Graph& graph, const Checkpoint& checkpoint) {
    if (checkpoint.stop() == Stop::running) {
        throw std::invalid_argument("reference verification requires a finished computation");
    }
    const auto actual = result_vector(graph, checkpoint);
    const auto expected = run_reference(graph, checkpoint.program);
    Comparison result;
    result.vertices = actual.size();
    const bool bfs = checkpoint.program.algorithm == Algorithm::bfs;
    Sum l1;
    for (std::size_t index = 0; index < actual.size(); ++index) {
        if (bfs) {
            result.mismatches += actual[index].distance != expected.states[index].distance ? 1U : 0U;
        } else {
            const auto error = std::abs(actual[index].rank - expected.states[index].rank);
            l1.add(error);
            result.max_abs = std::max(result.max_abs, error);
            result.mismatches += error > rank_error_limit ? 1U : 0U;
        }
    }
    result.l1 = l1.value();
    result.ok = result.mismatches == 0 && result.l1 <= rank_error_limit;
    if (bfs) {
        result.ok = result.ok && checkpoint.stop() == expected.stop &&
            checkpoint.updates() == expected.steps;
    }
    return result;
}

Digest result_digest(const Graph& graph, Algorithm algorithm, std::span<const VertexState> states) {
    check_size(graph, states);
    Hash hash;
    hash.bytes("FrontierLoom result");
    hash.number(graph.identity());
    hash.number(static_cast<std::uint64_t>(algorithm));
    for (const auto& state : states) {
        hash.number(state.distance);
        hash.number(std::bit_cast<std::uint64_t>(state.rank));
    }
    return hash.value();
}

void print_vector(std::ostream& output, const Graph& graph, Algorithm algorithm,
                  std::span<const VertexState> states) {
    check_size(graph, states);
    output << std::setprecision(std::numeric_limits<double>::max_digits10)
           << "vertex," << (algorithm == Algorithm::bfs ? "distance" : "rank") << '\n';
    for (Vertex vertex = 0; vertex < graph.size(); ++vertex) {
        output << graph.external(vertex) << ',';
        if (algorithm == Algorithm::bfs && states[vertex].distance == unreachable) {
            output << "inf";
        } else if (algorithm == Algorithm::bfs) {
            output << states[vertex].distance;
        } else {
            output << states[vertex].rank;
        }
        output << '\n';
    }
    if (!output) {
        throw std::runtime_error("printing result vector failed");
    }
}

void create_vector(const std::filesystem::path& file, const Graph& graph, Algorithm algorithm,
                   std::span<const VertexState> states) {
    std::ostringstream contents;
    contents.imbue(std::locale::classic());
    print_vector(contents, graph, algorithm, states);
    write_new_text_file(file, contents.view());
}

void print_comparison(std::ostream& output, const Comparison& comparison) {
    output << std::setprecision(17) << "verify=" << (comparison.ok ? "pass" : "fail")
           << " vertices=" << comparison.vertices << " mismatches=" << comparison.mismatches
           << " l1=" << comparison.l1 << " max_abs=" << comparison.max_abs << '\n';
}

}  // namespace frontier
