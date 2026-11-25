#include "kernel/reference.hpp"

#include <algorithm>
#include <cmath>
#include <deque>
#include <stdexcept>

namespace frontier {
namespace {

ReferenceResult reference_bfs(const Graph& graph, const Program& program) {
    ReferenceResult result;
    result.states.resize(graph.size());
    const auto root = graph.locate(program.source);
    std::deque<Vertex> queue{root};
    result.states[root].distance = 0;
    bool last_layer_emits = false;
    while (!queue.empty()) {
        const auto vertex = queue.front();
        queue.pop_front();
        const auto distance = result.states[vertex].distance;
        if (distance + 1 > result.steps) {
            result.steps = distance + 1;
            last_layer_emits = false;
        }
        const auto edges = graph.outgoing(vertex);
        last_layer_emits = last_layer_emits || !edges.empty();
        if (distance + 1 >= program.limit) {
            continue;
        }
        for (const auto target : edges) {
            if (result.states[target].distance == unreachable) {
                result.states[target].distance = distance + 1;
                queue.push_back(target);
            }
        }
    }

    if (last_layer_emits && result.steps == program.limit) {
        result.stop = Stop::limit;
    } else {
        result.steps += last_layer_emits ? 1U : 0U;
        result.stop = Stop::quiescent;
    }
    return result;
}

ReferenceResult reference_rank(const Graph& graph, const Program& program) {
    ReferenceResult result;
    const auto count = static_cast<long double>(graph.size());
    std::vector<long double> ranks(graph.size(), 1.0L / count);
    std::vector<long double> next(graph.size());
    std::vector<std::size_t> degrees(graph.size());
    std::vector<Vertex> dangling;
    for (Vertex vertex = 0; vertex < graph.size(); ++vertex) {
        degrees[vertex] = graph.outgoing(vertex).size();
        if (degrees[vertex] == 0) {
            dangling.push_back(vertex);
        }
    }
    result.stop = Stop::limit;

    const auto damping = static_cast<long double>(program.damping);
    for (std::uint32_t round = 0; round < program.limit; ++round) {
        long double dangling_mass{};
        long double residual{};
        for (const auto vertex : dangling) {
            dangling_mass += ranks[vertex];
        }

        for (Vertex vertex = 0; vertex < graph.size(); ++vertex) {
            long double incoming{};
            for (const auto source : graph.incoming(vertex)) {
                incoming += ranks[source] / static_cast<long double>(degrees[source]);
            }
            next[vertex] = (1.0L - damping) / count + damping * (incoming + dangling_mass / count);
            residual += std::abs(next[vertex] - ranks[vertex]);
        }
        ranks.swap(next);
        result.steps = round + 1;
        result.residual = static_cast<double>(residual);
        if (residual <= program.tolerance) {
            result.stop = Stop::converged;
            break;
        }
    }

    result.states.reserve(graph.size());
    for (const auto rank : ranks) {
        result.states.push_back({unreachable, static_cast<double>(rank)});
    }
    return result;
}

}  // namespace

const char* name(Stop stop) {
    switch (stop) {
    case Stop::running:   return "running";
    case Stop::quiescent: return "quiescent";
    case Stop::converged: return "converged";
    case Stop::limit:     return "limit";
    }
    throw std::invalid_argument("unknown stop reason");
}

ReferenceResult run_reference(const Graph& graph, const Program& program) {
    program.validate(graph);
    if (program.algorithm == Algorithm::bfs) {
        return reference_bfs(graph, program);
    }
    return reference_rank(graph, program);
}

}  // namespace frontier
