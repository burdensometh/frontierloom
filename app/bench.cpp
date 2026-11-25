#include "graph/text.hpp"
#include "workers/engine.hpp"
#include "workers/result.hpp"

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <unistd.h>

namespace frontier {
namespace {

using Clock = std::chrono::steady_clock;

struct Options {
    std::filesystem::path directory;
    std::size_t size = 4096;
    std::uint32_t repeats = 3;
};

Options read_options(int argc, char** argv) {
    if (argc < 2 || argc > 4) {
        throw std::invalid_argument("usage: frontier-bench DIRECTORY [VERTICES [REPEATS]]");
    }
    Options options;
    options.directory = argv[1];
    if (argc >= 3) {
        options.size = parse_number<std::size_t>(argv[2], "benchmark vertices");
    }
    if (argc >= 4) {
        options.repeats = parse_number<std::uint32_t>(argv[3], "benchmark repeats");
    }
    if (options.size < 128 || options.size > 100'000 || options.repeats < 1 || options.repeats > 20) {
        throw std::invalid_argument("benchmark needs 128–100000 vertices and 1–20 repeats");
    }
    if (!std::filesystem::create_directory(options.directory)) {
        throw std::invalid_argument("benchmark output directory already exists");
    }
    return options;
}

Graph input_graph(std::size_t size, bool hub) {
    GraphInput input;
    input.revision = hub ? 1 : 2;
    input.vertices.reserve(size);
    input.edges.reserve(3 * size);
    for (VertexId vertex = 0; vertex < size; ++vertex) {
        input.vertices.push_back(vertex);
    }
    if (hub) {
        for (VertexId vertex = 1; vertex < size; ++vertex) {
            input.edges.emplace_back(0, vertex);
            input.edges.emplace_back(vertex, 0);
            if (vertex % 13 != 0) {
                const auto next = vertex + 1 == size ? 1 : vertex + 1;
                input.edges.emplace_back(vertex, next);
            }
        }
    } else {
        constexpr VertexId path = 16;
        for (VertexId vertex = 0; vertex + 1 < path; ++vertex) {
            input.edges.emplace_back(vertex, vertex + 1);
        }
        for (VertexId vertex = path; vertex < size; ++vertex) {
            const auto next = vertex + 1 == size ? path : vertex + 1;
            input.edges.emplace_back(vertex, next);
        }
    }
    return Graph(std::move(input));
}

struct Trial {
    double elapsed{};
    std::optional<Digest> digest;
    std::uint64_t messages{};
    std::uint64_t combined{};
    std::uint64_t pending{};
    std::uint64_t recoveries{};
    std::uint64_t peak_width{};
    std::uint64_t bytes{};
    Epoch next_epoch{};
};

double seconds(Clock::time_point start) {
    return std::chrono::duration<double>(Clock::now() - start).count();
}

Trial describe(const Graph& graph, const Checkpoint& result) {
    Trial trial;
    const auto states = result_vector(graph, result);
    trial.digest = result_digest(graph, result.program.algorithm, states);
    trial.messages = result.totals.emitted;
    trial.combined = result.totals.combined;
    trial.pending = result.mailbox.combined_count();
    trial.recoveries = result.totals.recovered_failures;
    trial.next_epoch = result.next_epoch;
    return trial;
}

Trial compute_trial(const Graph& graph, const std::filesystem::path& path,
                    const Program& program, bool inject) {
    Trial trial;
    Runtime runtime;
    if (inject) {
        runtime.faults.push_back({FaultPoint::after_send, 1, 1});
    }
    std::uint64_t max_frontier{};
    const Observer observe = [&](const Event& event) {
        if (event.step) {
            max_frontier = std::max(max_frontier, event.step->combined);
        }
    };

    Store store(path);
    const auto begin = Clock::now();
    const auto finished = compute(graph, store, {program, 8, 3}, runtime, observe);
    const auto elapsed = seconds(begin);
    if (!verify_result(graph, finished).ok) {
        throw std::runtime_error("benchmark distributed result differs from serial reference");
    }
    if (finished.totals.recovered_failures != (inject ? 1U : 0U)) {
        throw std::runtime_error("benchmark recovery count differs from planned loss");
    }
    trial = describe(graph, finished);
    trial.elapsed = elapsed;
    trial.peak_width = max_frontier;
    trial.bytes = std::filesystem::file_size(path);
    return trial;
}

void print_trial(std::string_view benchmark, std::uint32_t index, const Graph& graph,
                 const Trial& trial) {
    std::cout << std::setprecision(8) << "case=" << benchmark << " trial=" << index + 1
              << " vertices=" << graph.size() << " edges=" << graph.edge_count()
              << " seconds=" << trial.elapsed << " epoch=" << trial.next_epoch
              << " emitted=" << trial.messages << " combined=" << trial.combined
              << " peak_width=" << trial.peak_width << " pending=" << trial.pending
              << " bytes=" << trial.bytes << " recovered=" << trial.recoveries
              << " digest=" << trial.digest.value() << '\n';
}

class Repeats {
public:
    explicit Repeats(std::string_view name) : name_(name) {}

    void record(std::uint32_t index, const Graph& graph, const Trial& trial) {
        if (digest_ && digest_ != trial.digest) {
            throw std::runtime_error("benchmark result digest changed between trials");
        }
        digest_ = trial.digest;
        elapsed_.push_back(trial.elapsed);
        print_trial(name_, index, graph, trial);
    }

    void summarize(std::uint64_t bytes = 0) {
        std::sort(elapsed_.begin(), elapsed_.end());
        const auto middle = elapsed_.size() / 2;
        auto median = elapsed_[middle];
        if (elapsed_.size() % 2 == 0) {
            median = (median + elapsed_[middle - 1]) / 2;
        }
        std::cout << "summary=" << name_ << " repeats=" << elapsed_.size()
                  << " min_s=" << elapsed_.front() << " median_s=" << median
                  << " max_s=" << elapsed_.back();
        if (bytes > 0) {
            std::cout << " median_MiB_per_s=" << static_cast<double>(bytes) / (1024 * 1024) / median;
        }
        std::cout << '\n';
    }

private:
    std::string_view name_;
    std::optional<Digest> digest_;
    std::vector<double> elapsed_;
};

void run_benchmarks(const Options& options) {
    const auto hub = input_graph(options.size, true);
    const auto sparse = input_graph(options.size, false);
    Program bfs;
    Program rank;
    rank.algorithm = Algorithm::pagerank;
    rank.limit = 8;

    Repeats fanout("high-out-degree");
    Repeats path("sparse-frontier");
    for (std::uint32_t trial = 0; trial < options.repeats; ++trial) {
        const auto suffix = std::to_string(trial) + ".flc";
        const auto high = compute_trial(hub, options.directory / ("hub-" + suffix), rank, false);
        fanout.record(trial, hub, high);
        const auto low = compute_trial(sparse, options.directory / ("path-" + suffix), bfs, true);
        path.record(trial, sparse, low);
    }
    fanout.summarize();
    path.summarize();

    const auto checkpoint = options.directory / "restore.flc";
    Checkpoint seed;
    {
        Store store(checkpoint);
        Runtime partial;
        partial.stop_after = 4;
        seed = compute(hub, store, {rank, 8, 3}, partial);
    }
    if (seed.stop() != Stop::running || seed.mailbox.combined_count() == 0) {
        throw std::runtime_error("restore benchmark requires a pre-terminal snapshot with a live frontier");
    }

    Repeats restore("recovery-read");
    const auto bytes = std::filesystem::file_size(checkpoint);
    for (std::uint32_t trial = 0; trial < options.repeats; ++trial) {
        const auto begin = Clock::now();
        const auto image = load_checkpoint(checkpoint, hub);
        const auto elapsed = seconds(begin);
        if (image != seed) {
            throw std::runtime_error("recovery read changed checkpoint state");
        }
        auto result = describe(hub, image);
        result.elapsed = elapsed;
        result.bytes = bytes;
        restore.record(trial, hub, result);
    }
    restore.summarize(bytes);
    Store store(checkpoint);
    const auto finished = resume(hub, store, Runtime{});
    if (!verify_result(hub, finished).ok) {
        throw std::runtime_error("recovery benchmark did not resume to the serial result");
    }
}

}  // namespace
}  // namespace frontier

int main(int argc, char** argv) {
    try {
        frontier::run_benchmarks(frontier::read_options(argc, argv));
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "frontier-bench: " << error.what() << '\n';
        return 2;
    }
}
