#include "app/inspection.hpp"
#include "graph/manifest.hpp"
#include "graph/text.hpp"
#include "workers/result.hpp"
#include "workers/settings.hpp"

#include <iostream>
#include <map>
#include <set>

namespace frontier {
namespace {

class Arguments {
public:
    Arguments(int argc, char** argv) {
        for (int index = 2; index < argc; ++index) {
            std::string key(argv[index]);
            if (key == "--verify" || key == "--quiet" || key == "--details" || key == "--audit") {
                if (!flags_.insert(key).second) {
                    throw std::invalid_argument("duplicate flag " + key);
                }
                continue;
            }
            if (!key.starts_with("--") || index + 1 >= argc ||
                std::string_view(argv[index + 1]).starts_with("--")) {
                throw std::invalid_argument("expected an option and its value near " + key);
            }
            options_[key].emplace_back(argv[++index]);
        }
    }

    [[nodiscard]] std::vector<std::string> list(const std::string& key) {
        auto option = options_.extract(key);
        return option ? std::move(option.mapped()) : std::vector<std::string>{};
    }

    [[nodiscard]] std::optional<std::string> single(const std::string& key) {
        auto values = list(key);
        if (values.size() > 1) {
            throw std::invalid_argument("duplicate option " + key);
        }
        if (values.empty()) {
            return std::nullopt;
        }
        return std::move(values.front());
    }

    [[nodiscard]] std::string need(const std::string& key) {
        auto value = single(key);
        if (!value || value->empty()) {
            throw std::invalid_argument("missing required option " + key);
        }
        return std::move(*value);
    }

    template<class Number>
    [[nodiscard]] Number number(const std::string& key, Number fallback) {
        const auto value = single(key);
        return value ? parse_number<Number>(*value, key) : fallback;
    }

    [[nodiscard]] bool flag(const std::string& key) {
        return flags_.erase(key) != 0;
    }

    void finish() const {
        if (!options_.empty()) {
            throw std::invalid_argument("unexpected option " + options_.begin()->first);
        }
        if (!flags_.empty()) {
            throw std::invalid_argument("unexpected flag " + *flags_.begin());
        }
    }

private:
    std::map<std::string, std::vector<std::string>> options_;
    std::set<std::string> flags_;
};

struct Output {
    std::optional<std::filesystem::path> vector;
    bool verify{};
    bool quiet{};
};

void usage(std::ostream& out) {
    out << R"(FrontierLoom v1
  compute --graph FILE --checkpoint FILE --algorithm bfs|pagerank [options]
  resume --graph FILE --checkpoint FILE [options]
  scale --graph FILE --checkpoint FILE --workers N [--config FILE] [--timeout-ms N] [--retry N] [--edge-budget EDGES]
  inspect --graph FILE --checkpoint FILE [--vector FILE|-] [--verify] [--details] [--audit]
  plan --graph FILE --checkpoint FILE --workers N [--edge-budget EDGES]
  reference --graph FILE --algorithm bfs|pagerank [program options] [--vector FILE|-]
  canonicalize --graph FILE --out FILE

BFS requires --source ID. PageRank supports --damping D and --tolerance E.
Both support --limit N and --config FILE.
Compute accepts --partitions N and --workers N.
Compute, resume and scale accept --edge-budget EDGES to bound each worker's
predicted outgoing edges; a scale that cannot meet the budget is refused.
Compute and resume accept --stop-after N, --scale-at EPOCH:WORKERS,
--fault POINT:EPOCH[:WORKER], --timeout-ms N, --retry N, --vector FILE|-,
--verify and --quiet. Repeat --scale-at and --fault for different boundaries.
)";
}

Settings settings_from(Arguments& args) {
    const auto file = args.single("--config");
    return file ? read_settings(*file) : Settings{};
}

Program program_from(Arguments& args, const Settings& settings) {
    Program program;
    const auto algorithm = args.need("--algorithm");
    if (algorithm == "bfs") {
        program.source = parse_number<VertexId>(args.need("--source"), "--source");
        program.limit = args.number("--limit", settings.bfs_limit);
    } else if (algorithm == "pagerank") {
        program.algorithm = Algorithm::pagerank;
        program.damping = args.number("--damping", settings.damping);
        program.tolerance = args.number("--tolerance", settings.tolerance);
        program.limit = args.number("--limit", settings.rank_limit);
    } else {
        throw std::invalid_argument("unknown algorithm " + algorithm);
    }
    return program;
}

Runtime runtime_from(Arguments& args, const Settings& settings, bool live) {
    Runtime runtime;
    runtime.timeout = std::chrono::milliseconds(
        args.number("--timeout-ms", static_cast<std::uint32_t>(settings.timeout.count())));
    runtime.retry_limit = args.number("--retry", settings.retry_limit);
    if (const auto budget = args.single("--edge-budget")) {
        runtime.edge_budget = parse_number<std::uint64_t>(*budget, "--edge-budget");
    } else {
        runtime.edge_budget = settings.edge_budget;
    }
    if (live) {
        if (const auto count = args.single("--stop-after")) {
            runtime.stop_after = parse_number<std::uint32_t>(*count, "--stop-after");
        }
        for (const auto& action : args.list("--scale-at")) {
            runtime.scaling.push_back(parse_scale(action));
        }
        for (const auto& fault : args.list("--fault")) {
            runtime.faults.push_back(parse_fault(fault));
        }
    }
    return runtime;
}

Output output_from(Arguments& args) {
    const auto path = args.single("--vector");
    return {path ? std::optional<std::filesystem::path>(*path) : std::nullopt,
            args.flag("--verify"), args.flag("--quiet")};
}

void output_vector(const Output& output, const Graph& graph, Algorithm algorithm,
                   std::span<const VertexState> states) {
    if (!output.vector) {
        return;
    }
    if (*output.vector == "-") {
        print_vector(std::cout, graph, algorithm, states);
    } else {
        create_vector(*output.vector, graph, algorithm, states);
    }
}

int show(const Graph& graph, const Checkpoint& result, const Output& output) {
    print_totals(std::cerr, result);
    if (output.vector) {
        output_vector(output, graph, result.program.algorithm, result_vector(graph, result));
    }
    if (!output.verify) {
        return 0;
    }
    if (result.stop() == Stop::running) {
        std::cerr << "verify=deferred reason=paused\n";
        return 0;
    }
    const auto comparison = verify_result(graph, result);
    print_comparison(std::cerr, comparison);
    return comparison.ok ? 0 : 3;
}

int mutable_command(std::string_view command, Arguments& args, const Graph& graph) {
    const auto path = args.need("--checkpoint");
    const auto settings = settings_from(args);
    const auto runtime = runtime_from(args, settings, command != "scale");
    const auto output = command == "scale" ? Output{} : output_from(args);
    const Observer report = output.quiet ? Observer{} : Observer([](const Event& event) {
        print_event(std::cerr, event);
    });

    if (command == "compute") {
        const Start start{program_from(args, settings),
                          args.number("--partitions", settings.partitions),
                          args.number("--workers", settings.workers)};
        args.finish();
        Store store(path);
        return show(graph, compute(graph, store, start, runtime, report), output);
    }
    if (command == "scale") {
        const auto workers = parse_number<Worker>(args.need("--workers"), "--workers");
        args.finish();
        Store store(path);
        return show(graph, rescale(graph, store, workers, runtime, report), output);
    }

    args.finish();
    Store store(path);
    return show(graph, resume(graph, store, runtime, report), output);
}

int run(std::string_view command, Arguments& args) {
    if (command != "compute" && command != "resume" && command != "scale" &&
        command != "inspect" && command != "plan" && command != "reference" && command != "canonicalize") {
        throw std::invalid_argument("unknown command " + std::string(command));
    }
    const auto graph = load_graph(args.need("--graph"));
    if (command == "compute" || command == "resume" || command == "scale") {
        return mutable_command(command, args, graph);
    }
    if (command == "inspect") {
        const auto path = args.need("--checkpoint");
        const auto output = output_from(args);
        const bool details = args.flag("--details");
        const bool audit = args.flag("--audit");
        args.finish();
        const auto image = load_checkpoint(path, graph);
        if (details) {
            print_checkpoint_detail(std::cerr, graph, image);
        }
        if (audit) {
            print_audit(std::cerr, graph, image);
        }
        return show(graph, image, output);
    }
    if (command == "plan") {
        const auto path = args.need("--checkpoint");
        const auto workers = parse_number<Worker>(args.need("--workers"), "--workers");
        const auto settings = settings_from(args);
        const auto budget = args.single("--edge-budget");
        args.finish();
        const auto image = load_checkpoint(path, graph);
        const auto limit = budget ? std::optional<std::uint64_t>(
                            parse_number<std::uint64_t>(*budget, "--edge-budget"))
                                  : settings.edge_budget;
        if (limit) {
            print_scale_plan(std::cout, graph, image, workers, *limit);
        } else {
            print_scale_plan(std::cout, graph, image, workers);
        }
        return 0;
    }
    if (command == "reference") {
        const auto program = program_from(args, settings_from(args));
        const auto output = output_from(args);
        if (output.verify) {
            throw std::invalid_argument("--verify applies to a committed distributed result");
        }
        args.finish();
        const auto serial = run_reference(graph, program);
        std::cerr << "reference algorithm=" << name(program.algorithm) << " updates=" << serial.steps
                  << " stop=" << name(serial.stop) << " residual=" << serial.residual << '\n';
        output_vector(output, graph, program.algorithm, serial.states);
        return 0;
    }

    const auto path = args.need("--out");
    args.finish();
    save_graph(path, graph);
    std::cerr << "graph revision=" << graph.revision() << " identity=" << graph.identity()
              << " vertices=" << graph.size() << " edges=" << graph.edge_count() << '\n';
    return 0;
}

}  // namespace
}  // namespace frontier

int main(int argc, char** argv) {
    try {
        if (argc < 2 || std::string_view(argv[1]) == "--help") {
            frontier::usage(std::cout);
            return argc < 2 ? 2 : 0;
        }
        frontier::Arguments args(argc, argv);
        return frontier::run(argv[1], args);
    } catch (const std::exception& error) {
        std::cerr << "frontierloom: " << error.what() << '\n';
        return 2;
    }
}
