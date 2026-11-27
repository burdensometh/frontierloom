#include "checkpoint/audit.hpp"
#include "graph/distribution.hpp"
#include "graph/manifest.hpp"
#include "kernel/contract.hpp"
#include "mailbox/exchange.hpp"
#include "workers/engine.hpp"
#include "workers/result.hpp"
#include "workers/settings.hpp"
#include "workers/wire.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <unistd.h>

namespace {

using namespace frontier;

void check(bool condition, std::string_view description) {
    if (!condition) {
        throw std::runtime_error(std::string(description));
    }
}

template<class Function>
void rejects(Function action, std::string_view text) {
    try {
        action();
    } catch (const std::exception& error) {
        check(std::string_view(error.what()).find(text) != std::string_view::npos, error.what());
        return;
    }
    throw std::runtime_error("expected rejection: " + std::string(text));
}

class Scratch {
public:
    Scratch() {
        auto path = (std::filesystem::temp_directory_path() / "frontier-tests.XXXXXX").string();
        if (::mkdtemp(path.data()) == nullptr) {
            throw std::runtime_error("cannot create native test directory");
        }
        path_ = path;
    }

    ~Scratch() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    std::filesystem::path file(const std::string& name) const {
        return path_ / name;
    }

private:
    std::filesystem::path path_;
};

Graph campus(const std::filesystem::path& samples) {
    return load_graph(samples / "campus.v1.graph");
}

Program bfs() {
    Program program;
    program.source = 10;
    return program;
}

Program pagerank(std::uint32_t limit = 100) {
    Program program;
    program.algorithm = Algorithm::pagerank;
    program.limit = limit;
    return program;
}

std::string read_bytes(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    check(static_cast<bool>(input), "sample file cannot be opened");
    std::ostringstream bytes;
    bytes << input.rdbuf();
    return bytes.str();
}

void graph_suite(const std::filesystem::path& samples) {
    const auto graph = campus(samples);
    check(graph.size() == 9 && graph.edge_count() == 13 && graph.revision() == 7,
          "campus manifest metadata");
    const auto source = graph.locate(10);
    check(graph.outgoing(source).size() == 2 && graph.incoming(source).size() == 1,
          "directed CSR is correct");

    std::ostringstream canonical;
    write_graph_v1(canonical, graph);
    check(canonical.str() == read_bytes(samples / "campus.v1.graph"), "graph v1 is byte-stable");
    Scratch scratch;
    const auto written = scratch.file("canonical.graph");
    save_graph(written, graph);
    rejects([&] { save_graph(written, graph); }, "File exists");
    check(load_graph(written).identity() == graph.identity(), "exclusive output preserves the existing manifest");
    std::istringstream reordered("FRONTIERLOOM_GRAPH 1 revision 1 vertices 2 77 20 edges 1 77 20 end");
    const auto order = read_graph_v1(reordered);
    check(order.external(0) == 20 && order.outgoing(1)[0] == 0, "stable dense IDs from unordered input");
    check(Graph({1, {20, 77}, {{77, 20}}}).identity() == order.identity(), "identity is canonical");
    check(Graph({2, {20, 77}, {{77, 20}}}).identity() != order.identity(), "revision binds identity");

    rejects([] { (void)Graph({1, {1, 1}, {}}); }, "duplicate vertex");
    rejects([] { (void)Graph({1, {1}, {{1, 2}}}); }, "unknown vertex");
    rejects([] { (void)Graph({1, {1}, {{1, 1}, {1, 1}}}); }, "duplicate directed");
    rejects([] {
        std::istringstream bad("FRONTIERLOOM_GRAPH 2 revision 1 vertices 1 0 edges 0 end");
        (void)read_graph_v1(bad);
    }, "expected 1");
    rejects([] {
        std::istringstream bad("FRONTIERLOOM_GRAPH 1 revision 1 vertices 1 0 edges 0 end extra");
        (void)read_graph_v1(bad);
    }, "trailing content");

    const Shards shards(graph, 8);
    for (Vertex vertex = 0; vertex < graph.size(); ++vertex) {
        const auto partition = shards.shard(vertex);
        check(shards.vertices(partition)[shards.position(vertex)] == vertex, "shard offset indexes the vertex");
    }
    const auto initial = PartitionMap::initial(8, 2);
    const auto expanded = plan_scale(initial, 4);
    check(expanded.moves.size() == 4 && expanded.target.generation == 2, "scale-up uses minimum moves");
    expanded.target.validate(8);
    const auto distribution = measure_distribution(graph, shards, expanded.target);
    check(distribution.cross_worker <= distribution.cross_partition &&
          distribution.cross_partition <= graph.edge_count(), "distribution accounts for cross-owner edges");
    const auto contracted = plan_scale(expanded.target, 1);
    check(contracted.moves.size() == 6 && contracted.target.generation == 3, "scale-down uses minimum moves");
    const auto unchanged = plan_scale(contracted.target, 1);
    check(unchanged.target == contracted.target && unchanged.moves.empty(), "same-size plan is a no-op");
    auto corrupted = initial;
    corrupted.owners[0] = 1;
    rejects([&] { corrupted.validate(8); }, "quotas");

    // Budgeted replanning bounds every worker's predicted outgoing edges while
    // keeping one owner per partition and deciding deterministically.
    const auto budgeted = plan_scale_budgeted(shards, graph, initial, 4, 6);
    check(budgeted.has_value(), "budget above shard peaks is feasible");
    check(budgeted->target.owners.size() == shards.count(), "each partition has exactly one owner");
    budgeted->target.validate(8);
    const auto both = [&](const PartitionMap& placement) {
        const auto loads = worker_edge_loads(shards, graph, placement);
        return *std::max_element(loads.begin(), loads.end());
    };
    check(both(budgeted->target) <= 6, "planned workers respect the edge budget");
    check(budgeted->moves.size() == 4, "budgeted plan moves only the shards it must");
    const auto replay = plan_scale_budgeted(shards, graph, initial, 4, 6);
    check(replay && replay->target == budgeted->target && replay->moves == budgeted->moves,
          "same input produces the same allocation");
    const auto equal_scores = plan_scale_budgeted(shards, graph, initial, 4, 8);
    check(equal_scores && both(equal_scores->target) <= 8 &&
          equal_scores->target.workers == 4, "generous budget stays within its bound");
    const auto impossible = plan_scale_budgeted(shards, graph, initial, 4, 3);
    check(!impossible.has_value(), "budget below every feasible peak is rejected");
}

void kernel_suite(const std::filesystem::path& samples) {
    const auto graph = campus(samples);
    const auto program = bfs();
    const auto serial = run_reference(graph, program);
    std::ostringstream vector;
    print_vector(vector, graph, Algorithm::bfs, serial.states);
    check(vector.str() == read_bytes(samples / "campus.bfs.csv"), "known BFS distances");
    check(serial.steps == 5 && serial.stop == Stop::quiescent, "final dead frontier consumed");

    auto capped = program;
    capped.limit = 2;
    const auto cropped = run_reference(graph, capped);
    check(cropped.stop == Stop::limit && cropped.steps == 2 &&
          cropped.states[graph.locate(42)].distance == unreachable, "BFS cap includes only committed layers");

    const auto rank = pagerank();
    const Graph no_edges({13, {12, 15, 18}, {}});
    const auto uniform = run_reference(no_edges, rank);
    check(uniform.stop == Stop::converged && uniform.steps == 1, "dangling-only graph converges");
    for (const auto& vertex : uniform.states) {
        check(std::abs(vertex.rank - 1.0 / 3.0) < 1e-15, "dangling mass redistributes uniformly");
    }

    const Shards shards(graph, 3);
    const auto from = graph.locate(10);
    const auto part = shards.shard(from);
    const auto image = bootstrap(graph, shards, program, 2);
    const auto result = run_partition(graph, shards, program, {0, 1, 1}, image.work(part, 0));
    check(result.states[shards.position(from)].distance == 0 &&
          result.stats.active == 1 && result.stats.consumed == 1 &&
          result.batch.messages.size() == 2, "BFS consumes its seed and scatters");
    auto false_batch = result;
    false_batch.batch.messages.front().value = 0;
    rejects([&] {
        validate_partition_output(graph, shards, program, image.work(part, 0), false_batch);
    }, "resulting vertex state");
    auto damaged = image.work(part, 0);
    damaged.inbox.push_back(damaged.inbox.front());
    rejects([&] { (void)run_partition(graph, shards, program, {0, 1, 1}, damaged); }, "invalid");
}

void exchange_suite(const std::filesystem::path&) {
    const auto closed = wire::Channel(-1);
    rejects([&] {
        (void)closed.receive(wire::Channel::Clock::now() + std::chrono::seconds(2));
    }, "channel is closed");
    const Graph graph({10, {8, 10, 11}, {{8, 11}, {10, 11}}});
    const Shards shards(graph, 3);
    const auto owners = PartitionMap::initial(shards.count(), 2);
    const std::vector<WorkerAttempt> attempts{{0, 1, 2}, {1, 1, 2}};
    const auto target = graph.locate(11);
    std::vector<MessageBatch> batches;
    for (Partition part = 0; part < shards.count(); ++part) {
        batches.push_back({3, attempts[owners.owner(part)], part, {}});
    }
    for (const auto source : {graph.locate(8), graph.locate(10)}) {
        batches[shards.shard(source)].messages.push_back({source, target, 0.25});
    }

    Exchange exchange(graph, shards, Algorithm::pagerank, 3, owners, attempts);
    auto stale = batches.front();
    --stale.attempt.incarnation;
    rejects([&] { (void)exchange.stage(stale); }, "stale");
    auto wrong_epoch = batches.front();
    ++wrong_epoch.epoch;
    rejects([&] { (void)exchange.stage(wrong_epoch); }, "open superstep");
    rejects([&] { (void)exchange.seal(); }, "unconfirmed");

    for (auto batch = batches.rbegin(); batch != batches.rend(); ++batch) {
        const auto digest = exchange.stage(*batch);
        check(digest == exchange.stage(*batch), "identical replay gets same digest");
        rejects([&] { exchange.confirm(batch->source_partition, batch->attempt, digest + 1); }, "does not acknowledge");
        exchange.confirm(batch->source_partition, batch->attempt, digest);
    }
    const auto merged = exchange.seal();
    check(merged.epoch == 4 && merged.raw_count == 2 && merged.combined_count() == 1 &&
          merged.inboxes[shards.shard(target)] == std::vector<InboxEntry>{{target, 0.5}},
          "distinct sources reduce once in the next epoch");

    const auto program = pagerank();
    const auto image = bootstrap(graph, shards, program, 2);
    Barrier barrier(graph, shards, program, owners, attempts, image.mailbox);
    rejects([&] { (void)barrier.finish(merged); }, "missing receipts");
    const auto first = owners.owner(0);
    BarrierReceipt receipt{0, attempts[first], 0, 5, {}};
    --receipt.attempt.incarnation;
    rejects([&] { barrier.acknowledge(receipt, 5); }, "lease");
    receipt.attempt = attempts[first];
    receipt.stats.active = shards.vertices(0).size();
    receipt.stats.consumed = 1;
    rejects([&] { barrier.acknowledge(receipt, 5); }, "waterline");
}

void snapshot_suite(const std::filesystem::path& samples) {
    const auto graph = campus(samples);
    const auto path = samples / "campus.epoch1.v1.flc";
    const auto fixture = load_checkpoint(path, graph);
    check(fixture.next_epoch == 1 && fixture.stop() == Stop::running &&
          fixture.mailbox.combined_count() == 2 && fixture.totals.emitted == 2,
          "stock v1 fixture has committed state and a live next frontier");
    check(audit_checkpoint(graph, fixture).combined == 2, "audit reconstructs stock v1 mailbox");
    auto forged = fixture;
    ++forged.totals.emitted;
    rejects([&] { (void)audit_checkpoint(graph, forged); }, "cumulative counts");
    std::ostringstream bytes;
    write_checkpoint_v1(bytes, fixture);
    check(bytes.str() == read_bytes(path), "checkpoint v1 is byte-stable");
    {
        auto truncated = bytes.str();
        truncated.pop_back();
        std::istringstream bad(truncated);
        rejects([&] { (void)read_checkpoint_v1(bad, graph); }, "checksum trailer");
    }
    {
        auto tampered = bytes.str();
        const auto position = tampered.find("vertex 0");
        check(position != std::string::npos, "fixture contains settled source");
        tampered[position + 7] = '1';
        std::istringstream bad(tampered);
        rejects([&] { (void)read_checkpoint_v1(bad, graph); }, "checksum mismatch");
    }
    rejects([&] { (void)load_checkpoint(path, load_graph(samples / "dangling.v1.graph")); }, "graph identity");

    Scratch scratch;
    const auto copy = scratch.file("resume.flc");
    std::filesystem::copy_file(path, copy);
    {
        Store store(copy);
        rejects([&] { const Store competing(copy); }, "exclusive checkpoint lock");
        check(store.load(graph) == fixture, "exclusive store loads persisted fixture");
        const Shards shards(graph, 8);
        auto invalid = fixture;
        ++invalid.next_epoch;
        rejects([&] { store.publish(graph, shards, invalid); }, "cursor");
        check(store.load(graph) == fixture, "rejected publish preserves the old file");
        const auto finished = resume(graph, store, Runtime{});
        check(finished.totals.resumes == 1 && verify_result(graph, finished).ok, "resume fixture through engine");
        check(store.load(graph) == finished, "read after final publication");
    }
}

void engine_suite(const std::filesystem::path& samples) {
    const auto graph = campus(samples);
    Scratch scratch;
    const Start start{bfs(), 8, 2};
    Checkpoint uninterrupted;
    {
        Store store(scratch.file("clean.flc"));
        std::set<pid_t> pids;
        std::uint32_t retired{};
        const Observer observe = [&](const Event& event) {
            if (event.kind == Event::Kind::started) {
                pids.insert(event.pids.begin(), event.pids.end());
            }
            if (event.step) {
                check(event.timing && event.timing->wire_sent > 0 && event.timing->wire_received > 0,
                      "commit measures real bidirectional IPC");
            }
            if (event.kind == Event::Kind::retired) {
                retired = event.graceful;
            }
        };
        uninterrupted = compute(graph, store, start, Runtime{}, observe);
        check(pids.size() == 2 && !pids.contains(::getpid()), "engine launched distinct child processes");
        check(retired == 2, "healthy workers acknowledge close and are reaped");
        check(verify_result(graph, uninterrupted).ok, "clean distributed BFS verifies");
    }
    {
        Runtime controls;
        controls.faults = {{FaultPoint::after_send, 0, 1}, {FaultPoint::before_commit, 1, 0},
                          {FaultPoint::before_replace, 2, 0}, {FaultPoint::after_replace, 2, 1}};
        controls.scaling = {{3, 3}, {4, 1}};
        Store store(scratch.file("faults.flc"));
        std::set<FaultPoint> faults;
        std::set<Epoch> committed;
        std::vector<Generation> leases;
        const Observer observe = [&](const Event& event) {
            if (event.fault) {
                faults.insert(event.fault->point);
            }
            if (event.step) {
                check(committed.insert(event.epoch).second, "only one visible commit per epoch");
            }
            if (event.kind == Event::Kind::started || event.kind == Event::Kind::scaled) {
                leases.push_back(event.incarnation);
            }
        };
        const auto recovered = compute(graph, store, start, controls, observe);
        check(faults.size() == 4 && committed.size() == uninterrupted.next_epoch,
              "all fault points execute and every epoch publishes");
        check(std::adjacent_find(leases.begin(), leases.end(), std::greater_equal<>()) == leases.end(),
              "released worker incarnations never recur");
        check(recovered.totals.recovered_failures == 5 && recovered.totals.scales == 2 &&
              recovered.placement.workers == 1 && recovered.placement.generation == 3,
              "worker retry and migration counters");
        check(result_vector(graph, recovered) == result_vector(graph, uninterrupted) &&
              recovered.totals.emitted == uninterrupted.totals.emitted &&
              recovered.totals.consumed == uninterrupted.totals.consumed,
              "replay and scale preserve output and avoid duplicate delivery");
    }
    {
        Store store(scratch.file("boundary.flc"));
        Runtime controls;
        controls.faults = {{FaultPoint::before_commit, 0, 0}};
        controls.retry_limit = 0;
        rejects([&] { (void)compute(graph, store, start, controls); }, "retry budget exhausted");
        const auto disk = store.load(graph);
        check(disk.next_epoch == 0 && disk.totals.emitted == 0 && disk.mailbox.combined_count() == 1,
              "precommit failure leaves prior epoch, vertex state and frontier visible");
        const auto moved = rescale(graph, store, 4, Runtime{});
        check(moved.placement.workers == 4 && moved.next_epoch == 0 && moved.mailbox == disk.mailbox,
              "standalone scale moves ownership without consuming a frontier");
        check(verify_result(graph, resume(graph, store, Runtime{})).ok, "failed run can be resumed");
    }
    {
        // The executed transition must be exactly the budgeted plan for the
        // same checkpoint, target and budget, and an infeasible budget must not
        // install any placement.
        Store store(scratch.file("budget.flc"));
        Runtime hold;
        hold.stop_after = 0;
        const auto seeded = compute(graph, store, start, hold);
        const Shards budget_shards(graph, static_cast<Partition>(seeded.placement.owners.size()));
        const auto expected = plan_scale_budgeted(budget_shards, graph, seeded.placement, 4, 6);
        check(expected.has_value(), "budgeted plan for the seeded checkpoint is feasible");

        Runtime bounded;
        bounded.edge_budget = 6;
        const auto scaled = rescale(graph, store, 4, bounded);
        check(scaled.placement == expected->target &&
              scaled.placement.generation == seeded.placement.generation + 1,
              "execution installs the same allocation the plan displays");
        const auto loads = worker_edge_loads(budget_shards, graph, scaled.placement);
        check(*std::max_element(loads.begin(), loads.end()) <= 6,
              "every worker stays within the edge budget");

        Runtime too_tight;
        too_tight.edge_budget = 3;
        rejects([&] { (void)rescale(graph, store, 4, too_tight); }, "edge budget");
        const auto after = store.load(graph);
        check(after.placement == scaled.placement && after.next_epoch == scaled.next_epoch &&
              after.placement.generation == scaled.placement.generation,
              "refused budget leaves the committed placement intact");
    }
    {
        const auto rank_graph = load_graph(samples / "dangling.v1.graph");
        const Start run{pagerank(), 7, 3};
        Store store(scratch.file("rank.flc"));
        Runtime controls;
        controls.stop_after = 4;
        const auto paused = compute(rank_graph, store, run, controls);
        check(paused.next_epoch == 4 && paused.updates() == 3 && paused.stop() == Stop::running,
              "PageRank scatter is not counted as an update");
        check(audit_checkpoint(rank_graph, paused).max_mailbox_error == 0,
              "audit rebuilds ordered PageRank reductions from stored ranks");
        const auto moved = rescale(rank_graph, store, 1, Runtime{});
        check(moved.mailbox == paused.mailbox && moved.states == paused.states, "handoff retains rank precision");
        const auto result = resume(rank_graph, store, Runtime{});
        check(result.stop() == Stop::converged && verify_result(rank_graph, result).ok,
              "PageRank agrees with long-double serial solver after recovery");
        Store unscaled(scratch.file("rank-clean.flc"));
        const auto clean = compute(rank_graph, unscaled, {pagerank(), 7, 2}, Runtime{});
        check(result_vector(rank_graph, result) == result_vector(rank_graph, clean),
              "fixed partition count makes worker count and arrival order bit-invariant");
    }
    {
        const Graph no_edges({13, {10}, {}});
        Store store(scratch.file("single.flc"));
        const auto result = compute(no_edges, store, {bfs(), 8, 8}, Runtime{});
        check(result.stop() == Stop::quiescent && result.next_epoch == 1 && verify_result(no_edges, result).ok,
              "zero-degree source and empty worker assignments still seal");
    }
}

}  // namespace

int main(int argc, char** argv) {
    using Suite = void (*)(const std::filesystem::path&);
    const std::map<std::string, Suite> suites{
        {"graph", graph_suite}, {"kernel", kernel_suite}, {"exchange", exchange_suite},
        {"snapshot", snapshot_suite}, {"engine", engine_suite}
    };
    try {
        if (argc != 3 || !suites.contains(argv[1])) {
            throw std::invalid_argument("expected SUITE and SAMPLE_DIRECTORY");
        }
        suites.at(argv[1])(argv[2]);
        std::cout << "native suite " << argv[1] << " passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "native suite failed: " << error.what() << '\n';
        return 1;
    }
}
