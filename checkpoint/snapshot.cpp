#include "checkpoint/snapshot.hpp"
#include "graph/text.hpp"

#include <bit>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <locale>
#include <sstream>
#include <stdexcept>

namespace frontier {
namespace {

constexpr double mass_epsilon = 1e-9;
constexpr auto max_snapshot_bytes = 128U * 1024U * 1024U;
constexpr auto max_shard_bytes = 128U * 1024U * 1024U;

Algorithm read_algorithm(TextReader& reader) {
    const auto algorithm = reader.token("algorithm");
    if (algorithm == "bfs") {
        return Algorithm::bfs;
    }
    if (algorithm == "pagerank") {
        return Algorithm::pagerank;
    }
    throw std::invalid_argument("unknown checkpoint program " + algorithm);
}

Stop read_stop(TextReader& reader) {
    const auto token = reader.token("stop");
    for (const auto stop : {Stop::running, Stop::quiescent, Stop::converged, Stop::limit}) {
        if (token == name(stop)) {
            return stop;
        }
    }
    throw std::invalid_argument("unknown checkpoint stop reason " + token);
}

StepSummary read_summary(TextReader& reader) {
    StepSummary step;
    step.epoch = reader.number<Epoch>("step epoch");
    step.topology = reader.number<Generation>("step topology");
    step.receipts = reader.number<std::uint32_t>("step receipts");
    step.stats.active = reader.number<std::uint64_t>("step active");
    step.stats.consumed = reader.number<std::uint64_t>("step consumed");
    step.stats.emitted = reader.number<std::uint64_t>("step emitted");
    step.stats.dangling = reader.number<double>("step dangling");
    step.stats.residual = reader.number<double>("step residual");
    step.stats.rank_mass = reader.number<double>("step mass");
    step.combined = reader.number<std::uint64_t>("step combined");
    step.mailbox = reader.number<Digest>("step mailbox digest");
    step.stop = read_stop(reader);
    return step;
}

Totals read_totals(TextReader& reader) {
    return {reader.number<std::uint64_t>("total active"),
            reader.number<std::uint64_t>("total consumed"),
            reader.number<std::uint64_t>("total emitted"),
            reader.number<std::uint64_t>("total combined"),
            reader.number<std::uint64_t>("recovered failures"),
            reader.number<std::uint64_t>("resumes"),
            reader.number<std::uint64_t>("scales"),
            reader.number<std::uint64_t>("moved partitions")};
}

void read_partitions(TextReader& reader, const Shards& shards, Checkpoint& checkpoint) {
    checkpoint.states.resize(shards.count());
    checkpoint.mailbox.inboxes.resize(shards.count());
    for (Partition part = 0; part < shards.count(); ++part) {
        reader.expect("shard");
        const auto index = reader.number<Partition>("shard ID");
        const auto count = reader.number<std::size_t>("state count");
        const auto inbox_size = reader.number<std::size_t>("inbox count");
        if (index != part || count != shards.vertices(part).size() || inbox_size > count) {
            throw std::invalid_argument("checkpoint partition counts disagree with graph");
        }
        auto& states = checkpoint.states[part];
        states.reserve(count);
        for (std::size_t item = 0; item < count; ++item) {
            reader.expect("vertex");
            auto distance = reader.token("BFS distance");
            const auto rank = reader.number<double>("vertex rank");
            states.push_back({distance == "inf" ? unreachable :
                parse_number<std::uint32_t>(distance, "BFS distance"), rank});
        }
        auto& inbox = checkpoint.mailbox.inboxes[part];
        inbox.reserve(inbox_size);
        for (std::size_t item = 0; item < inbox_size; ++item) {
            reader.expect("message");
            const auto target = reader.number<Vertex>("message target");
            const auto value = reader.number<double>("message value");
            inbox.push_back({target, value});
        }
    }
}

std::string read_stream(std::istream& input, std::string_view subject, std::size_t limit) {
    std::string file;
    char buffer[8192];
    while (input.read(buffer, sizeof(buffer)) || input.gcount() > 0) {
        file.append(buffer, static_cast<std::size_t>(input.gcount()));
        if (file.size() > limit) {
            throw std::invalid_argument(std::string(subject) + " exceeds the byte limit");
        }
    }
    if (!input.eof() || input.bad()) {
        throw std::runtime_error(std::string(subject) + " read failed");
    }
    return file;
}

// Verifies the trailing checksum line and returns the payload it covers,
// including the newline that terminates the `end` record.
std::string strip_checksum(std::string file, std::string_view subject) {
    const auto separator = file.rfind("\nchecksum ");
    if (separator == std::string::npos || file.back() != '\n') {
        throw std::invalid_argument(std::string(subject) + " has no complete checksum trailer");
    }
    constexpr auto label_size = std::string_view("\nchecksum ").size();
    const auto checksum_at = separator + label_size;
    const auto checksum = parse_number<Digest>(
        std::string_view(file).substr(checksum_at, file.size() - checksum_at - 1), "checksum");
    file.resize(separator + 1);
    Hash hash;
    hash.bytes(file);
    if (hash.value() != checksum) {
        throw std::invalid_argument(std::string(subject) + " checksum mismatch");
    }
    return file;
}

std::string read_checked_payload(std::istream& input) {
    return strip_checksum(read_stream(input, "checkpoint", max_snapshot_bytes), "checkpoint");
}

void write_summary(std::ostream& output, const StepSummary& step) {
    output << "step " << step.epoch << ' ' << step.topology << ' ' << step.receipts
           << ' ' << step.stats.active << ' ' << step.stats.consumed << ' ' << step.stats.emitted
           << ' ' << step.stats.dangling << ' ' << step.stats.residual << ' ' << step.stats.rank_mass
           << ' ' << step.combined << ' ' << step.mailbox << ' ' << name(step.stop) << '\n';
}

void write_partitions(std::ostream& output, const Checkpoint& checkpoint) {
    for (Partition part = 0; part < checkpoint.states.size(); ++part) {
        const auto& states = checkpoint.states[part];
        const auto& inbox = checkpoint.mailbox.inboxes.at(part);
        output << "shard " << part << ' ' << states.size() << ' ' << inbox.size() << '\n';
        for (const auto& state : states) {
            output << "vertex ";
            if (state.distance == unreachable) {
                output << "inf";
            } else {
                output << state.distance;
            }
            output << ' ' << state.rank << '\n';
        }
        for (const auto& entry : inbox) {
            output << "message " << entry.target << ' ' << entry.value << '\n';
        }
    }
}

void validate_totals(const Checkpoint& checkpoint, const Graph& graph, Partition partitions) {
    const auto& totals = checkpoint.totals;
    const auto epoch = checkpoint.next_epoch;
    if (totals.active > graph.size() * epoch || totals.consumed > graph.size() * epoch ||
        totals.emitted > graph.edge_count() * epoch ||
        totals.combined > totals.emitted || totals.scales != checkpoint.placement.generation - 1 ||
        totals.moved_partitions < totals.scales ||
        (totals.scales == 0 && totals.moved_partitions != 0) ||
        (totals.scales > 0 && (totals.moved_partitions - 1) / partitions >= totals.scales)) {
        throw std::invalid_argument("checkpoint cumulative counts are inconsistent");
    }
    if (checkpoint.last &&
        (checkpoint.last->stats.active > totals.active || checkpoint.last->stats.consumed > totals.consumed ||
         checkpoint.last->stats.emitted > totals.emitted || checkpoint.last->combined > totals.combined)) {
        throw std::invalid_argument("checkpoint totals precede the last committed step");
    }
}

void validate_vectors(const Checkpoint& checkpoint, const Graph& graph, const Shards& shards) {
    std::size_t reached{};
    Sum rank;
    Sum dangling;
    Sum inbox_mass;
    if (checkpoint.states.size() != shards.count()) {
        throw std::invalid_argument("checkpoint vertex image is missing partitions");
    }
    for (Partition part = 0; part < shards.count(); ++part) {
        const auto members = shards.vertices(part);
        const auto& states = checkpoint.states[part];
        if (states.size() != members.size()) {
            throw std::invalid_argument("checkpoint vertex image does not cover its partition");
        }
        for (std::size_t item = 0; item < states.size(); ++item) {
            const auto& state = states[item];
            validate_state(checkpoint.program.algorithm, state, graph.size());
            if (state.distance != unreachable) {
                if (state.distance >= checkpoint.next_epoch ||
                    (state.distance == 0) != (graph.external(members[item]) == checkpoint.program.source)) {
                    throw std::invalid_argument("BFS checkpoint contains an impossible distance");
                }
                ++reached;
            }
            rank.add(state.rank);
            if (state.rank > 0 && graph.outgoing(members[item]).empty()) {
                dangling.add(state.rank);
            }
        }
        for (const auto& entry : checkpoint.mailbox.inboxes[part]) {
            if (checkpoint.program.algorithm == Algorithm::bfs &&
                entry.value != static_cast<double>(checkpoint.next_epoch)) {
                throw std::invalid_argument("BFS checkpoint contains an out-of-epoch distance message");
            }
            inbox_mass.add(entry.value);
        }
    }

    if (checkpoint.program.algorithm == Algorithm::bfs && reached != checkpoint.totals.active) {
        throw std::invalid_argument("BFS checkpoint reach count disagrees with progress");
    }
    if (checkpoint.program.algorithm == Algorithm::pagerank &&
        (std::abs(rank.value() - 1) > mass_epsilon ||
         (checkpoint.last && (std::abs(dangling.value() - checkpoint.last->stats.dangling) > mass_epsilon ||
          std::abs(inbox_mass.value() + dangling.value() - 1) > mass_epsilon)))) {
        throw std::invalid_argument("checkpoint does not conserve PageRank mass");
    }
}

std::string shard_filename(Partition part) {
    std::ostringstream name;
    name << "part-" << std::setw(4) << std::setfill('0') << part << ".shard";
    return name.str();
}

void write_shard_payload(std::ostream& output, std::uint64_t revision, Digest identity,
                         const ShardImage& image) {
    output << "FRONTIERLOOM_SHARD 1\ngraph " << revision << ' ' << identity
           << "\npartition " << image.partition << "\nepoch " << image.epoch
           << "\nalgorithm " << name(image.algorithm) << "\nvertices " << image.states.size()
           << "\ninbox " << image.inbox.size() << '\n';
    for (const auto& state : image.states) {
        output << "vertex ";
        if (state.distance == unreachable) {
            output << "inf";
        } else {
            output << state.distance;
        }
        output << ' ' << state.rank << '\n';
    }
    for (const auto& entry : image.inbox) {
        output << "message " << entry.target << ' ' << entry.value << '\n';
    }
    output << "end\n";
    if (!output) {
        throw std::runtime_error("writing checkpoint shard failed");
    }
}

ShardImage parse_shard_payload(const Graph& graph, std::string payload) {
    std::istringstream stream(std::move(payload));
    stream.imbue(std::locale::classic());
    TextReader reader(stream, "checkpoint shard");
    reader.expect("FRONTIERLOOM_SHARD");
    reader.expect("1");
    reader.expect("graph");
    const auto revision = reader.number<std::uint64_t>("shard graph revision");
    const auto identity = reader.number<Digest>("shard graph ID");
    if (revision != graph.revision() || identity != graph.identity()) {
        throw std::invalid_argument("checkpoint shard belongs to a different graph");
    }
    ShardImage image;
    reader.expect("partition");
    image.partition = reader.number<Partition>("shard partition");
    reader.expect("epoch");
    image.epoch = reader.number<Epoch>("shard epoch");
    reader.expect("algorithm");
    image.algorithm = read_algorithm(reader);
    reader.expect("vertices");
    const auto vertices = reader.number<std::size_t>("shard vertex count");
    reader.expect("inbox");
    const auto inbox_size = reader.number<std::size_t>("shard inbox count");
    if (vertices > graph.size() || inbox_size > vertices) {
        throw std::invalid_argument("checkpoint shard counts disagree with the graph");
    }
    image.states.reserve(vertices);
    for (std::size_t item = 0; item < vertices; ++item) {
        reader.expect("vertex");
        auto distance = reader.token("BFS distance");
        const auto rank = reader.number<double>("vertex rank");
        VertexState state{distance == "inf" ? unreachable :
            parse_number<std::uint32_t>(distance, "BFS distance"), rank};
        validate_state(image.algorithm, state, graph.size());
        if (image.algorithm == Algorithm::bfs && state.distance != unreachable &&
            state.distance >= image.epoch) {
            throw std::invalid_argument("checkpoint shard carries a BFS distance beyond its epoch");
        }
        image.states.push_back(state);
    }
    image.inbox.reserve(inbox_size);
    for (std::size_t item = 0; item < inbox_size; ++item) {
        reader.expect("message");
        const auto target = reader.number<Vertex>("message target");
        const auto value = reader.number<double>("message value");
        if (target >= graph.size() || (item > 0 && target <= image.inbox.back().target) ||
            !valid_message(image.algorithm, value) ||
            (image.algorithm == Algorithm::bfs && value != static_cast<double>(image.epoch))) {
            throw std::invalid_argument("checkpoint shard has an invalid inbox entry");
        }
        image.inbox.push_back({target, value});
    }
    reader.expect("end");
    reader.finish();
    return image;
}

}  // namespace

std::uint32_t Checkpoint::updates() const {
    if (!last) {
        return 0;
    }
    return static_cast<std::uint32_t>(last->epoch + (program.algorithm == Algorithm::bfs ? 1U : 0U));
}

StepInput Checkpoint::work(Partition partition, double original_dangling) const {
    return {next_epoch, partition, last ? last->stats.dangling : original_dangling,
            states.at(partition), mailbox.inboxes.at(partition)};
}

void Checkpoint::validate(const Graph& graph, const Shards& shards) const {
    if (graph.revision() != graph_revision || graph.identity() != graph_identity) {
        throw std::invalid_argument("checkpoint belongs to a different graph revision or adjacency");
    }
    program.validate(graph);
    placement.validate(shards.count());
    mailbox.validate(shards, program.algorithm);
    if (next_epoch != mailbox.epoch || next_epoch > static_cast<Epoch>(program.limit) + 1 ||
        (next_epoch == 0) != !last ||
        (next_epoch != 0 && incarnation == 0) ||
        totals.resumes > incarnation || totals.scales > incarnation) {
        throw std::invalid_argument("checkpoint cursor, incarnation or topology is invalid");
    }

    if (last) {
        validate_progress(program, graph, *last);
        if (last->epoch + 1 != next_epoch || last->topology > placement.generation ||
            last->receipts != shards.count() || last->stats.emitted != mailbox.raw_count ||
            last->combined != mailbox.combined_count() || last->mailbox != mailbox_digest(mailbox)) {
            throw std::invalid_argument("checkpoint cursor does not match its committed barrier");
        }
    } else if (mailbox != initial_mailbox(graph, shards, program) ||
               states != initial_state(graph, shards, program)) {
        throw std::invalid_argument("checkpoint epoch zero does not contain the initial image");
    }
    validate_totals(*this, graph, shards.count());
    validate_vectors(*this, graph, shards);
}

Checkpoint bootstrap(const Graph& graph, const Shards& shards, Program program, Worker workers) {
    Checkpoint checkpoint;
    checkpoint.graph_revision = graph.revision();
    checkpoint.graph_identity = graph.identity();
    checkpoint.program = program;
    checkpoint.placement = PartitionMap::initial(shards.count(), workers);
    checkpoint.states = initial_state(graph, shards, program);
    checkpoint.mailbox = initial_mailbox(graph, shards, program);
    return checkpoint;
}

void advance(Checkpoint& checkpoint, const StepSummary& step,
             std::vector<std::vector<VertexState>> states, Mailbox mailbox) {
    if (checkpoint.stop() != Stop::running || checkpoint.next_epoch != step.epoch ||
        checkpoint.placement.generation != step.topology || mailbox.epoch != step.epoch + 1) {
        throw std::logic_error("attempted to advance outside the committed BSP cursor");
    }
    checkpoint.last = step;
    checkpoint.next_epoch = mailbox.epoch;
    checkpoint.totals.active += step.stats.active;
    checkpoint.totals.consumed += step.stats.consumed;
    checkpoint.totals.emitted += step.stats.emitted;
    checkpoint.totals.combined += step.combined;
    checkpoint.states = std::move(states);
    checkpoint.mailbox = std::move(mailbox);
}

Checkpoint read_checkpoint_v1(std::istream& input, const Graph& graph) {
    std::istringstream payload(read_checked_payload(input));
    payload.imbue(std::locale::classic());
    TextReader reader(payload, "checkpoint");
    reader.expect("FRONTIERLOOM_CHECKPOINT");
    reader.expect("1");
    reader.expect("graph");
    Checkpoint checkpoint;
    checkpoint.graph_revision = reader.number<std::uint64_t>("graph revision");
    checkpoint.graph_identity = reader.number<Digest>("graph ID");
    if (checkpoint.graph_identity != graph.identity() || checkpoint.graph_revision != graph.revision()) {
        throw std::invalid_argument("checkpoint graph identity mismatch");
    }
    reader.expect("program");
    checkpoint.program = {read_algorithm(reader), reader.number<VertexId>("source ID"),
                          reader.number<double>("damping"), reader.number<double>("tolerance"),
                          reader.number<std::uint32_t>("step limit")};
    checkpoint.program.validate(graph);

    reader.expect("cursor");
    checkpoint.next_epoch = reader.number<Epoch>("next epoch");
    checkpoint.incarnation = reader.number<Generation>("incarnation");
    reader.expect("placement");
    checkpoint.placement.generation = reader.number<Generation>("topology");
    checkpoint.placement.workers = reader.number<Worker>("worker count");
    const auto partitions = reader.number<Partition>("partition count");
    const Shards shards(graph, partitions);
    reader.expect("owners");
    checkpoint.placement.owners.reserve(partitions);
    for (Partition part = 0; part < partitions; ++part) {
        checkpoint.placement.owners.push_back(reader.number<Worker>("partition owner"));
    }
    checkpoint.placement.validate(partitions);
    reader.expect("totals");
    checkpoint.totals = read_totals(reader);
    reader.expect("last");
    const auto last = reader.number<unsigned>("last step flag");
    if (last == 1) {
        reader.expect("step");
        checkpoint.last = read_summary(reader);
    } else if (last != 0) {
        throw std::invalid_argument("unknown last step flag");
    }

    reader.expect("mailbox");
    checkpoint.mailbox.epoch = reader.number<Epoch>("mailbox epoch");
    checkpoint.mailbox.raw_count = reader.number<std::uint64_t>("raw message count");
    read_partitions(reader, shards, checkpoint);
    reader.expect("end");
    reader.finish();
    checkpoint.validate(graph, shards);
    return checkpoint;
}

void write_checkpoint_v1(std::ostream& output, const Checkpoint& checkpoint) {
    std::ostringstream payload;
    payload.imbue(std::locale::classic());
    payload << std::setprecision(std::numeric_limits<double>::max_digits10);
    payload << "FRONTIERLOOM_CHECKPOINT 1\ngraph " << checkpoint.graph_revision << ' '
            << checkpoint.graph_identity << "\nprogram " << name(checkpoint.program.algorithm)
            << ' ' << checkpoint.program.source << ' ' << checkpoint.program.damping << ' '
            << checkpoint.program.tolerance << ' ' << checkpoint.program.limit
            << "\ncursor " << checkpoint.next_epoch << ' ' << checkpoint.incarnation
            << "\nplacement " << checkpoint.placement.generation << ' ' << checkpoint.placement.workers
            << ' ' << checkpoint.placement.owners.size() << "\nowners";
    for (const auto owner : checkpoint.placement.owners) {
        payload << ' ' << owner;
    }
    const auto& total = checkpoint.totals;
    payload << "\ntotals " << total.active << ' ' << total.consumed << ' ' << total.emitted
            << ' ' << total.combined << ' ' << total.recovered_failures << ' ' << total.resumes
            << ' ' << total.scales << ' ' << total.moved_partitions
            << "\nlast " << (checkpoint.last ? 1 : 0) << '\n';
    if (checkpoint.last) {
        write_summary(payload, *checkpoint.last);
    }
    payload << "mailbox " << checkpoint.mailbox.epoch << ' ' << checkpoint.mailbox.raw_count << '\n';
    write_partitions(payload, checkpoint);
    payload << "end\n";
    auto bytes = std::move(payload).str();
    if (!payload || bytes.size() >= max_snapshot_bytes - 32) {
        throw std::runtime_error("serializing checkpoint exceeds supported size");
    }
    Hash hash;
    hash.bytes(bytes);
    output << bytes << "checksum " << std::to_string(hash.value()) << '\n';
    if (!output) {
        throw std::runtime_error("writing checkpoint failed");
    }
}

std::filesystem::path shard_root(const std::filesystem::path& checkpoint) {
    return std::filesystem::path(checkpoint.string() + std::string(shard_root_suffix));
}

std::string generation_directory(Generation generation) {
    constexpr std::string_view hex = "0123456789abcdef";
    std::string result = "g";
    for (int shift = 60; shift >= 0; shift -= 4) {
        result.push_back(hex[static_cast<std::size_t>((generation >> shift) & 0xFU)]);
    }
    return result;
}

std::filesystem::path shard_path(const std::filesystem::path& root, Generation generation,
                                 Partition partition) {
    return root / generation_directory(generation) / shard_filename(partition);
}

std::vector<ShardFile> serialize_shards(const Shards& shards, const Checkpoint& checkpoint) {
    if (checkpoint.states.size() != shards.count() ||
        checkpoint.mailbox.inboxes.size() != shards.count()) {
        throw std::invalid_argument("checkpoint does not carry one image per partition");
    }
    std::vector<ShardFile> files;
    files.reserve(shards.count());
    for (Partition part = 0; part < shards.count(); ++part) {
        if (checkpoint.states[part].size() != shards.vertices(part).size()) {
            throw std::invalid_argument("checkpoint partition image does not match its shard");
        }
        ShardImage image{checkpoint.next_epoch, part, checkpoint.program.algorithm,
                         checkpoint.states[part], checkpoint.mailbox.inboxes[part]};
        std::ostringstream payload;
        payload.imbue(std::locale::classic());
        payload << std::setprecision(std::numeric_limits<double>::max_digits10);
        write_shard_payload(payload, checkpoint.graph_revision, checkpoint.graph_identity, image);
        auto body = std::move(payload).str();
        if (body.size() >= max_shard_bytes - 32) {
            throw std::runtime_error("serializing checkpoint shard exceeds supported size");
        }
        Hash hash;
        hash.bytes(body);
        ShardFile file;
        file.partition = part;
        file.checksum = hash.value();
        file.payload = body + "checksum " + std::to_string(hash.value()) + '\n';
        file.bytes = file.payload.size();
        files.push_back(std::move(file));
    }
    return files;
}

Digest checkpoint_generation(const Checkpoint& checkpoint, std::span<const ShardFile> shards) {
    Hash hash;
    hash.bytes("FrontierLoom generation v2");
    hash.number(checkpoint.graph_revision);
    hash.number(checkpoint.graph_identity);
    hash.number(static_cast<std::uint64_t>(checkpoint.program.algorithm));
    hash.number(checkpoint.program.source);
    hash.number(std::bit_cast<std::uint64_t>(checkpoint.program.damping));
    hash.number(std::bit_cast<std::uint64_t>(checkpoint.program.tolerance));
    hash.number(checkpoint.program.limit);
    hash.number(checkpoint.next_epoch);
    hash.number(checkpoint.incarnation);
    hash.number(checkpoint.placement.generation);
    hash.number(checkpoint.placement.workers);
    hash.number(checkpoint.placement.owners.size());
    for (const auto owner : checkpoint.placement.owners) {
        hash.number(owner);
    }
    const auto& total = checkpoint.totals;
    hash.number(total.active);
    hash.number(total.consumed);
    hash.number(total.emitted);
    hash.number(total.combined);
    hash.number(total.recovered_failures);
    hash.number(total.resumes);
    hash.number(total.scales);
    hash.number(total.moved_partitions);
    hash.number(checkpoint.last ? 1 : 0);
    if (checkpoint.last) {
        const auto& step = *checkpoint.last;
        hash.number(step.epoch);
        hash.number(step.topology);
        hash.number(step.receipts);
        hash.number(step.stats.active);
        hash.number(step.stats.consumed);
        hash.number(step.stats.emitted);
        hash.number(std::bit_cast<std::uint64_t>(step.stats.dangling));
        hash.number(std::bit_cast<std::uint64_t>(step.stats.residual));
        hash.number(std::bit_cast<std::uint64_t>(step.stats.rank_mass));
        hash.number(step.combined);
        hash.number(step.mailbox);
        hash.number(static_cast<std::uint64_t>(step.stop));
    }
    hash.number(checkpoint.mailbox.epoch);
    hash.number(checkpoint.mailbox.raw_count);
    hash.number(checkpoint.mailbox.combined_count());
    hash.number(shards.size());
    for (const auto& shard : shards) {
        hash.number(shard.partition);
        hash.number(shard.bytes);
        hash.number(shard.checksum);
    }
    return hash.value();
}

void write_manifest_v2(std::ostream& output, const Checkpoint& checkpoint, Generation generation,
                       Generation previous, std::span<const ShardFile> shards) {
    std::ostringstream payload;
    payload.imbue(std::locale::classic());
    payload << std::setprecision(std::numeric_limits<double>::max_digits10);
    payload << "FRONTIERLOOM_CHECKPOINT 2\ngraph " << checkpoint.graph_revision << ' '
            << checkpoint.graph_identity << "\nprogram " << name(checkpoint.program.algorithm)
            << ' ' << checkpoint.program.source << ' ' << checkpoint.program.damping << ' '
            << checkpoint.program.tolerance << ' ' << checkpoint.program.limit
            << "\ncursor " << checkpoint.next_epoch << ' ' << checkpoint.incarnation
            << "\nplacement " << checkpoint.placement.generation << ' ' << checkpoint.placement.workers
            << ' ' << checkpoint.placement.owners.size() << "\nowners";
    for (const auto owner : checkpoint.placement.owners) {
        payload << ' ' << owner;
    }
    const auto& total = checkpoint.totals;
    payload << "\ntotals " << total.active << ' ' << total.consumed << ' ' << total.emitted
            << ' ' << total.combined << ' ' << total.recovered_failures << ' ' << total.resumes
            << ' ' << total.scales << ' ' << total.moved_partitions
            << "\nlast " << (checkpoint.last ? 1 : 0) << '\n';
    if (checkpoint.last) {
        write_summary(payload, *checkpoint.last);
    }
    payload << "mailbox " << checkpoint.mailbox.epoch << ' ' << checkpoint.mailbox.raw_count
            << "\ngeneration " << generation << "\nprevious " << previous
            << "\nshards " << shards.size() << '\n';
    for (const auto& shard : shards) {
        payload << "shard " << shard.partition << ' ' << checkpoint.next_epoch << ' '
                << shard.bytes << ' ' << shard.checksum << '\n';
    }
    payload << "end\n";
    auto bytes = std::move(payload).str();
    if (!payload || bytes.size() >= max_snapshot_bytes - 32) {
        throw std::runtime_error("serializing checkpoint manifest exceeds supported size");
    }
    Hash hash;
    hash.bytes(bytes);
    output << bytes << "checksum " << std::to_string(hash.value()) << '\n';
    if (!output) {
        throw std::runtime_error("writing checkpoint manifest failed");
    }
}

ShardImage read_shard_v2(std::istream& input, const Graph& graph) {
    return parse_shard_payload(graph, strip_checksum(
        read_stream(input, "checkpoint shard", max_shard_bytes), "checkpoint shard"));
}

Checkpoint read_checkpoint_v2(std::istream& manifest, const std::filesystem::path& checkpoint,
                              const Graph& graph) {
    auto file = strip_checksum(read_stream(manifest, "checkpoint manifest", max_snapshot_bytes),
                               "checkpoint manifest");
    std::istringstream payload(std::move(file));
    payload.imbue(std::locale::classic());
    TextReader reader(payload, "checkpoint manifest");
    reader.expect("FRONTIERLOOM_CHECKPOINT");
    reader.expect("2");
    reader.expect("graph");
    Checkpoint result;
    result.graph_revision = reader.number<std::uint64_t>("graph revision");
    result.graph_identity = reader.number<Digest>("graph ID");
    if (result.graph_identity != graph.identity() || result.graph_revision != graph.revision()) {
        throw std::invalid_argument("checkpoint graph identity mismatch");
    }
    reader.expect("program");
    result.program = {read_algorithm(reader), reader.number<VertexId>("source ID"),
                      reader.number<double>("damping"), reader.number<double>("tolerance"),
                      reader.number<std::uint32_t>("step limit")};
    result.program.validate(graph);

    reader.expect("cursor");
    result.next_epoch = reader.number<Epoch>("next epoch");
    result.incarnation = reader.number<Generation>("incarnation");
    reader.expect("placement");
    result.placement.generation = reader.number<Generation>("topology");
    result.placement.workers = reader.number<Worker>("worker count");
    const auto partitions = reader.number<Partition>("partition count");
    const Shards shards(graph, partitions);
    reader.expect("owners");
    result.placement.owners.reserve(partitions);
    for (Partition part = 0; part < partitions; ++part) {
        result.placement.owners.push_back(reader.number<Worker>("partition owner"));
    }
    result.placement.validate(partitions);
    reader.expect("totals");
    result.totals = read_totals(reader);
    reader.expect("last");
    const auto last = reader.number<unsigned>("last step flag");
    if (last == 1) {
        reader.expect("step");
        result.last = read_summary(reader);
    } else if (last != 0) {
        throw std::invalid_argument("unknown last step flag");
    }

    reader.expect("mailbox");
    result.mailbox.epoch = reader.number<Epoch>("mailbox epoch");
    result.mailbox.raw_count = reader.number<std::uint64_t>("raw message count");
    reader.expect("generation");
    const auto generation = reader.number<Generation>("generation");
    reader.expect("previous");
    (void)reader.number<Generation>("previous generation");
    reader.expect("shards");
    const auto count = reader.number<Partition>("shard count");
    if (count != partitions) {
        throw std::invalid_argument("checkpoint manifest does not list every partition");
    }
    struct Descriptor {
        Partition partition;
        Epoch epoch;
        std::uint64_t bytes;
        Digest checksum;
    };
    std::vector<Descriptor> descriptors;
    descriptors.reserve(count);
    for (Partition part = 0; part < count; ++part) {
        reader.expect("shard");
        descriptors.push_back({reader.number<Partition>("shard partition"),
                               reader.number<Epoch>("shard epoch"),
                               reader.number<std::uint64_t>("shard bytes"),
                               reader.number<Digest>("shard checksum")});
    }
    reader.expect("end");
    reader.finish();

    const auto root = shard_root(checkpoint);
    result.states.resize(shards.count());
    result.mailbox.inboxes.resize(shards.count());
    for (Partition part = 0; part < count; ++part) {
        const auto& descriptor = descriptors[part];
        if (descriptor.partition != part || descriptor.epoch != result.next_epoch) {
            throw std::invalid_argument("checkpoint manifest shard order or epoch is invalid");
        }
        const auto path = shard_path(root, generation, part);
        std::ifstream input(path, std::ios::binary);
        if (!input) {
            throw std::runtime_error("checkpoint generation is incomplete: cannot open " + path.string());
        }
        const auto bytes = read_stream(input, "checkpoint shard", max_shard_bytes);
        if (bytes.size() != descriptor.bytes) {
            throw std::invalid_argument("checkpoint shard size disagrees with its manifest");
        }
        Hash hash;
        hash.bytes(strip_checksum(bytes, "checkpoint shard"));
        if (hash.value() != descriptor.checksum) {
            throw std::invalid_argument("checkpoint shard checksum disagrees with its manifest");
        }
        std::istringstream shard_stream(bytes);
        shard_stream.imbue(std::locale::classic());
        const auto image = read_shard_v2(shard_stream, graph);
        if (image.partition != part || image.epoch != result.next_epoch ||
            image.algorithm != result.program.algorithm) {
            throw std::invalid_argument("checkpoint shard belongs to another generation");
        }
        result.states[part] = image.states;
        result.mailbox.inboxes[part] = image.inbox;
    }
    result.validate(graph, shards);
    return result;
}

Checkpoint read_checkpoint(std::istream& input, const std::filesystem::path& checkpoint,
                           const Graph& graph) {
    auto bytes = read_stream(input, "checkpoint", max_snapshot_bytes);
    if (bytes.starts_with("FRONTIERLOOM_CHECKPOINT 2")) {
        std::istringstream manifest(std::move(bytes));
        return read_checkpoint_v2(manifest, checkpoint, graph);
    }
    std::istringstream legacy(std::move(bytes));
    return read_checkpoint_v1(legacy, graph);
}

Generation manifest_generation(std::istream& input) {
    const auto bytes = read_stream(input, "checkpoint manifest", max_snapshot_bytes);
    if (!bytes.starts_with("FRONTIERLOOM_CHECKPOINT 2")) {
        return 0;
    }
    const auto label = std::string_view("\ngeneration ");
    const auto position = bytes.find(label);
    if (position == std::string::npos) {
        return 0;
    }
    const auto start = position + label.size();
    const auto end = bytes.find_first_of(" \n", start);
    if (end == std::string::npos) {
        return 0;
    }
    try {
        return parse_number<Generation>(std::string_view(bytes).substr(start, end - start),
                                        "generation");
    } catch (const std::invalid_argument&) {
        return 0;
    }
}

std::uint64_t snapshot_bytes(const std::filesystem::path& checkpoint) {
    std::error_code error;
    const auto status = std::filesystem::symlink_status(checkpoint, error);
    if (error || status.type() == std::filesystem::file_type::not_found) {
        return 0;
    }
    std::uint64_t total = std::filesystem::file_size(checkpoint, error);
    if (error) {
        return 0;
    }
    std::ifstream input(checkpoint, std::ios::binary);
    if (!input) {
        return total;
    }
    auto generation = manifest_generation(input);
    if (generation == 0) {
        return total;
    }
    const auto directory = shard_root(checkpoint) / generation_directory(generation);
    std::filesystem::directory_iterator entries(directory, error);
    if (error) {
        return total;
    }
    for (const auto& entry : entries) {
        if (entry.is_regular_file(error)) {
            total += entry.file_size(error);
            if (error) {
                return total;
            }
        }
    }
    return total;
}

Digest work_digest(const StepInput& work) {
    Hash hash;
    hash.bytes("FrontierLoom handoff");
    hash.number(work.epoch);
    hash.number(work.partition);
    hash.number(std::bit_cast<std::uint64_t>(work.dangling));
    hash.number(work.states.size());
    for (const auto& state : work.states) {
        hash.number(state.distance);
        hash.number(std::bit_cast<std::uint64_t>(state.rank));
    }
    hash.number(work.inbox.size());
    for (const auto& entry : work.inbox) {
        hash.number(entry.target);
        hash.number(std::bit_cast<std::uint64_t>(entry.value));
    }
    return hash.value();
}

}  // namespace frontier
