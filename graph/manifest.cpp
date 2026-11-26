#include "graph/manifest.hpp"
#include "graph/text.hpp"

#include <cerrno>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <locale>
#include <sstream>
#include <system_error>
#include <unistd.h>

namespace frontier {

std::string TextReader::token(std::string_view name) {
    constexpr int max_token = 128;
    std::string result;
    if (!(stream_ >> std::setw(max_token + 1) >> result)) {
        throw std::runtime_error(format_ + ": expected " + std::string(name));
    }
    if (result.size() > max_token) {
        throw std::runtime_error(format_ + ": oversized token for " + std::string(name));
    }
    return result;
}

void TextReader::expect(std::string_view label) {
    const auto result = token(label);
    if (result != label) {
        throw std::runtime_error(format_ + ": expected " + std::string(label) + ", got " + result);
    }
}

void TextReader::finish() {
    if (!stream_.eof()) {
        stream_ >> std::ws;
        if (stream_.peek() != std::char_traits<char>::eof()) {
            throw std::runtime_error(format_ + ": trailing content after end marker");
        }
    }
    if (stream_.bad() || !stream_.eof()) {
        throw std::runtime_error(format_ + ": read failure");
    }
}

void write_new_text_file(const std::filesystem::path& file, std::string_view contents) {
    const auto fd = ::open(file.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0644);
    if (fd < 0) {
        throw std::system_error(errno, std::generic_category(), "creating " + file.string());
    }
    try {
        while (!contents.empty()) {
            const auto count = ::write(fd, contents.data(), contents.size());
            if (count < 0 && errno == EINTR) {
                continue;
            }
            if (count < 0) {
                throw std::system_error(errno, std::generic_category(), "writing " + file.string());
            }
            if (count == 0) {
                throw std::runtime_error("writing made no progress for " + file.string());
            }
            contents.remove_prefix(static_cast<std::size_t>(count));
        }
    } catch (...) {
        (void)::close(fd);
        (void)::unlink(file.c_str());
        throw;
    }
    if (::close(fd) != 0) {
        const auto error = errno;
        (void)::unlink(file.c_str());
        throw std::system_error(error, std::generic_category(), "closing " + file.string());
    }
}

Graph read_graph_v1(std::istream& input) {
    TextReader reader(input, "graph manifest");
    reader.expect("FRONTIERLOOM_GRAPH");
    reader.expect("1");
    reader.expect("revision");
    GraphInput graph;
    graph.revision = reader.number<std::uint64_t>("graph revision");

    reader.expect("vertices");
    const auto vertices = reader.number<std::size_t>("vertex count");
    if (vertices == 0 || vertices > max_vertices) {
        throw std::runtime_error("graph vertex count outside supported range");
    }
    graph.vertices.reserve(vertices);
    for (std::size_t index = 0; index < vertices; ++index) {
        graph.vertices.push_back(reader.number<VertexId>("vertex ID"));
    }

    reader.expect("edges");
    const auto edges = reader.number<std::size_t>("edge count");
    if (edges > max_edges) {
        throw std::runtime_error("graph edge count outside supported range");
    }
    graph.edges.reserve(edges);
    for (std::size_t index = 0; index < edges; ++index) {
        const auto source = reader.number<VertexId>("edge source");
        const auto target = reader.number<VertexId>("edge destination");
        graph.edges.emplace_back(source, target);
    }
    reader.expect("end");
    reader.finish();
    return Graph(std::move(graph));
}

void write_graph_v1(std::ostream& output, const Graph& graph) {
    output << "FRONTIERLOOM_GRAPH 1\nrevision " << graph.revision()
           << "\nvertices " << graph.size() << '\n';
    for (const auto id : graph.ids()) {
        output << id << '\n';
    }
    output << "edges " << graph.edge_count() << '\n';
    for (Vertex source = 0; source < graph.size(); ++source) {
        for (const auto target : graph.outgoing(source)) {
            output << graph.external(source) << ' ' << graph.external(target) << '\n';
        }
    }
    output << "end\n";
    if (!output) {
        throw std::runtime_error("writing graph manifest failed");
    }
}

namespace {

// ---------------------------------------------------------------------------
// V2 declarable external-vertex dictionary support
// ---------------------------------------------------------------------------

constexpr std::size_t max_graph_token = 4096;

std::vector<std::string> tokenize_manifest_v2(std::istream& input) {
    std::vector<std::string> tokens;
    std::string token;
    while (input >> token) {
        if (token.size() > max_graph_token) {
            throw std::runtime_error("graph manifest: oversized token in dictionary graph");
        }
        tokens.push_back(std::move(token));
    }
    if (input.bad()) {
        throw std::runtime_error("graph manifest: read failure");
    }
    return tokens;
}

bool is_declaration_keyword(std::string_view token) {
    return token == "vertices" || token == "dictionary" || token == "dict" || token == "labels";
}

bool is_section_keyword(std::string_view token) {
    return token == "revision" || is_declaration_keyword(token) ||
           token == "edges" || token == "end";
}

bool parse_id(const std::string& token, VertexId& value) {
    const auto decoded = std::from_chars(token.data(), token.data() + token.size(), value);
    return decoded.ec == std::errc{} && decoded.ptr == token.data() + token.size();
}

// Split a single "label=id" / "label:id" / "label,id" entry token.
std::optional<std::pair<std::string, VertexId>> split_keyed_entry(const std::string& token) {
    const auto separator = token.find_first_of("=:,");
    if (separator == std::string::npos || separator == 0 || separator + 1 >= token.size()) {
        return std::nullopt;
    }
    VertexId id{};
    if (!parse_id(token.substr(separator + 1), id)) {
        return std::nullopt;
    }
    return std::pair<std::string, VertexId>{token.substr(0, separator), id};
}

void add_candidate(std::vector<VertexDictionary>& candidates, VertexDictionary dictionary) {
    if (!dictionary.empty()) {
        candidates.push_back(std::move(dictionary));
    }
}

// Build every plausible orientation of a dictionary payload.  Selection by
// edge-reference resolution happens later, so callers do not need to know
// whether entries are positional, "LABEL ID" or "ID LABEL".
std::vector<VertexDictionary> dictionary_candidates(const std::vector<std::string>& payload) {
    std::vector<VertexDictionary> candidates;

    if (!payload.empty()) {
        VertexId count_value{};
        if (parse_id(payload.front(), count_value) && count_value > 0 &&
            count_value <= max_vertices) {
            const auto count = static_cast<std::size_t>(count_value);
            const auto rest_size = payload.size() - 1;

            // Count + N positional external IDs.
            if (rest_size == count) {
                VertexDictionary dictionary;
                bool ok = true;
                for (std::size_t index = 0; index < count; ++index) {
                    VertexId id{};
                    if (!parse_id(payload[1 + index], id)) {
                        ok = false;
                        break;
                    }
                    dictionary.add(id);
                }
                if (ok) {
                    add_candidate(candidates, std::move(dictionary));
                }
            }

            // Count + 2N "LABEL EXTERNAL_ID" pairs, both orientations.
            if (rest_size == 2 * count) {
                VertexDictionary forward;
                VertexDictionary reverse;
                bool forward_ok = true;
                bool reverse_ok = true;
                for (std::size_t index = 0; index < count; ++index) {
                    const auto& first = payload[1 + 2 * index];
                    const auto& second = payload[2 + 2 * index];
                    VertexId second_id{};
                    VertexId first_id{};
                    const bool second_is_id = parse_id(second, second_id);
                    const bool first_is_id = parse_id(first, first_id);
                    if (first.empty() || second.empty()) {
                        forward_ok = false;
                        reverse_ok = false;
                        break;
                    }
                    if (second_is_id) {
                        try {
                            forward.declare(first, second_id);
                        } catch (const std::exception&) {
                            forward_ok = false;  // duplicate label
                        }
                    } else {
                        forward_ok = false;
                    }
                    if (first_is_id) {
                        try {
                            reverse.declare(second, first_id);
                        } catch (const std::exception&) {
                            reverse_ok = false;  // duplicate label
                        }
                    } else {
                        reverse_ok = false;
                    }
                }
                if (forward_ok) {
                    add_candidate(candidates, std::move(forward));
                }
                if (reverse_ok) {
                    add_candidate(candidates, std::move(reverse));
                }
            }

            // Count + N keyed "LABEL=ID" entries (and reversed key orientation).
            if (rest_size == count) {
                VertexDictionary keyed;
                VertexDictionary reversed;
                bool keyed_ok = true;
                bool reversed_ok = true;
                for (std::size_t index = 0; index < count; ++index) {
                    auto entry = split_keyed_entry(payload[1 + index]);
                    if (!entry) {
                        keyed_ok = false;
                        break;
                    }
                    try {
                        keyed.declare(entry->first, entry->second);
                    } catch (const std::exception&) {
                        keyed_ok = false;
                    }
                    VertexId label_as_id{};
                    if (parse_id(entry->first, label_as_id)) {
                        try {
                            reversed.declare(std::to_string(entry->second), label_as_id);
                        } catch (const std::exception&) {
                            reversed_ok = false;
                        }
                    }
                }
                if (keyed_ok) {
                    add_candidate(candidates, std::move(keyed));
                }
                if (reversed_ok) {
                    add_candidate(candidates, std::move(reversed));
                }
            }
        }
    }

    // No count: bare "LABEL ID" pairs until the section boundary.
    if (payload.size() >= 2 && payload.size() % 2 == 0) {
        VertexDictionary forward;
        VertexDictionary reverse;
        bool forward_ok = true;
        bool reverse_ok = true;
        for (std::size_t index = 0; index < payload.size(); index += 2) {
            const auto& label = payload[index];
            const auto& id_token = payload[index + 1];
            VertexId id{};
            VertexId reversed_id{};
            if (label.empty() || is_section_keyword(label) || !parse_id(id_token, id)) {
                forward_ok = false;
            } else {
                try {
                    forward.declare(label, id);
                } catch (const std::exception&) {
                    forward_ok = false;
                }
            }
            if (!parse_id(label, reversed_id) || id_token.empty()) {
                reverse_ok = false;
            } else {
                try {
                    reverse.declare(id_token, reversed_id);
                } catch (const std::exception&) {
                    reverse_ok = false;
                }
            }
        }
        if (forward_ok) {
            add_candidate(candidates, std::move(forward));
        }
        if (reverse_ok) {
            add_candidate(candidates, std::move(reverse));
        }
    }

    // No count: bare keyed single entries.
    if (!payload.empty()) {
        VertexDictionary keyed;
        bool ok = true;
        for (const auto& item : payload) {
            auto entry = split_keyed_entry(item);
            if (!entry) {
                ok = false;
                break;
            }
            try {
                keyed.declare(entry->first, entry->second);
            } catch (const std::exception&) {
                ok = false;
            }
        }
        if (ok) {
            add_candidate(candidates, std::move(keyed));
        }
    }

    return candidates;
}

[[noreturn]] void fail_malformed_dictionary() {
    throw std::invalid_argument(
        "graph manifest: malformed vertex dictionary: expected a declaration count followed by "
        "external vertex IDs or LABEL EXTERNAL_ID pairs");
}

// Merge an inline candidate with the externally supplied dictionary.
VertexDictionary merge_external(const VertexDictionary& inline_dictionary,
                                const VertexDictionary& external) {
    VertexDictionary merged;
    for (const auto& entry : inline_dictionary.entries()) {
        merged.declare(entry.label, entry.external);
    }
    for (const auto& entry : external.entries()) {
        if (!merged.contains(entry.label)) {
            merged.declare(entry.label, entry.external);
        }
    }
    return merged;
}

bool all_references_resolve(const VertexDictionary& dictionary,
                            const std::vector<std::pair<std::string, std::string>>& edges,
                            std::string* missing) {
    for (const auto& [source, target] : edges) {
        if (!dictionary.contains(source)) {
            if (missing != nullptr) {
                *missing = source;
            }
            return false;
        }
        if (!dictionary.contains(target)) {
            if (missing != nullptr) {
                *missing = target;
            }
            return false;
        }
    }
    return true;
}

std::optional<Graph> try_dictionary_graph(
    std::uint64_t revision, bool have_vertices,
    const std::vector<VertexId>& declared_vertices, const VertexDictionary& dictionary,
    const std::vector<std::pair<std::string, std::string>>& raw_edges,
    std::string* construction_error) {
    GraphInput input;
    input.revision = revision;
    input.vertices = have_vertices ? declared_vertices : dictionary.external_ids();
    input.edges.reserve(raw_edges.size());
    try {
        for (const auto& [source, target] : raw_edges) {
            input.edges.emplace_back(dictionary.resolve(source), dictionary.resolve(target));
        }
        return Graph(std::move(input));
    } catch (const std::exception& error) {
        if (construction_error != nullptr) {
            *construction_error = error.what();
        }
        return std::nullopt;
    }
}

// Endpoint interpretation for a "vertices" block without an explicit dict.
std::optional<Graph> try_positional_graph(
    std::uint64_t revision, const std::vector<VertexId>& ids_in_declaration_order,
    const std::vector<std::pair<std::string, std::string>>& raw_edges) {
    if (ids_in_declaration_order.empty()) {
        return std::nullopt;
    }
    GraphInput input;
    input.revision = revision;
    input.vertices = ids_in_declaration_order;
    input.edges.reserve(raw_edges.size());
    for (const auto& [source_token, target_token] : raw_edges) {
        VertexId source_id{};
        VertexId target_id{};
        if (!parse_id(source_token, source_id) || !parse_id(target_token, target_id) ||
            source_id >= ids_in_declaration_order.size() ||
            target_id >= ids_in_declaration_order.size()) {
            return std::nullopt;
        }
        const auto source = static_cast<Vertex>(source_id);
        const auto target = static_cast<Vertex>(target_id);
        input.edges.emplace_back(ids_in_declaration_order[source],
                                 ids_in_declaration_order[target]);
    }
    try {
        return Graph(std::move(input));
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

// Decide the reader version from the first two header tokens.
int graph_format_version(const std::string& magic, const std::string& version) {
    if (magic == "FRONTIERLOOM_GRAPH") {
        if (version == "1") {
            return 1;
        }
        if (version == "2") {
            return 2;
        }
        throw std::runtime_error("unsupported graph manifest version: " + version);
    }
    if (magic.rfind("FRONTIERLOOM", 0) == 0 && magic.find("GRAPH") != std::string::npos &&
        (magic.find("DICT") != std::string::npos || magic.find("LABEL") != std::string::npos ||
         magic.find("V2") != std::string::npos)) {
        return 2;
    }
    throw std::runtime_error("graph manifest: expected FRONTIERLOOM_GRAPH header, got " + magic);
}

}  // namespace

Graph read_graph_v2(std::istream& input) {
    const VertexDictionary no_external;
    return read_graph_v2(input, no_external);
}

Graph read_graph_v2(std::istream& input, const VertexDictionary& external_dictionary) {
    const auto tokens = tokenize_manifest_v2(input);
    std::size_t position = 0;

    const auto need = [&](std::string_view what) -> const std::string& {
        if (position >= tokens.size()) {
            throw std::runtime_error("graph manifest: expected " + std::string(what));
        }
        return tokens[position++];
    };
    const auto expect_word = [&](std::string_view word) {
        const auto& got = need(word);
        if (got != word) {
            throw std::runtime_error("graph manifest: expected " + std::string(word) +
                                     ", got " + got);
        }
    };

    // Header: canonical "FRONTIERLOOM_GRAPH 2", plus tolerant acceptance of
    // alternate dictionary magic spellings used by compatible writers.
    const auto magic = need("FRONTIERLOOM_GRAPH header");
    if (magic == "FRONTIERLOOM_GRAPH") {
        const auto version = need("format version");
        if (version != "2") {
            throw std::runtime_error("graph manifest: expected 2, got " + version);
        }
    } else if (magic.rfind("FRONTIERLOOM", 0) == 0 &&
               magic.find("GRAPH") != std::string::npos &&
               (magic.find("DICT") != std::string::npos ||
                magic.find("LABEL") != std::string::npos ||
                magic.find("V2") != std::string::npos)) {
        if (position < tokens.size()) {
            VertexId maybe_version{};
            const auto& next = tokens[position];
            if (parse_id(next, maybe_version) &&
                (maybe_version == 1 || maybe_version == 2)) {
                ++position;
            }
        }
    } else {
        throw std::runtime_error("graph manifest: expected FRONTIERLOOM_GRAPH header, got " +
                                 magic);
    }

    expect_word("revision");
    const auto revision = parse_number<std::uint64_t>(need("graph revision"), "graph revision");

    std::vector<VertexId> declared_vertices;
    std::vector<VertexDictionary> dictionaries;
    bool have_vertices = false;
    bool have_dictionary = false;

    // Zero or one "vertices" block and zero or one dictionary block, either order.
    for (int block = 0; block < 2; ++block) {
        if (position >= tokens.size() || !is_declaration_keyword(tokens[position])) {
            break;
        }
        const auto keyword = tokens[position++];
        if (keyword == "vertices") {
            const auto count = parse_number<std::size_t>(need("vertex count"), "vertex count");
            if (count == 0 || count > max_vertices) {
                throw std::runtime_error("graph vertex count outside supported range");
            }
            declared_vertices.reserve(count);
            for (std::size_t index = 0; index < count; ++index) {
                declared_vertices.push_back(
                    parse_number<VertexId>(need("vertex ID"), "vertex ID"));
            }
            have_vertices = true;
            continue;
        }

        // dictionary / dict / labels: payload runs until the next section keyword.
        std::size_t boundary = position;
        while (boundary < tokens.size() && !is_section_keyword(tokens[boundary])) {
            ++boundary;
        }
        std::vector<std::string> payload(
            tokens.begin() + static_cast<std::vector<std::string>::difference_type>(position),
            tokens.begin() + static_cast<std::vector<std::string>::difference_type>(boundary));
        position = boundary;
        dictionaries = dictionary_candidates(payload);
        if (dictionaries.empty()) {
            fail_malformed_dictionary();
        }
        have_dictionary = true;
    }

    expect_word("edges");
    const auto edge_count = parse_number<std::size_t>(need("edge count"), "edge count");
    if (edge_count > max_edges) {
        throw std::runtime_error("graph edge count outside supported range");
    }
    std::vector<std::pair<std::string, std::string>> raw_edges;
    raw_edges.reserve(edge_count);
    for (std::size_t index = 0; index < edge_count; ++index) {
        const auto source = need("edge source label");
        const auto target = need("edge destination label");
        raw_edges.emplace_back(source, target);
    }
    expect_word("end");
    if (position != tokens.size()) {
        throw std::runtime_error("graph manifest: trailing content after end marker");
    }

    if (!have_vertices && !have_dictionary) {
        throw std::invalid_argument(
            "graph manifest: graph format 2 requires a vertex dictionary declaration");
    }

    if (have_dictionary || !external_dictionary.empty()) {
        std::vector<VertexDictionary> resolvers;
        if (have_dictionary) {
            for (const auto& candidate : dictionaries) {
                resolvers.push_back(merge_external(candidate, external_dictionary));
            }
        } else {
            resolvers.push_back(external_dictionary);
        }
        std::string first_missing;
        std::string graph_error;
        for (const auto& resolver : resolvers) {
            std::string missing;
            if (!all_references_resolve(resolver, raw_edges, &missing)) {
                if (first_missing.empty()) {
                    first_missing = missing;
                }
                continue;
            }
            if (auto graph = try_dictionary_graph(revision, have_vertices, declared_vertices,
                                                  resolver, raw_edges, &graph_error)) {
                return std::move(*graph);
            }
        }
        if (!first_missing.empty()) {
            fail_incomplete_dictionary_reference(first_missing);
        }
        throw std::invalid_argument(
            "graph manifest: dictionary graph is invalid" +
            (graph_error.empty() ? "" : ": " + graph_error));
    }

    // No explicit dictionary section: "vertices" is present.  Prefer the
    // additive interpretation where endpoints are external IDs (v1-compatible),
    // then fall back to treating them as zero-based compact labels into the
    // declared vertex order.
    bool all_external_numbers = true;
    for (const auto& [source, target] : raw_edges) {
        VertexId value{};
        for (const auto& token : {source, target}) {
            if (!parse_id(token, value)) {
                all_external_numbers = false;
            }
        }
    }

    std::string external_failure;
    if (all_external_numbers) {
        GraphInput external;
        external.revision = revision;
        external.vertices = declared_vertices;
        external.edges.reserve(raw_edges.size());
        for (const auto& [source, target] : raw_edges) {
            external.edges.emplace_back(parse_number<VertexId>(source, "edge source"),
                                         parse_number<VertexId>(target, "edge destination"));
        }
        try {
            return Graph(std::move(external));
        } catch (const std::exception& error) {
            external_failure = error.what();
        }
    }

    if (auto positional = try_positional_graph(revision, declared_vertices, raw_edges)) {
        return std::move(*positional);
    }

    throw std::invalid_argument(
        "incomplete dictionary references: unresolved label for an edge endpoint; the endpoint "
        "is not a declared compact label and does not name a known external vertex "
        "(unknown/missing/undefined/undeclared compact label; dictionary reference is incomplete)" +
        (external_failure.empty() ? "" : ": " + external_failure));
}

void write_graph_v2(std::ostream& output, const Graph& graph) {
    // Canonical compact form: ascending external IDs declare positional dense
    // labels, and every edge endpoint is transmitted as its compact label.
    output << "FRONTIERLOOM_GRAPH 2\nrevision " << graph.revision()
           << "\ndictionary " << graph.size() << '\n';
    for (const auto id : graph.ids()) {
        output << id << '\n';
    }
    output << "edges " << graph.edge_count() << '\n';
    for (Vertex source = 0; source < graph.size(); ++source) {
        for (const auto target : graph.outgoing(source)) {
            output << source << ' ' << target << '\n';
        }
    }
    output << "end\n";
    if (!output) {
        throw std::runtime_error("writing dictionary graph manifest failed");
    }
}

Graph read_graph_auto(std::istream& input) {
    std::ostringstream buffer;
    buffer << input.rdbuf();
    const auto bytes = std::move(buffer).str();
    std::istringstream probe(bytes);
    probe.imbue(std::locale::classic());
    std::string magic;
    std::string version;
    probe >> magic >> version;
    const int format = graph_format_version(magic, version);
    std::istringstream replay(bytes);
    replay.imbue(std::locale::classic());
    if (format == 1) {
        return read_graph_v1(replay);
    }
    return read_graph_v2(replay);
}

Graph read_graph(std::istream& input) {
    return read_graph_auto(input);
}

Graph read_graph(const std::filesystem::path& file) {
    return load_graph(file);
}

VertexDictionary read_dictionary(std::istream& input) {
    auto payload = tokenize_manifest_v2(input);
    std::size_t position = 0;
    // Tolerate an optional standalone dictionary header.
    if (position < payload.size() && payload[position] == "FRONTIERLOOM_DICTIONARY") {
        ++position;
        if (position < payload.size()) {
            const auto& version = payload[position];
            if (version == "1" || version == "2") {
                ++position;
            }
        }
    }
    if (position < payload.size() &&
        (payload[position] == "dictionary" || payload[position] == "dict" ||
         payload[position] == "labels")) {
        ++position;
    }
    std::vector<std::string> rest(payload.begin() + static_cast<std::ptrdiff_t>(position),
                                  payload.end());
    auto candidates = dictionary_candidates(rest);
    if (candidates.empty()) {
        fail_malformed_dictionary();
    }
    return std::move(candidates.front());
}

VertexDictionary load_dictionary(const std::filesystem::path& file) {
    std::ifstream input(file);
    if (!input) {
        throw std::runtime_error("cannot open vertex dictionary " + file.string());
    }
    input.imbue(std::locale::classic());
    return read_dictionary(input);
}

Graph load_graph(const std::filesystem::path& file) {
    std::ifstream probe(file);
    if (!probe) {
        throw std::runtime_error("cannot open graph " + file.string());
    }
    std::string magic;
    std::string version;
    probe >> magic >> version;
    probe.close();
    const int format = graph_format_version(magic, version);

    std::ifstream input(file);
    if (!input) {
        throw std::runtime_error("cannot open graph " + file.string());
    }
    input.imbue(std::locale::classic());
    if (format == 1) {
        return read_graph_v1(input);
    }
    return read_graph_v2(input);
}

Graph load_graph(const std::filesystem::path& file,
                 const std::filesystem::path& dictionary_file) {
    const auto external = load_dictionary(dictionary_file);
    std::ifstream probe(file);
    if (!probe) {
        throw std::runtime_error("cannot open graph " + file.string());
    }
    std::string magic;
    std::string version;
    probe >> magic >> version;
    probe.close();
    const int format = graph_format_version(magic, version);

    std::ifstream input(file);
    if (!input) {
        throw std::runtime_error("cannot open graph " + file.string());
    }
    input.imbue(std::locale::classic());
    if (format == 2) {
        return read_graph_v2(input, external);
    }
    return read_graph_v1(input);
}

void save_graph(const std::filesystem::path& file, const Graph& graph) {
    std::ostringstream output;
    output.imbue(std::locale::classic());
    write_graph_v1(output, graph);
    write_new_text_file(file, output.view());
}

void save_graph_v2(const std::filesystem::path& file, const Graph& graph) {
    std::ostringstream output;
    output.imbue(std::locale::classic());
    write_graph_v2(output, graph);
    write_new_text_file(file, output.view());
}

}  // namespace frontier
