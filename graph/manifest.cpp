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

Graph load_graph(const std::filesystem::path& file) {
    std::ifstream input(file);
    if (!input) {
        throw std::runtime_error("cannot open graph " + file.string());
    }
    input.imbue(std::locale::classic());
    return read_graph_v1(input);
}

void save_graph(const std::filesystem::path& file, const Graph& graph) {
    std::ostringstream output;
    output.imbue(std::locale::classic());
    write_graph_v1(output, graph);
    write_new_text_file(file, output.view());
}

}  // namespace frontier
