#include "workers/settings.hpp"
#include "graph/text.hpp"

#include <fstream>
#include <set>

namespace frontier {
namespace {

std::string_view trim(std::string_view text) {
    constexpr auto whitespace = " \t\r\n";
    const auto first = text.find_first_not_of(whitespace);
    if (first == std::string_view::npos) {
        return {};
    }
    const auto last = text.find_last_not_of(whitespace);
    return text.substr(first, last - first + 1);
}

void set_value(Settings& settings, std::string_view key, std::string_view value) {
    if (key == "partitions") {
        settings.partitions = parse_number<Partition>(value, key);
    } else if (key == "workers") {
        settings.workers = parse_number<Worker>(value, key);
    } else if (key == "timeout_ms") {
        settings.timeout = std::chrono::milliseconds(parse_number<std::uint32_t>(value, key));
    } else if (key == "retry_limit") {
        settings.retry_limit = parse_number<std::uint32_t>(value, key);
    } else if (key == "damping") {
        settings.damping = parse_number<double>(value, key);
    } else if (key == "tolerance") {
        settings.tolerance = parse_number<double>(value, key);
    } else if (key == "rank_limit") {
        settings.rank_limit = parse_number<std::uint32_t>(value, key);
    } else if (key == "bfs_limit") {
        settings.bfs_limit = parse_number<std::uint32_t>(value, key);
    } else {
        throw std::invalid_argument("unknown setting " + std::string(key));
    }
}

FaultPoint parse_point(std::string_view token) {
    for (const auto point : {FaultPoint::after_send, FaultPoint::before_commit,
                             FaultPoint::before_replace, FaultPoint::after_replace}) {
        if (token == name(point)) {
            return point;
        }
    }
    throw std::invalid_argument("unknown fault point " + std::string(token));
}

}  // namespace

Settings read_settings(const std::filesystem::path& file) {
    std::ifstream input(file);
    if (!input) {
        throw std::runtime_error("cannot open config " + file.string());
    }
    Settings settings;
    std::set<std::string> keys;
    std::string line;
    std::size_t number{};
    while (std::getline(input, line)) {
        ++number;
        const auto text = trim(std::string_view(line).substr(0, line.find('#')));
        if (text.empty()) {
            continue;
        }
        try {
            const auto separator = text.find('=');
            if (separator == std::string_view::npos) {
                throw std::invalid_argument("expected key=value");
            }
            const auto key = trim(text.substr(0, separator));
            if (!keys.emplace(key).second) {
                throw std::invalid_argument("duplicate setting " + std::string(key));
            }
            set_value(settings, key, trim(text.substr(separator + 1)));
        } catch (const std::invalid_argument& error) {
            throw std::invalid_argument(file.string() + ":" + std::to_string(number) + ": " + error.what());
        }
    }
    if (input.bad()) {
        throw std::runtime_error("reading config " + file.string() + " failed");
    }
    return settings;
}

Fault parse_fault(std::string_view value) {
    const auto point_at = value.find(':');
    if (point_at == std::string_view::npos) {
        throw std::invalid_argument("fault must be POINT:EPOCH[:WORKER]");
    }
    const auto point = parse_point(value.substr(0, point_at));
    const auto rest = value.substr(point_at + 1);
    const auto worker_at = rest.find(':');
    Fault fault{point, parse_number<Epoch>(rest.substr(0, worker_at), "fault epoch")};
    if (worker_at != std::string_view::npos) {
        if (point == FaultPoint::before_commit) {
            throw std::invalid_argument("before-commit fault has no worker argument");
        }
        fault.worker = parse_number<Worker>(rest.substr(worker_at + 1), "fault worker");
    }
    return fault;
}

ScaleAt parse_scale(std::string_view value) {
    const auto separator = value.find(':');
    if (separator == std::string_view::npos) {
        throw std::invalid_argument("scale schedule must be EPOCH:WORKERS");
    }
    return {parse_number<Epoch>(value.substr(0, separator), "scale epoch"),
            parse_number<Worker>(value.substr(separator + 1), "scale workers")};
}

}  // namespace frontier
