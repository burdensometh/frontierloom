#pragma once

#include "checkpoint/snapshot.hpp"

#include <filesystem>

namespace frontier {

Checkpoint load_checkpoint(const std::filesystem::path& file, const Graph& graph);

class Store {
public:
    explicit Store(std::filesystem::path file);
    ~Store();

    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    [[nodiscard]] bool exists() const;
    [[nodiscard]] Checkpoint load(const Graph& graph) const;
    void publish(const Graph& graph, const Shards& shards, const Checkpoint& checkpoint) const;

    // Re-encodes whatever is at the path as the current sharded format. A V1
    // image is read and republished; a V2 image is rewritten under the same
    // content-addressed generation so a second conversion has one target.
    [[nodiscard]] std::uint32_t format() const;
    void convert(const Graph& graph) const;

private:
    std::filesystem::path file_;
    int lock_ = -1;
};

}  // namespace frontier
