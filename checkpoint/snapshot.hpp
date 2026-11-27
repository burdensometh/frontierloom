#pragma once

#include "barrier/barrier.hpp"

#include <filesystem>
#include <iosfwd>
#include <span>
#include <string>
#include <string_view>

namespace frontier {

// A snapshot is one of two on-disk encodings. V1 is the legacy single file.
// V2 keeps a small manifest at the checkpoint path and writes one file per
// partition into a generation directory beside it. The manifest is the atomic
// commit point: until it is renamed into place every shard of the candidate
// generation is unreachable, so a partial shard save still exposes the last
// complete generation.
inline constexpr std::uint32_t checkpoint_format_v1 = 1;
inline constexpr std::uint32_t checkpoint_format_v2 = 2;

// Suffix appended to the checkpoint path to form the shard root directory.
inline constexpr std::string_view shard_root_suffix = ".shards";

struct Totals {
    std::uint64_t active{};
    std::uint64_t consumed{};
    std::uint64_t emitted{};
    std::uint64_t combined{};
    std::uint64_t recovered_failures{};
    std::uint64_t resumes{};
    std::uint64_t scales{};
    std::uint64_t moved_partitions{};
    bool operator==(const Totals&) const = default;
};

struct Checkpoint {
    std::uint64_t graph_revision{};
    Digest graph_identity{};
    Program program;
    PartitionMap placement;
    Generation incarnation{};
    Epoch next_epoch{};
    Totals totals;
    std::optional<StepSummary> last;
    std::vector<std::vector<VertexState>> states;
    Mailbox mailbox;

    [[nodiscard]] Stop stop() const { return last ? last->stop : Stop::running; }
    [[nodiscard]] std::uint32_t updates() const;
    [[nodiscard]] StepInput work(Partition partition, double original_dangling) const;
    void validate(const Graph& graph, const Shards& shards) const;
    bool operator==(const Checkpoint&) const = default;
};

// Resident data of a single partition as stored in a V2 shard file.
struct ShardImage {
    Epoch epoch{};
    Partition partition{};
    Algorithm algorithm{Algorithm::bfs};
    std::vector<VertexState> states;
    std::vector<InboxEntry> inbox;
    bool operator==(const ShardImage&) const = default;
};

// A serialized V2 shard file. `payload` is the complete file including its
// checksum trailer, `bytes` is its size, and `checksum` covers the bytes before
// the trailer.
struct ShardFile {
    Partition partition{};
    std::string payload;
    std::uint64_t bytes{};
    Digest checksum{};
    bool operator==(const ShardFile&) const = default;
};

Checkpoint bootstrap(const Graph& graph, const Shards& shards, Program program, Worker workers);
void advance(Checkpoint& checkpoint, const StepSummary& step,
             std::vector<std::vector<VertexState>> states, Mailbox mailbox);

// V1: one newline-delimited file carrying the manifest and every partition.
Checkpoint read_checkpoint_v1(std::istream& input, const Graph& graph);
void write_checkpoint_v1(std::ostream& output, const Checkpoint& checkpoint);

// Auto-detecting reader. V2 shard files resolve relative to `file`.
Checkpoint read_checkpoint(std::istream& input, const std::filesystem::path& file, const Graph& graph);

// V2: manifest plus per-partition shard files. The reader resolves shard files
// relative to the checkpoint path and rejects any missing or mismatched shard,
// so control state and resident data always come from one generation.
std::filesystem::path shard_root(const std::filesystem::path& checkpoint);
std::string generation_directory(Generation generation);
std::filesystem::path shard_path(const std::filesystem::path& root, Generation generation,
                                 Partition partition);
std::vector<ShardFile> serialize_shards(const Shards& shards, const Checkpoint& checkpoint);
// Digest binding every control field, the mailbox summary and the byte size and
// checksum of each shard file. A candidate directory is named after this value.
Digest checkpoint_generation(const Checkpoint& checkpoint, std::span<const ShardFile> shards);
void write_manifest_v2(std::ostream& output, const Checkpoint& checkpoint, Generation generation,
                       Generation previous, std::span<const ShardFile> shards);
// Reports the generation recorded in a manifest. Returns zero for a V1 image.
Generation manifest_generation(std::istream& input);
ShardImage read_shard_v2(std::istream& input, const Graph& graph);
Checkpoint read_checkpoint_v2(std::istream& manifest, const std::filesystem::path& checkpoint,
                              const Graph& graph);
// Total on-disk size of a checkpoint, summing the manifest and every shard file
// of its current generation. Returns zero if the path does not exist.
std::uint64_t snapshot_bytes(const std::filesystem::path& checkpoint);

Digest work_digest(const StepInput& work);

}  // namespace frontier
