#include "checkpoint/store.hpp"

#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <fstream>
#include <locale>
#include <set>
#include <sstream>
#include <sys/file.h>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace frontier {
namespace {

[[noreturn]] void fail_os(std::string message) {
    throw std::system_error(errno, std::generic_category(), std::move(message));
}

class Descriptor {
public:
    explicit Descriptor(int fd) : fd_(fd) {}
    ~Descriptor() {
        if (fd_ >= 0) {
            (void)::close(fd_);
        }
    }
    Descriptor(const Descriptor&) = delete;
    Descriptor& operator=(const Descriptor&) = delete;

    [[nodiscard]] int get() const { return fd_; }

    int release() {
        return std::exchange(fd_, -1);
    }

private:
    int fd_;
};

void sync(int fd, std::string_view subject) {
    while (::fsync(fd) != 0) {
        if (errno != EINTR) {
            fail_os(std::string(subject));
        }
    }
}

void persist(int fd, std::string_view bytes) {
    while (!bytes.empty()) {
        const auto count = ::write(fd, bytes.data(), bytes.size());
        if (count < 0 && errno == EINTR) {
            continue;
        }
        if (count < 0) {
            fail_os("writing checkpoint file");
        }
        if (count == 0) {
            throw std::runtime_error("checkpoint write made no progress");
        }
        bytes.remove_prefix(static_cast<std::size_t>(count));
    }
    sync(fd, "syncing checkpoint file");
}

// Writes `bytes` to a uniquely named temporary file in `directory` and syncs it.
std::filesystem::path stage_file(const std::filesystem::path& directory, std::string_view bytes) {
    auto path = (directory / "stage.XXXXXX").string();
    Descriptor fd(::mkostemp(path.data(), O_CLOEXEC));
    if (fd.get() < 0) {
        fail_os("creating checkpoint temporary file");
    }
    persist(fd.get(), bytes);
    if (::close(fd.release()) < 0) {
        fail_os("closing checkpoint temporary file");
    }
    return path;
}

void sync_directory(const std::filesystem::path& directory) {
    Descriptor fd(::open(directory.c_str(), O_RDONLY | O_CLOEXEC | O_DIRECTORY));
    if (fd.get() < 0) {
        fail_os("opening a checkpoint directory");
    }
    sync(fd.get(), "syncing a checkpoint directory");
}

// Publishes `bytes` as the manifest at `target`: stage, sync, atomically rename
// over the destination, then sync the destination directory.
void publish_file(const std::filesystem::path& target, std::string_view bytes) {
    const auto temporary = stage_file(target.parent_path(), bytes);
    if (::rename(temporary.c_str(), target.c_str()) < 0) {
        const auto error = errno;
        (void)::unlink(temporary.c_str());
        errno = error;
        fail_os("publishing checkpoint");
    }
    sync_directory(target.parent_path());
}

std::string render_manifest(const Checkpoint& checkpoint, Generation generation,
                            Generation previous, std::span<const ShardFile> shards) {
    std::ostringstream stream;
    stream.imbue(std::locale::classic());
    write_manifest_v2(stream, checkpoint, generation, previous, shards);
    return std::move(stream).str();
}

}  // namespace

Checkpoint load_checkpoint(const std::filesystem::path& file, const Graph& graph) {
    std::ifstream input(file, std::ios::binary);
    if (!input) {
        throw std::runtime_error("cannot open checkpoint " + file.string());
    }
    return read_checkpoint(input, file, graph);
}

Store::Store(std::filesystem::path file) {
    if (file.filename().empty() || file.filename() == "." || file.filename() == "..") {
        throw std::invalid_argument("checkpoint path must name a file");
    }
    const auto directory = std::filesystem::absolute(file).parent_path();
    std::filesystem::create_directories(directory);
    file_ = std::filesystem::canonical(directory) / file.filename();

    const auto lock_path = file_.string() + ".lock";
    Descriptor fd(::open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600));
    if (fd.get() < 0) {
        fail_os("opening checkpoint lock");
    }
    for (;;) {
        if (::flock(fd.get(), LOCK_EX | LOCK_NB) == 0) {
            break;
        }
        if (errno != EINTR) {
            fail_os("acquiring exclusive checkpoint lock");
        }
    }
    (void)exists();
    lock_ = fd.release();
}

Store::~Store() {
    if (lock_ >= 0) {
        (void)::close(lock_);
    }
}

bool Store::exists() const {
    const auto status = std::filesystem::symlink_status(file_);
    if (status.type() == std::filesystem::file_type::not_found) {
        return false;
    }
    if (!std::filesystem::is_regular_file(status)) {
        throw std::invalid_argument("checkpoint path is reserved for a regular file");
    }
    return true;
}

Checkpoint Store::load(const Graph& graph) const {
    if (!exists()) {
        throw std::runtime_error("checkpoint does not exist");
    }
    return load_checkpoint(file_, graph);
}

std::uint32_t Store::format() const {
    std::ifstream input(file_, std::ios::binary);
    if (!input) {
        throw std::runtime_error("cannot open checkpoint " + file_.string());
    }
    std::string head;
    char line[64];
    if (!input.getline(line, sizeof(line))) {
        throw std::invalid_argument("checkpoint header is empty");
    }
    head = line;
    if (head == "FRONTIERLOOM_CHECKPOINT 2") {
        return checkpoint_format_v2;
    }
    if (head == "FRONTIERLOOM_CHECKPOINT 1") {
        return checkpoint_format_v1;
    }
    throw std::invalid_argument("checkpoint header is not a supported format");
}

void Store::publish(const Graph& graph, const Shards& shards, const Checkpoint& checkpoint) const {
    checkpoint.validate(graph, shards);
    (void)exists();
    const auto shards_on_disk = serialize_shards(shards, checkpoint);
    const auto generation = checkpoint_generation(checkpoint, shards_on_disk);

    // Stage every shard of the candidate generation first. If the process dies
    // here the manifest still names the previous complete generation, and the
    // half-written directory is named after an unreferenced digest.
    const auto root = shard_root(file_);
    const auto directory = root / generation_directory(generation);
    std::error_code error;
    std::filesystem::create_directories(directory, error);
    if (error) {
        throw std::system_error(error, "creating checkpoint generation directory");
    }
    for (const auto& shard : shards_on_disk) {
        publish_file(shard_path(root, generation, shard.partition), shard.payload);
    }
    sync_directory(directory);
    sync_directory(root);

    // The manifest rename is the single atomic commit point for the generation.
    Generation previous{};
    if (exists()) {
        std::ifstream current(file_, std::ios::binary);
        if (current) {
            previous = manifest_generation(current);
        }
    }
    if (previous == generation) {
        // Re-publishing identical content (e.g. converting twice) keeps one
        // visible generation rather than recording itself as its predecessor.
        previous = 0;
    }
    publish_file(file_, render_manifest(checkpoint, generation, previous, shards_on_disk));

    // Reclaim shard directories neither the manifest nor its predecessor names.
    // The previous generation is intentionally retained so a reader that loaded
    // the old manifest just before the rename still finds one complete image.
    std::set<std::string> live;
    live.insert(generation_directory(generation));
    if (previous != 0) {
        live.insert(generation_directory(previous));
    }
    std::vector<std::filesystem::path> dead;
    std::filesystem::directory_iterator entries(root, error);
    if (!error) {
        for (const auto& entry : entries) {
            const auto name = entry.path().filename().string();
            if (!live.contains(name)) {
                dead.push_back(entry.path());
            }
        }
    }
    for (const auto& path : dead) {
        std::error_code ignored;
        std::filesystem::remove_all(path, ignored);
    }
}

void Store::convert(const Graph& graph) const {
    if (!exists()) {
        throw std::runtime_error("checkpoint does not exist");
    }
    const auto image = load(graph);
    const auto shards = Shards(graph, static_cast<Partition>(image.placement.owners.size()));
    // Republishing is deterministic: a V2 input is re-staged under the same
    // content-addressed generation and its manifest is unchanged, so converting
    // twice yields a single visible target.
    publish(graph, shards, image);
}

}  // namespace frontier
