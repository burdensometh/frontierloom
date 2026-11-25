#include "checkpoint/store.hpp"

#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <fstream>
#include <locale>
#include <sstream>
#include <sys/file.h>
#include <system_error>
#include <unistd.h>

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
            fail_os("writing checkpoint temporary file");
        }
        if (count == 0) {
            throw std::runtime_error("checkpoint write made no progress");
        }
        bytes.remove_prefix(static_cast<std::size_t>(count));
    }
    sync(fd, "syncing checkpoint temporary file");
}

class Temporary {
public:
    explicit Temporary(const std::filesystem::path& checkpoint)
        : path_(checkpoint.string() + ".tmp.XXXXXX"), fd_(::mkostemp(path_.data(), O_CLOEXEC)) {
        if (fd_.get() < 0) {
            fail_os("creating checkpoint temporary file");
        }
    }

    ~Temporary() {
        if (!path_.empty()) {
            (void)::unlink(path_.c_str());
        }
    }

    void replace(const std::filesystem::path& checkpoint, std::string_view bytes) {
        persist(fd_.get(), bytes);
        if (::close(fd_.release()) < 0) {
            fail_os("closing checkpoint temporary file");
        }
        if (::rename(path_.c_str(), checkpoint.c_str()) < 0) {
            fail_os("publishing checkpoint");
        }
        path_.clear();
    }

private:
    std::string path_;
    Descriptor fd_;
};

}  // namespace

Checkpoint load_checkpoint(const std::filesystem::path& file, const Graph& graph) {
    std::ifstream input(file, std::ios::binary);
    if (!input) {
        throw std::runtime_error("cannot open checkpoint " + file.string());
    }
    return read_checkpoint_v1(input, graph);
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

void Store::publish(const Graph& graph, const Shards& shards, const Checkpoint& checkpoint) const {
    checkpoint.validate(graph, shards);
    (void)exists();
    std::ostringstream stream;
    stream.imbue(std::locale::classic());
    write_checkpoint_v1(stream, checkpoint);
    auto bytes = std::move(stream).str();

    Descriptor directory(::open(file_.parent_path().c_str(), O_RDONLY | O_CLOEXEC | O_DIRECTORY));
    if (directory.get() < 0) {
        fail_os("opening the checkpoint directory");
    }
    Temporary temporary(file_);
    temporary.replace(file_, bytes);
    sync(directory.get(), "checkpoint renamed but directory sync failed");
}

}  // namespace frontier
