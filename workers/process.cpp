#include "workers/process.hpp"
#include "graph/text.hpp"

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace frontier {
namespace {

using wire::Decoder;
using wire::Encoder;
using wire::Failure;
using wire::Kind;

void send(const wire::Channel& channel, Kind kind, Encoder body, wire::Channel::Deadline deadline) {
    channel.send(kind, std::move(body).take(), deadline);
}

void encode_header(Encoder& out, WorkerAttempt attempt, Epoch epoch) {
    wire::encode(out, attempt);
    out.u64(epoch);
}

void verify_header(Decoder& in, WorkerAttempt attempt, Epoch epoch) {
    if (wire::decode_attempt(in) != attempt || in.u64() != epoch) {
        throw Failure("worker command or response carries a stale lease or epoch");
    }
}

void close_inherited(int keep) {
    // Closing every other endpoint and the parent's checkpoint lock ensures
    // child EOF and lock lifetime remain tied to their actual owners.
    std::vector<int> descriptors;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
        const auto descriptor = parse_number<int>(entry.path().filename().string(), "descriptor");
        if (descriptor > STDERR_FILENO && descriptor != keep) {
            descriptors.push_back(descriptor);
        }
    }
    for (const auto descriptor : descriptors) {
        (void)::close(descriptor);
    }
}

class Child {
public:
    Child(const Graph& graph, const Shards& shards, const Checkpoint& checkpoint,
          WorkerAttempt attempt, std::span<const PartitionMove> moves)
        : graph_(graph), shards_(shards), program_(checkpoint.program), attempt_(attempt),
          epoch_(checkpoint.next_epoch), assigned_(checkpoint.placement.assigned(attempt.worker)),
          resident_(shards.count()), awaiting_(shards.count()), imported_(shards.count()) {
        for (const auto& move : moves) {
            if (move.next == attempt_.worker) {
                awaiting_.at(move.partition) = true;
            }
        }
        for (const auto part : assigned_) {
            if (!awaiting_[part]) {
                resident_[part] = checkpoint.states.at(part);
            }
        }
    }

    void hello(const wire::Channel& channel) const {
        Encoder reply;
        encode_header(reply, attempt_, epoch_);
        reply.u64(graph_.identity());
        reply.u32(static_cast<std::uint32_t>(::getpid()));
        reply.u32(static_cast<std::uint32_t>(assigned_.size()));
        for (const auto partition : assigned_) {
            reply.u32(partition);
        }
        send(channel, Kind::hello, std::move(reply), idle_deadline());
    }

    void execute(const wire::Channel& channel) {
        for (;;) {
            const auto frame = channel.receive(idle_deadline());
            Decoder input(frame.body);
            verify_header(input, attempt_, epoch_);
            switch (frame.kind) {
            case Kind::compute:
                compute(channel, input);
                break;
            case Kind::confirm:
                confirm(channel, input);
                break;
            case Kind::export_shard:
                export_shard(channel, input);
                break;
            case Kind::import_shard:
                import_shard(channel, input);
                break;
            case Kind::close: {
                input.finish();
                if (!pending_.empty()) {
                    throw Failure("cannot retire a worker with unresolved receipts");
                }
                Encoder reply;
                encode_header(reply, attempt_, epoch_);
                send(channel, Kind::closed, std::move(reply), idle_deadline());
                return;
            }
            default:
                throw Failure("unexpected worker command");
            }
        }
    }

private:
    static wire::Channel::Deadline idle_deadline() {
        return wire::Channel::Clock::now() + std::chrono::hours(24);
    }

    void require_owned(const StepInput& work) const {
        if (work.epoch != epoch_ ||
            !std::binary_search(assigned_.begin(), assigned_.end(), work.partition)) {
            throw Failure("partition work does not belong to the worker lease");
        }
        validate_step_input(graph_, shards_, program_, work);
    }

    void require_resident(const StepInput& work) const {
        require_owned(work);
        if (awaiting_[work.partition] || work.states != resident_[work.partition] ||
            (imported_[work.partition] && work_digest(work) != *imported_[work.partition])) {
            throw Failure("partition input disagrees with the resident vertex image or imported frontier");
        }
    }

    void compute(const wire::Channel& channel, Decoder& input) {
        if (!pending_.empty() || input.u32() != assigned_.size()) {
            throw Failure("worker has pending acknowledgements or incomplete work");
        }
        std::vector<PartitionResult> results;
        results.reserve(assigned_.size());
        for (const auto part : assigned_) {
            auto work = wire::decode_work(input, graph_, shards_);
            if (work.partition != part) {
                throw Failure("worker received out-of-order partition work");
            }
            require_resident(work);
            results.push_back(run_partition(graph_, shards_, program_, attempt_, work));
        }
        input.finish();

        Encoder reply;
        encode_header(reply, attempt_, epoch_);
        reply.u32(static_cast<std::uint32_t>(results.size()));
        for (auto& result : results) {
            const auto part = result.batch.source_partition;
            wire::encode(reply, result);
            pending_.push_back({epoch_, attempt_, part, batch_digest(result.batch), result.stats});
            resident_[part] = std::move(result.states);
            imported_[part].reset();
        }
        send(channel, Kind::batches, std::move(reply), idle_deadline());
    }

    void confirm(const wire::Channel& channel, Decoder& input) {
        if (pending_.empty() || input.u32() != pending_.size()) {
            throw Failure("worker received an acknowledgement without matching batches");
        }
        for (const auto& receipt : pending_) {
            const auto partition = input.u32();
            const auto digest = input.u64();
            if (partition != receipt.partition || digest != receipt.batch) {
                throw Failure("controller acknowledgement does not bind the sent batch");
            }
        }
        input.finish();

        Encoder reply;
        encode_header(reply, attempt_, epoch_);
        reply.u32(static_cast<std::uint32_t>(pending_.size()));
        for (const auto& receipt : pending_) {
            wire::encode(reply, receipt);
        }
        send(channel, Kind::receipts, std::move(reply), idle_deadline());
        pending_.clear();
        ++epoch_;
    }

    void export_shard(const wire::Channel& channel, Decoder& input) const {
        auto work = wire::decode_work(input, graph_, shards_);
        input.finish();
        if (!pending_.empty()) {
            throw Failure("cannot export a partition during an open superstep");
        }
        require_resident(work);
        Encoder reply;
        encode_header(reply, attempt_, epoch_);
        wire::encode(reply, work);
        reply.u64(work_digest(work));
        send(channel, Kind::exported, std::move(reply), idle_deadline());
    }

    void import_shard(const wire::Channel& channel, Decoder& input) {
        auto work = wire::decode_work(input, graph_, shards_);
        const auto digest = input.u64();
        input.finish();
        require_owned(work);
        if (!pending_.empty() || !awaiting_[work.partition] || work_digest(work) != digest) {
            throw Failure("partition handoff is unexpected or corrupt");
        }
        resident_[work.partition] = std::move(work.states);
        imported_[work.partition] = digest;
        awaiting_[work.partition] = false;

        Encoder reply;
        encode_header(reply, attempt_, epoch_);
        reply.u32(work.partition);
        reply.u64(digest);
        send(channel, Kind::imported, std::move(reply), idle_deadline());
    }

    const Graph& graph_;
    const Shards& shards_;
    const Program& program_;
    WorkerAttempt attempt_;
    Epoch epoch_;
    std::vector<Partition> assigned_;
    std::vector<std::vector<VertexState>> resident_;
    std::vector<bool> awaiting_;
    std::vector<std::optional<Digest>> imported_;
    std::vector<BarrierReceipt> pending_;
};

[[noreturn]] void run_child(int fd, pid_t parent, const Graph& graph, const Shards& shards,
                            const Checkpoint& image, WorkerAttempt attempt,
                            std::span<const PartitionMove> moves) {
    if (::prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || ::getppid() != parent) {
        ::_exit(71);
    }
    const wire::Channel channel(fd);
    try {
        close_inherited(fd);
        Child child(graph, shards, image, attempt, moves);
        child.hello(channel);
        child.execute(channel);
    } catch (const std::exception& error) {
        try {
            Encoder feedback;
            feedback.text(std::string_view(error.what()).substr(0, 4096));
            send(channel, Kind::error, std::move(feedback),
                 wire::Channel::Clock::now() + std::chrono::milliseconds(100));
        } catch (...) {
            // This includes normal parent shutdown after its last receipt.
        }
        ::_exit(72);
    }
    ::_exit(0);
}

}  // namespace

WorkerProcess::WorkerProcess(const Graph& graph, const Shards& shards, const Checkpoint& checkpoint,
                             Worker worker, std::span<const PartitionMove> moves,
                             std::chrono::milliseconds timeout)
    : attempt_{worker, checkpoint.placement.generation, checkpoint.incarnation},
      assigned_(checkpoint.placement.assigned(worker)), timeout_(timeout) {
    if (attempt_.incarnation == 0) {
        throw std::logic_error("cannot start worker without an allocated incarnation");
    }
    int sockets[2];
    if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0, sockets) != 0) {
        throw Failure("creating worker socketpair: " + std::string(std::strerror(errno)));
    }
    auto controller = wire::Channel(sockets[0]);
    auto child = wire::Channel(sockets[1]);
    const auto parent = ::getpid();
    pid_ = ::fork();
    if (pid_ == 0) {
        controller.close();
        run_child(sockets[1], parent, graph, shards, checkpoint, attempt_, moves);
    }
    if (pid_ < 0) {
        throw Failure("forking worker: " + std::string(std::strerror(errno)));
    }
    child.close();
    channel_ = std::move(controller);

    try {
        const auto frame = channel_.expect(Kind::hello, deadline());
        auto input = reply_header(frame, checkpoint.next_epoch);
        if (input.u64() != graph.identity() || input.u32() != static_cast<std::uint32_t>(pid_) ||
            input.u32() != assigned_.size()) {
            throw Failure("worker startup handshake disagrees with controller");
        }
        for (const auto part : assigned_) {
            if (input.u32() != part) {
                throw Failure("worker startup acknowledged the wrong assignment");
            }
        }
        input.finish();
    } catch (...) {
        terminate();
        throw;
    }
}

WorkerProcess::~WorkerProcess() {
    terminate();
}

void WorkerProcess::terminate() noexcept {
    channel_.close();
    if (pid_ > 0) {
        (void)::kill(pid_, SIGKILL);
        while (::waitpid(pid_, nullptr, 0) < 0 && errno == EINTR) {
        }
        pid_ = -1;
    }
}

wire::Channel::Deadline WorkerProcess::deadline() const {
    return wire::Channel::Clock::now() + timeout_;
}

Decoder WorkerProcess::reply_header(const wire::Frame& frame, Epoch epoch) const {
    Decoder input(frame.body);
    verify_header(input, attempt_, epoch);
    return input;
}

void WorkerProcess::submit(const Checkpoint& image, double original_dangling) const {
    Encoder request;
    encode_header(request, attempt_, image.next_epoch);
    request.u32(static_cast<std::uint32_t>(assigned_.size()));
    for (const auto part : assigned_) {
        wire::encode(request, image.work(part, original_dangling));
    }
    send(channel_, Kind::compute, std::move(request), deadline());
}

std::vector<PartitionResult> WorkerProcess::receive(const Graph& graph, const Shards& shards,
                                                    Epoch epoch) const {
    const auto frame = channel_.expect(Kind::batches, deadline());
    auto input = reply_header(frame, epoch);
    if (input.u32() != assigned_.size()) {
        throw Failure("worker omitted partition results");
    }
    std::vector<PartitionResult> results;
    results.reserve(assigned_.size());
    for (const auto part : assigned_) {
        auto result = wire::decode_result(input, graph, shards);
        if (result.batch.source_partition != part || result.batch.attempt != attempt_ ||
            result.batch.epoch != epoch || result.stats.emitted != result.batch.messages.size()) {
            throw Failure("worker result has an incorrect partition, lease, epoch, or emission count");
        }
        results.push_back(std::move(result));
    }
    input.finish();
    return results;
}

void WorkerProcess::confirm(Epoch epoch, std::span<const StagedBatch> batches) const {
    Encoder request;
    encode_header(request, attempt_, epoch);
    request.u32(static_cast<std::uint32_t>(batches.size()));
    for (const auto& stage : batches) {
        request.u32(stage.partition);
        request.u64(stage.digest);
    }
    send(channel_, Kind::confirm, std::move(request), deadline());
}

std::vector<BarrierReceipt> WorkerProcess::receipts(Epoch epoch) const {
    const auto frame = channel_.expect(Kind::receipts, deadline());
    auto input = reply_header(frame, epoch);
    if (input.u32() != assigned_.size()) {
        throw Failure("worker omitted partition receipts");
    }
    std::vector<BarrierReceipt> result;
    result.reserve(assigned_.size());
    for (const auto part : assigned_) {
        auto receipt = wire::decode_receipt(input);
        if (receipt.partition != part || receipt.epoch != epoch || receipt.attempt != attempt_) {
            throw Failure("worker receipt carries the wrong partition, epoch or attempt");
        }
        result.push_back(receipt);
    }
    input.finish();
    return result;
}

StepInput WorkerProcess::export_shard(const StepInput& work, const Graph& graph,
                                      const Shards& shards) const {
    Encoder request;
    encode_header(request, attempt_, work.epoch);
    wire::encode(request, work);
    send(channel_, Kind::export_shard, std::move(request), deadline());

    const auto frame = channel_.expect(Kind::exported, deadline());
    auto input = reply_header(frame, work.epoch);
    auto exported = wire::decode_work(input, graph, shards);
    const auto digest = input.u64();
    input.finish();
    if (exported != work || digest != work_digest(work)) {
        throw Failure("exported image differs from the last committed handoff boundary");
    }
    return exported;
}

void WorkerProcess::import_shard(const StepInput& work) const {
    const auto digest = work_digest(work);
    Encoder request;
    encode_header(request, attempt_, work.epoch);
    wire::encode(request, work);
    request.u64(digest);
    send(channel_, Kind::import_shard, std::move(request), deadline());

    const auto frame = channel_.expect(Kind::imported, deadline());
    auto input = reply_header(frame, work.epoch);
    if (input.u32() != work.partition || input.u64() != digest) {
        throw Failure("destination did not acknowledge the transferred partition image");
    }
    input.finish();
}

bool WorkerProcess::retire(Epoch epoch) noexcept {
    if (pid_ <= 0) {
        return false;
    }
    const auto limit = wire::Channel::Clock::now() + std::min(timeout_, std::chrono::milliseconds(250));
    try {
        Encoder request;
        encode_header(request, attempt_, epoch);
        send(channel_, Kind::close, std::move(request), limit);
        const auto frame = channel_.expect(Kind::closed, limit);
        reply_header(frame, epoch).finish();
        channel_.close();

        while (wire::Channel::Clock::now() < limit) {
            int status{};
            const auto reaped = ::waitpid(pid_, &status, WNOHANG);
            if (reaped == pid_) {
                pid_ = -1;
                return WIFEXITED(status) && WEXITSTATUS(status) == 0;
            }
            if (reaped < 0 && errno != EINTR) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    } catch (...) {
        // Retirement is best effort once a successor or committed image exists.
    }
    terminate();
    return false;
}

}  // namespace frontier
