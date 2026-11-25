#pragma once

#include "barrier/barrier.hpp"

#include <iosfwd>

namespace frontier {

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

Checkpoint bootstrap(const Graph& graph, const Shards& shards, Program program, Worker workers);
void advance(Checkpoint& checkpoint, const StepSummary& step,
             std::vector<std::vector<VertexState>> states, Mailbox mailbox);

Checkpoint read_checkpoint_v1(std::istream& input, const Graph& graph);
void write_checkpoint_v1(std::ostream& output, const Checkpoint& checkpoint);
Digest work_digest(const StepInput& work);

}  // namespace frontier
