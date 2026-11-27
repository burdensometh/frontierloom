#include "workers/metrics.hpp"

#include <iomanip>
#include <ostream>

namespace frontier {
namespace {

void print_atom(std::ostream& output, std::string_view value) {
    output << '"';
    for (const auto character : value) {
        switch (character) {
        case '\n': output << "\\n"; break;
        case '\r': output << "\\r"; break;
        case '"': output << "\\\""; break;
        case '\\': output << "\\\\"; break;
        default: output << character; break;
        }
    }
    output << '"';
}

const char* event_name(Event::Kind kind) {
    switch (kind) {
    case Event::Kind::started:   return "workers";
    case Event::Kind::committed: return "commit";
    case Event::Kind::retry:     return "retry";
    case Event::Kind::fault:     return "fault";
    case Event::Kind::scaled:    return "scale";
    case Event::Kind::retired:   return "retire";
    }
    throw std::invalid_argument("unknown engine event");
}

void print_progress(std::ostream& output, const StepSummary& step) {
    output << " receipts=" << step.receipts
           << " active=" << step.stats.active
           << " consumed=" << step.stats.consumed
           << " emitted=" << step.stats.emitted
           << " combined=" << step.combined
           << " mailbox=" << step.mailbox
           << " dangling=" << step.stats.dangling
           << " residual=" << step.stats.residual
           << " rank_mass=" << step.stats.rank_mass
           << " stop=" << name(step.stop);
}

void print_timing(std::ostream& output, const StepTiming& timing) {
    output << " dispatch_us=" << timing.dispatch_us
           << " staging_us=" << timing.staging_us
           << " confirm_us=" << timing.confirm_us
           << " merge_us=" << timing.merge_us
           << " publish_us=" << timing.publish_us
           << " wire_sent=" << timing.wire_sent
           << " wire_received=" << timing.wire_received;
}

}  // namespace

const char* name(FaultPoint point) {
    switch (point) {
    case FaultPoint::after_send:     return "after-send";
    case FaultPoint::before_commit:  return "before-commit";
    case FaultPoint::before_replace: return "before-replace";
    case FaultPoint::after_replace:  return "after-replace";
    }
    throw std::invalid_argument("unknown fault point");
}

void print_event(std::ostream& output, const Event& event) {
    output << std::setprecision(17) << "event=" << event_name(event.kind)
           << " epoch=" << event.epoch << " topology=" << event.topology
           << " incarnation=" << event.incarnation;
    if (!event.pids.empty()) {
        output << " pids=";
        const char* separator = "";
        for (const auto pid : event.pids) {
            output << separator << pid;
            separator = ",";
        }
    }
    if (event.step) {
        print_progress(output, *event.step);
    }
    if (event.timing) {
        print_timing(output, *event.timing);
    }
    if (event.fault) {
        output << " point=" << name(event.fault->point) << " worker=" << event.fault->worker
               << " staged=" << event.staged;
    }
    if (event.kind == Event::Kind::scaled) {
        output << " moved=" << event.moved << " peak_edges=" << event.peak_edges;
    }
    if (event.kind == Event::Kind::retired) {
        output << " graceful=" << event.graceful << " total=" << event.pids.size();
    }
    if (!event.detail.empty()) {
        output << " detail=";
        print_atom(output, event.detail);
    }
    output << '\n' << std::flush;
}

void print_totals(std::ostream& output, const Checkpoint& checkpoint) {
    const auto& totals = checkpoint.totals;
    output << "result algorithm=" << name(checkpoint.program.algorithm)
           << " stop=" << name(checkpoint.stop())
           << " updates=" << checkpoint.updates()
           << " next_epoch=" << checkpoint.next_epoch
           << " topology=" << checkpoint.placement.generation
           << " incarnation=" << checkpoint.incarnation
           << " workers=" << checkpoint.placement.workers
           << " partitions=" << checkpoint.placement.owners.size()
           << " active=" << totals.active
           << " consumed=" << totals.consumed
           << " emitted=" << totals.emitted
           << " combined=" << totals.combined
           << " pending=" << checkpoint.mailbox.combined_count()
           << " recovered_failures=" << totals.recovered_failures
           << " resumes=" << totals.resumes
           << " scales=" << totals.scales
           << " moved_partitions=" << totals.moved_partitions << '\n';
}

}  // namespace frontier
