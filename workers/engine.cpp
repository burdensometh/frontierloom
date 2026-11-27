#include "workers/engine.hpp"
#include "workers/pool.hpp"

#include <algorithm>
#include <set>
#include <stdexcept>

namespace frontier {
namespace {

enum class Mode { compute, resume, scale };

class Run {
public:
    Run(const Graph& graph, Store& store, Checkpoint image,
        const Runtime& runtime, const Observer& observer)
        : graph_(graph), shards_(graph, static_cast<Partition>(image.placement.owners.size())),
          store_(store), image_(std::move(image)), runtime_(runtime), observer_(observer),
          initial_dangling_(initial_dangling(graph, image_.program)),
          faults_used_(runtime.faults.size()), actions_(runtime.scaling) {
        validate_options();
        std::sort(actions_.begin(), actions_.end(), [](const auto& left, const auto& right) {
            return left.epoch < right.epoch;
        });
    }

    Checkpoint execute(Mode mode) {
        if (mode == Mode::compute) {
            store_.publish(graph_, shards_, image_);
        }
        if (mode == Mode::scale) {
            apply_scale_only();
            retire_current();
            return std::move(image_);
        }
        if (finished()) {
            return std::move(image_);
        }
        if (mode == Mode::resume) {
            image_.totals.resumes = next_generation(image_.totals.resumes);
        }

        while (!finished()) {
            try {
                ensure_pool();
                if (scale_due()) {
                    apply_scale(actions_[next_action_].workers);
                    ++next_action_;
                    failures_here_ = 0;
                }

                force_replacement_if_requested();
                advance_once();
                ++commits_here_;
                failures_here_ = 0;
                requested_replacement_ = false;
            } catch (const wire::Failure& failure) {
                recover(failure);
            }
        }
        retire_current();
        return std::move(image_);
    }

private:
    void validate_options() const {
        if (runtime_.timeout < std::chrono::milliseconds(20) ||
            runtime_.timeout > std::chrono::minutes(5) || runtime_.retry_limit > 100 ||
            (runtime_.stop_after && *runtime_.stop_after > max_steps + 1)) {
            throw std::invalid_argument("invalid runtime timeout, commit budget or retry count");
        }
        std::set<Epoch> scales;
        for (const auto& action : actions_) {
            if (action.epoch < image_.next_epoch || action.epoch > max_steps + 1 ||
                action.workers == 0 || action.workers > max_workers || action.workers > shards_.count() ||
                !scales.insert(action.epoch).second) {
                throw std::invalid_argument("invalid or duplicate planned scale operation");
            }
        }
        std::set<std::pair<Epoch, FaultPoint>> faults;
        for (const auto& fault : runtime_.faults) {
            if (fault.epoch < image_.next_epoch || fault.epoch > max_steps ||
                fault.worker >= max_workers || !faults.emplace(fault.epoch, fault.point).second) {
                throw std::invalid_argument("invalid or duplicate fault boundary");
            }
        }
    }

    [[nodiscard]] bool finished() const {
        return image_.stop() != Stop::running ||
            (runtime_.stop_after && commits_here_ >= *runtime_.stop_after);
    }

    [[nodiscard]] bool scale_due() const {
        return next_action_ < actions_.size() && actions_[next_action_].epoch == image_.next_epoch;
    }

    void emit(Event event) const {
        if (observer_) {
            observer_(event);
        }
    }

    [[nodiscard]] Event event(Event::Kind kind) const {
        Event result{};
        result.kind = kind;
        result.epoch = image_.next_epoch;
        result.topology = image_.placement.generation;
        result.incarnation = image_.incarnation;
        return result;
    }

    std::optional<std::size_t> pending(FaultPoint point) const {
        for (std::size_t index = 0; index < runtime_.faults.size(); ++index) {
            const auto& fault = runtime_.faults[index];
            if (!faults_used_[index] && fault.point == point && fault.epoch == image_.next_epoch) {
                return index;
            }
        }
        return std::nullopt;
    }

    Fault use_fault(std::size_t index, std::uint64_t staged = 0) {
        const auto fault = runtime_.faults[index];
        if (fault.point != FaultPoint::before_commit && fault.worker >= image_.placement.workers) {
            throw std::invalid_argument("fault targets a worker absent at its configured epoch");
        }
        faults_used_[index] = true;
        auto observation = event(Event::Kind::fault);
        observation.staged = staged;
        observation.fault = fault;
        emit(std::move(observation));
        return fault;
    }

    void reserve_incarnation() {
        image_.incarnation = next_generation(image_.incarnation);
        store_.publish(graph_, shards_, image_);
    }

    void ensure_pool() {
        if (pool_) {
            return;
        }
        if (replacing_) {
            if (const auto fault = pending(FaultPoint::before_replace)) {
                (void)use_fault(*fault);
                throw wire::Failure("replacement interrupted before allocating a new worker lease");
            }
        }

        reserve_incarnation();
        pool_ = std::make_unique<WorkerPool>(graph_, shards_, image_,
                                             std::span<const PartitionMove>{}, runtime_.timeout);
        auto observation = event(Event::Kind::started);
        observation.pids = pool_->pids();
        emit(std::move(observation));

        if (replacing_) {
            if (const auto fault = pending(FaultPoint::after_replace)) {
                const auto configured = use_fault(*fault);
                pool_->kill(configured.worker);
                throw wire::Failure("replacement worker lost after the new lease was acknowledged");
            }
        }
        replacing_ = false;
    }

    void force_replacement_if_requested() {
        if (requested_replacement_) {
            return;
        }
        auto request = pending(FaultPoint::before_replace);
        if (!request) {
            request = pending(FaultPoint::after_replace);
        }
        if (!request) {
            return;
        }
        const auto worker = runtime_.faults[*request].worker;
        if (worker >= image_.placement.workers) {
            throw std::invalid_argument("replacement targets a worker absent at its configured epoch");
        }
        requested_replacement_ = true;
        pool_->kill(worker);
        throw wire::Failure("worker lost to start controlled replacement");
    }

    void advance_once() {
        auto step = pool_->step(image_, initial_dangling_, [&](Worker worker, std::uint64_t staged) {
            if (const auto fault = pending(FaultPoint::after_send)) {
                const auto target = runtime_.faults[*fault].worker;
                if (target >= image_.placement.workers) {
                    throw std::invalid_argument("after-send fault targets a worker absent at this epoch");
                }
                if (target == worker) {
                    (void)use_fault(*fault, staged);
                    pool_->kill(worker);
                }
            }
        });
        if (const auto fault = pending(FaultPoint::before_commit)) {
            (void)use_fault(*fault, step.step.stats.emitted);
            throw wire::Failure("interrupted after receipts and before publishing the superstep");
        }
        auto committed = event(Event::Kind::committed);
        committed.step = step.step;
        advance(image_, step.step, std::move(step.states), std::move(step.inbox));
        const auto publish_started = std::chrono::steady_clock::now();
        store_.publish(graph_, shards_, image_);
        const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - publish_started);
        step.timing.publish_us = static_cast<std::uint64_t>(elapsed.count());
        committed.timing = step.timing;
        emit(std::move(committed));
    }

    void retire_current() {
        if (pool_) {
            retire(*pool_, event(Event::Kind::retired));
            pool_.reset();
        }
    }

    void retire(WorkerPool& pool, Event retired) const {
        retired.pids = pool.pids();
        retired.graceful = pool.retire(retired.epoch);
        emit(std::move(retired));
    }

    void apply_scale(Worker workers) {
        const auto plan = budgeted_plan(workers);
        if (plan.target == image_.placement) {
            return;
        }
        const auto old_incarnation = image_.incarnation;
        reserve_incarnation();
        auto candidate = image_;
        candidate.placement = plan.target;
        candidate.totals.scales = next_generation(candidate.totals.scales);
        candidate.totals.moved_partitions += plan.moves.size();
        if (candidate.totals.moved_partitions < image_.totals.moved_partitions) {
            throw std::overflow_error("migrated partition count exhausted");
        }

        auto next = std::make_unique<WorkerPool>(graph_, shards_, candidate, plan.moves, runtime_.timeout);
        pool_->transfer_to(*next, image_, initial_dangling_, plan.moves);
        store_.publish(graph_, shards_, candidate);
        auto retired = event(Event::Kind::retired);
        retired.incarnation = old_incarnation;
        image_ = std::move(candidate);
        pool_.swap(next);
        auto observation = event(Event::Kind::scaled);
        observation.moved = plan.moves.size();
        const auto loads = worker_edge_loads(shards_, graph_, image_.placement);
        observation.peak_edges = *std::max_element(loads.begin(), loads.end());
        observation.pids = pool_->pids();
        emit(std::move(observation));
        retire(*next, std::move(retired));
    }

    // Produces the exact placement that the `plan` command would display for the
    // same checkpoint, target and budget. A configured edge budget is mandatory:
    // if no target placement keeps every worker within the budget the transition
    // is refused, never downgraded to an over-budget layout.
    [[nodiscard]] ScalePlan budgeted_plan(Worker workers) const {
        if (!runtime_.edge_budget) {
            return plan_scale(image_.placement, workers);
        }
        auto plan = plan_scale_budgeted(shards_, graph_, image_.placement, workers,
                                        *runtime_.edge_budget);
        if (!plan) {
            throw std::runtime_error(
                "edge budget " + std::to_string(*runtime_.edge_budget) +
                " cannot be met scaling to " + std::to_string(workers) + " workers at epoch " +
                std::to_string(image_.next_epoch) + "; refusing to install the placement");
        }
        return std::move(*plan);
    }

    void apply_scale_only() {
        if (actions_.size() != 1 || !scale_due()) {
            throw std::logic_error("explicit scale is missing a transition");
        }
        const auto target = actions_.front().workers;
        // Without a budget a same-size transition is a no-op. With one it may
        // still need to rebalance ownership, so fall through to planning.
        if (!runtime_.edge_budget && target == image_.placement.workers) {
            return;
        }
        for (;;) {
            try {
                ensure_pool();
                apply_scale(target);
                return;
            } catch (const wire::Failure& failure) {
                recover(failure);
            }
        }
    }

    void recover(const wire::Failure& failure) {
        auto observation = event(Event::Kind::retry);
        observation.detail = failure.what();
        emit(std::move(observation));
        pool_.reset();
        if (failures_here_ >= runtime_.retry_limit) {
            throw std::runtime_error("worker retry budget exhausted at epoch " +
                                     std::to_string(image_.next_epoch) + ": " + failure.what());
        }
        ++failures_here_;
        replacing_ = true;
        image_ = store_.load(graph_);
        image_.totals.recovered_failures = next_generation(image_.totals.recovered_failures);
        store_.publish(graph_, shards_, image_);
    }

    const Graph& graph_;
    Shards shards_;
    Store& store_;
    Checkpoint image_;
    const Runtime& runtime_;
    const Observer& observer_;
    double initial_dangling_;
    std::vector<bool> faults_used_;
    std::vector<ScaleAt> actions_;
    std::unique_ptr<WorkerPool> pool_;
    std::size_t next_action_{};
    std::uint32_t commits_here_{};
    std::uint32_t failures_here_{};
    bool replacing_{};
    bool requested_replacement_{};
};

}  // namespace

Checkpoint compute(const Graph& graph, Store& store, const Start& start,
                   const Runtime& runtime, const Observer& observer) {
    if (store.exists()) {
        throw std::invalid_argument("checkpoint already exists; use resume to keep committed progress");
    }
    const Shards shards(graph, start.partitions);
    Run run(graph, store, bootstrap(graph, shards, start.program, start.workers), runtime, observer);
    return run.execute(Mode::compute);
}

Checkpoint resume(const Graph& graph, Store& store, const Runtime& runtime, const Observer& observer) {
    Run run(graph, store, store.load(graph), runtime, observer);
    return run.execute(Mode::resume);
}

Checkpoint rescale(const Graph& graph, Store& store, Worker workers, const Runtime& runtime,
                   const Observer& observer) {
    if (!runtime.scaling.empty() || !runtime.faults.empty() || runtime.stop_after) {
        throw std::invalid_argument("explicit scale accepts only timeout and retry runtime controls");
    }
    Runtime scaling = runtime;
    auto image = store.load(graph);
    scaling.scaling.push_back({image.next_epoch, workers});
    Run run(graph, store, std::move(image), scaling, observer);
    return run.execute(Mode::scale);
}

}  // namespace frontier
