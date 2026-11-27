# FrontierLoom

FrontierLoom runs bulk-synchronous BFS and PageRank over directed graphs. A Linux coordinator assigns fixed graph partitions to forked worker processes. Workers calculate vertex state and scatter messages over Unix socket pairs. The coordinator seals the next inbox only after every partition acknowledges its batch, then publishes a checkpoint. Checkpoints are written as sharded V2 images: a small manifest plus one file per partition, so a half-saved snapshot still exposes the previous complete state. V1 images remain readable and can be converted in place.

## Build and run

The engine uses C++20, the standard library and Linux process and filesystem APIs. GCC 12 and GNU Make are sufficient. CMake 3.20 or newer with CTest is also supported.

```sh
make -j2 all
make check
bash scripts/demo.sh build
bash scripts/bench.sh build
```

```sh
cmake -S . -B build-cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build-cmake -j2
ctest --test-dir build-cmake --output-on-failure
```

The demo prints the output directory, worker PIDs, attempt and topology generations, barriers, partition transfers, and verification results. It executes both algorithms, compares BFS with a checked-in vector, resumes the stock v1 snapshot, and triggers all four failure boundaries. Supply a fresh output directory as the second argument to either script. The benchmark also accepts vertex count and repeat count after that directory.

## Compute and recover

```sh
build/frontierloom compute --graph samples/campus.v1.graph \
  --algorithm bfs --source 10 --config config/baseline.conf \
  --checkpoint runs/campus.flc --stop-after 2

build/frontierloom inspect --graph samples/campus.v1.graph \
  --checkpoint runs/campus.flc --details --audit

build/frontierloom plan --graph samples/campus.v1.graph \
  --checkpoint runs/campus.flc --workers 4

build/frontierloom scale --graph samples/campus.v1.graph \
  --checkpoint runs/campus.flc --workers 4

build/frontierloom resume --graph samples/campus.v1.graph \
  --checkpoint runs/campus.flc --verify --vector -

build/frontierloom convert --graph samples/campus.v1.graph \
  --checkpoint runs/campus.flc
```

Each successful commit advances the persisted cursor by one superstep. `--stop-after` counts commits in the current invocation. `--stop-after 0` on a new run writes the initial image. Resume reads the graph identity, partition map, algorithm, tuning parameters, state and pending messages from the committed image. A failed compute with an existing snapshot can be continued through the same resume entry point.

A checkpoint is a sharded V2 image: a manifest at the checkpoint path and one shard file per partition under `<checkpoint>.shards/<generation>/`. Publication writes and syncs every shard first, then atomically renames the new manifest into place, so the manifest is the single commit point. If only some shard files of a new generation are saved, the manifest still names the previous generation and reading returns the previous complete state. Graph identity, the superstep cursor and the pending inbox come only from the manifest; resident vertex state comes only from the shards that generation names. A missing, corrupt or cross-generation shard is rejected as an incomplete image, never silently mixed. `convert` rewrites any checkpoint, including a stock V1 image, as V2; converting twice recomputes the same content-addressed generation and leaves one visible target.

```sh
build/frontierloom compute --graph samples/dangling.v1.graph \
  --algorithm pagerank --partitions 8 --workers 3 --damping 0.85 \
  --tolerance 1e-10 --limit 100 --checkpoint runs/rank.flc \
  --fault after-send:1:1 --verify --vector runs/rank.csv
```

BFS starts with one source message of distance zero. A vertex forwards a distance only when its stored distance improves. The barrier stops on an empty outgoing frontier or reports a step limit. PageRank starts uniformly, scatters the starting rank at epoch zero and uses compensated, fixed-order reductions. Starting at epoch one, the update redistributes dangling mass uniformly and stops when the L1 residual reaches the configured tolerance or the update limit.

`reference` runs queue-based BFS or a long-double incoming-edge PageRank solver in one process. `--verify` checks BFS distances and termination exactly and checks PageRank against a vector L1 error bound of `1e-8`. A paused result reports deferred verification. `inspect --audit` independently reconstructs the latest frontier from the stored state, including BFS path causality and cumulative message counts or PageRank contributions in source order.

CSV goes to the specified fresh file or to stdout with `--vector -`. Rows use ascending external IDs. Process and barrier events go to stderr, where `commit` reports raw emitted records, merged destination entries, consumed entries, residual, wall time by phase and bytes transferred over IPC. Recovery and topology events also report the cursor and incarnation. `--quiet` suppresses progress events. The CLI exits with 2 on input or runtime error and 3 on a failed reference comparison.

## Scale and faults

The partition count stays fixed across scaling. Partition assignment uses a stable hash of the external vertex ID. Rebalancing chooses deterministic worker quotas and retains existing ownership wherever capacity allows. The old worker exports each moved shard's resident vertex state and coordinator-supplied pending inbox. The new worker imports that image under its new lease and returns a digest. Once all transfers are acknowledged, the coordinator publishes the new ownership map and retires the old workers.

`scale` applies a new placement at the checkpoint's current boundary. Repeat `--scale-at EPOCH:WORKERS` on compute or resume to schedule transitions before specific supersteps. The schedule belongs to that invocation. The `plan` command shows proposed moves and predicted cross-worker edges without opening a writer.

| Point | Effect |
| --- | --- |
| `after-send:EPOCH[:WORKER]` | Kills the worker after its batch has been staged and before the worker can confirm it. |
| `before-commit:EPOCH` | Interrupts the coordinator after receipt and reduction, before checkpoint publication. |
| `before-replace:EPOCH[:WORKER]` | Causes a worker loss and interrupts the subsequent replacement before it obtains a lease. |
| `after-replace:EPOCH[:WORKER]` | Kills a replacement worker after startup acknowledges its new lease. |

Fault directives are one-shot per invocation. Worker IDs start at zero. The default worker for a directive is zero. Each retry reloads the committed image, reserves a new incarnation durably and starts a fresh group. The default retry budget is four per commit or scale boundary. `--retry 0` exposes the failed boundary for a later resume. `--timeout-ms` sets the maximum duration of each worker exchange. The coordinator owns one writable checkpoint path per run. Start separate invocations to run separate graphs concurrently.

## Source and diagnostics

| Directory | Responsibility |
| --- | --- |
| `graph/` | Canonical manifest, forward and reverse adjacency, graph identity, deterministic placement and load measurement |
| `kernel/` | BFS, PageRank, vertex and contribution contracts, independent serial solver |
| `mailbox/` | Attempt-scoped stage and confirm, deterministic destination reductions |
| `barrier/` | Complete receipt set, message waterlines, progress and stopping |
| `checkpoint/` | V1 codec, sharded V2 publication and conversion, durable writer lock and offline audit |
| `workers/` | Linux process supervision, framed transport, retries, migration and metrics |

The graph, checkpoint and seed fixture contracts are documented in [v1 formats](docs/v1.md). The operator sample contains a cycle, a self-loop, a converging BFS frontier and an isolated vertex. The PageRank sample includes a dangling vertex. The benchmark uses deterministic high-out-degree PageRank and sparse-frontier BFS inputs, verifies each vector, then times warm checkpoint parsing and performs a real resume. Timing includes regular checkpoint publication for compute cases. Recovery-read timing covers parsing and validation from a file in the OS cache.
