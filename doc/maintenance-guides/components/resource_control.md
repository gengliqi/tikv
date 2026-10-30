# `components/resource_control` Maintenance Guide

## Purpose And Scope

`resource_control` owns request-level fairness and resource-isolation logic for
resource groups. It watches PD metadata, maintains group state, wraps futures
and channels with accounting, and adjusts limiters over time.

## Architectural Views

### Policy view

- resource-group definition and runtime state
- limiter implementation
- fairness and admission control policy
- PD watch/report control plane

### Runtime view

- hot-path wrappers on futures/channels
- periodic background adjustment workers
- asynchronous PD metadata synchronization

## Process Lifecycle And Startup Sequencing

- The owning process starts this subsystem from server bootstrap.
- Periodic tasks are started by `start_periodic_tasks`:
  min-virtual-time advancement, PD watch loop, quota adjustment, RU reporting.
- Shutdown safety depends on the owning worker/runtime lifecycle, so long-lived
  tasks here should remain cancellation-safe and retry-safe.

Concrete runtime anchors:

- periodic-task setup:
  `lib.rs::start_periodic_tasks`
- PD watch and reload loop:
  `service.rs`
- quota adjustment:
  `worker.rs`

## Data Model And Metadata Contracts

- Resource-group definitions come from PD meta storage.
- Runtime metadata includes:
  group mode, quotas, virtual-time state, baseline usage, limiter statistics,
  admission decisions.
- Background usage reports back to PD as request-unit accounting.

Hot contracts to review carefully:

- group-name identity and default/background-group semantics
- read-policy generation, positive weights, and priority updates
- shared baseline-window accounting for foreground write admission
- RU accounting inputs and report-unit interpretation

## Start Here

- `components/resource_control/src/lib.rs`
- `components/resource_control/src/resource_group.rs`
- `components/resource_control/src/resource_limiter.rs`
- `components/resource_control/src/service.rs`
- `components/resource_control/src/config.rs`
- `components/resource_control/src/worker.rs`
- `components/resource_control/src/score.rs`
- `components/resource_control/src/future.rs`
- `components/resource_control/src/channel.rs`
- `src/read_pool.rs` (external caller coupled to the read-pool contract below)

## Must-Read File Order

1. `components/resource_control/src/lib.rs`
2. `components/resource_control/src/config.rs`
3. `components/resource_control/src/resource_group.rs`
4. `components/resource_control/src/resource_limiter.rs`
5. `components/resource_control/src/service.rs`
6. `components/resource_control/src/worker.rs`
7. `components/resource_control/src/score.rs`
8. `components/resource_control/src/future.rs`
9. `components/resource_control/src/channel.rs`

## Main Responsibilities

- maintain `ResourceGroupManager`
- track per-group quotas and consumption
- wrap execution with `ControlledFuture` / `with_resource_limiter`
- watch resource-group definitions from PD meta storage
- report background RU usage back to PD
- adjust quotas and throttling behavior periodically
- expose fair scheduling and admission-control decisions to callers
- compute common CPU/IO/compaction pressure scores (`score.rs`) shared by
  background quota adjustment and foreground write throttling
- expose stable `GroupPolicy` handles through `GroupProvider` for the unified
  read pool; its scheduling ledger and CPU budget live in `tikv_util`

## Important Design Points

- `service.rs` is the PD-facing control plane. It reloads resource groups,
  watches config paths, and retries on compaction or transient failures.
- `resource_group.rs` is the policy core. It contains:
  - resource group state
  - legacy virtual-time controllers for non-unified-read pools
  - stable policy identities for the unified read scheduler
  - shared usage history and foreground write admission decisions
- `resource_limiter.rs` is the actual limiter and statistics store.
- `worker.rs` adjusts background quota and related runtime state periodically.
  Each tick it measures CPU/IO/compaction inputs, calls
  `score::compute_resource_scores` to get `cpu_score`/`io_score`/
  `compaction_score`, uses `cpu_score`/`io_score` for its own background-quota
  adjustment, and passes `cpu_score` to
  `ResourceGroupManager::online_adjust_resource_quota` so the same signal
  drives foreground write throttling in `resource_group.rs`. The unified read
  scheduler does not consume this signal or the historical baseline.
- `score.rs` is the shared pressure-scoring module. `compute_resource_scores`
  turns raw CPU/IO/compaction measurements into three independent 0-100
  scores; `pressure_fraction` maps a score onto a `[0, 1]` pressure fraction
  against caller-supplied thresholds; `ThreadGroupCpuTracker` measures CPU
  cores consumed by threads matching a name prefix (e.g.
  `UNIFIED_READ_POOL_THREAD`, `GRPC_SERVER_THREAD`) via `/proc` thread stats.
  This is the one shared code path behind both background quota adjustment
  (`worker.rs`) and foreground write throttling (`resource_group.rs`) — a
  bug here silently skews both.
- `config.rs` defines background/write admission policy knobs. The obsolete
  `enable-fair-scheduling` and `enable-read-admission-control` fields remain
  parseable but are ignored; validation warns when they are enabled.
- Background quota limiting is independent of priority-queue selection. A
  background limiter can throttle a task in place even when the transaction
  scheduler uses its vanilla queue.
- `ResourceController::is_customized` reflects non-default resource groups;
  background configuration is tracked separately by `ResourceGroupManager`.

## Cross-Component Contract: Unified Read Pool

`ResourceGroupManager` implements `tikv_util::yatp_pool::fair_queue::GroupProvider`.
The provider returns an `Arc<GroupPolicy>` containing canonical name, identity,
configured priority, positive weight, and retirement state. It does not read or
mutate scheduling VT. The fair queue owns each priority lane's committed VT,
reservations, idle placement, task history, and CPU gate.

Identity and update rules:

- Names are ASCII-case-insensitive. Unknown names resolve to the existing
  default handle; lookup never creates another scheduling entitlement.
- Ordinary PD updates and watch reloads update the existing handle. They must
  not create a generation or reset queue history.
- Deletion and `retain` retire the removed handle. A later create receives a
  new identity. In-flight work retains its original handle for settlement;
  the queue redirects retired queued work to default.
- Default remains available. Deleting its customized definition restores its
  default policy without retiring its identity.
- Normal finite weights equal read quota (RU fill rate in RU mode, CPU fill
  rate in raw mode). Zero quota and default's unlimited sentinel use
  `min(10 * max(10000, largest normal quota), MAX_RU_QUOTA)` instead.
  Changing this reference weight updates only special handles; ordinary
  groups keep their units and the queue retains committed progress.
- The registry serializes metadata publication. It never calls the scheduler
  or destroys tasks while holding its lock. Consumers must not call the
  provider while holding a scheduler lock that can be reentered.

Read admission and accounting:

- Foreground reads bypass the shared historical limiter's admission delay.
  Its debt/rate is not cleared, because foreground writes may still use it.
- `LimitedFuture` continues recording shared read/write usage. Keeping this
  history preserves existing write policy; splitting read and write history
  would be a separate behavior change.
- Background requests still consult their background limiter. Removing
  historical read admission must not disable background CPU/IO control.
- New read scheduling does not consume the legacy controller's VT or external
  RPC CPU/write-byte penalties. Other pools keep their legacy controllers.
- The global CPU gate and live-task limits belong to the unified read pool.
  There is no Phase bit, historical read-pool CPU floor, pressure-driven
  target, or recovery-to-core release signal. RC unified pools keep their
  configured `max-thread-count`: `auto-adjust-pool-size` and `cpu-threshold`
  do not control that pool, and `read-cpu-ratio` owns its CPU budget. Non-RC
  pools retain their existing CPU-threshold and autoscaling behavior.
- A ready `BTreeSet` indexes current committed-plus-reserved VT per nonempty
  priority lane. Selection and the initial precharge share one scheduler
  critical section. A separate committed index advances each priority's
  monotonic reference; the new queue does not use periodic legacy VT calibration.
- RG generations share live capacity and task-id history across their priority
  lanes. The internal scheduler task ID identifies one live task for lookup and
  cancellation; it is distinct from the reusable MLFQ history task ID.
- CPU ratio, per-RG base capacity, precharge, and time slice are startup
  settings. Global capacity follows configured workers times tasks per worker;
  online changes update the queue shared by existing handles.

Cancellation ownership lives in `tikv_util::yatp_pool::fair_queue`. The queue
registry owns Records, permits weakly reference Records, and a Record weakly
references the task's `FutureSlot`. A slot owns a boxed pinned future behind a
per-task mutex. Cancel uses `try_lock`, never waits for a running poll, and
drops extracted futures outside locks to break IO-waker retention cycles.
Do not replace this with a strong Record-to-future ownership cycle or run
future destructors under the scheduler lock.

Implementation and validation details are in
[`resource-control-read-pool.md`](../../design/resource-control-read-pool.md).

## Critical Invariants

- Group configuration must converge safely when PD watch streams restart or the
  watch revision is compacted.
- Accounting paths must remain cheap because they can sit on hot request paths.
- Admission control and fair scheduling must degrade specific traffic classes,
  not accidentally all traffic.
- Dynamic config updates must alter runtime behavior, not only the stored config
  value.
- Background-group reporting must not silently double-count or regress versioned
  limiter statistics.

## Observability And Operational Signals

- limiter and scheduling metrics in `metrics.rs`
- logs on PD watch/reload failures, compaction restarts, and config loads
- RU reporting cadence and background-group behavior
- The process-level `tikv_yatp_fair_pool_*` metrics in
  `tikv_util/src/yatp_pool/metrics.rs` report actual poll CPU/wall time, poll
  duration, CPU-gate Pending attempts, global/group capacity rejections, and
  eviction. They have no RG labels and do not report precharge as consumed CPU.
- `READ_POOL_CPU_VEC` continues publishing current CPU and the configured
  target for RC pools even though their worker count is fixed. Updating these
  measurements does not reintroduce a CPU feedback controller.

Start triage with:

- `metrics.rs`
- `service.rs` watch/reload logs
- `worker.rs` quota-adjustment logic

## Change Management Guidance

- If policy knobs, admission behavior, or PD metadata contracts change, update
  this guide in the same patch.
- If callers in storage/server/batch-system start depending on new semantics,
  update both ends of the contract.
- Fairness or admission changes should come with before/after reasoning about
  who gets delayed, who gets rejected, and under which pressure signal.

## Change-Impact Matrix

- PD watch or config reload changes:
  inspect `service.rs`, PD client interactions, and caller assumptions about
  convergence
- Read fairness or metadata-lifecycle changes:
  inspect `resource_group.rs`, `tikv_util::yatp_pool::fair_queue`, and
  `src/read_pool.rs`; test ordinary updates, reload, delete/recreate, fallback,
  and in-flight settlement separately
- Write baseline changes:
  inspect `resource_group.rs`, `resource_limiter.rs`, and storage consumers
- Request admission or delay/reject changes:
  inspect `future.rs`, `channel.rs`, `src/server/service/kv.rs`, and
  `src/storage`
- Background RU reporting changes:
  inspect `worker.rs`, limiter statistics, and PD-facing reporting contracts
- Config knob changes:
  inspect `config.rs`, runtime update sites, metrics, and reviewer-facing docs
- Pressure-scoring changes (`score.rs`):
  inspect both consumers — background quota adjustment in `worker.rs` and
  foreground write throttling in `resource_group.rs` — since they share
  the same `cpu_score`/`io_score`/`compaction_score` computation
- Read provider or admission changes:
  inspect `resource_group.rs` and `src/read_pool.rs`; update this guide's
  "Cross-Component Contract" section and the design document together

## Review Checklist

- Does the change affect `watch_resource_groups`, reload, or retry behavior?
- Does it preserve read policy identity on update/reload and retire it on
  deletion? Does it change priority ordering or write baseline logic?
- Does it change request rejection versus delay behavior?
- Does it change accounting units or resource-cost interpretation?
- Does it create lock contention in the hot path?
- Does it update metrics and tests for new scheduling outcomes?
- Does it touch `score.rs`? If so, does it affect both the background
  (`worker.rs`) and foreground write (`resource_group.rs`) consumers as
  intended?
- Does read admission still bypass foreground historical debt without changing
  write/background delay behavior?
- Does it preserve the distinction between the new read policy weight and the
  legacy inverse-quota factor used by other pool controllers?

## Observability And Tests

- Metrics live under `metrics.rs` and within the group/limiter code.
- Many unit tests are inline in:
  `future.rs`, `service.rs`, `worker.rs`, `channel.rs`, `resource_limiter.rs`,
  and `score.rs`.
- Changes should usually be validated together with the call sites in
  `src/server`, `src/storage`, `components/batch-system`, and (for
  read-provider / admission changes) `src/read_pool.rs`.

## Common Failure Modes

- watcher stalls after compaction or transient PD errors
- starvation due to incorrect priority or baseline math
- too-aggressive shedding that impacts non-target traffic
- silently stale background-limiter reporting
- dynamic-config changes that fail to take effect operationally
- transient `/proc` read failures in `ThreadGroupCpuTracker` corrupting the
  next tick's CPU delta baseline (see `score.rs`)
- replacing a read policy handle during a PD reload and accidentally granting
  fresh queue progress
- clearing shared foreground limiter debt when disabling read admission,
  thereby changing write admission

## Reading Map And Companion Docs

Suggested reading order:

1. `lib.rs`
2. `config.rs`
3. `resource_group.rs`
4. `resource_limiter.rs`
5. `service.rs`
6. `worker.rs`
7. `score.rs`
8. `future.rs`

Companion docs:

- `repo-overview.md`
- `src/server.md`
- `src/storage.md`
- `src/read_pool.rs` — no dedicated guide yet; see "Cross-Component Contract:
  Unified Read Pool" above for the API surface it depends on here

## Glossary

- RU:
  request unit used for resource accounting
- Baseline:
  shared historical usage reference for foreground write admission
- Admission control:
  delay or reject logic under pressure
- Virtual time:
  scheduling progress notion used to compare group fairness state
- Resource score (`cpu_score`/`io_score`/`compaction_score`):
  0-100 utilization percentages from `score::compute_resource_scores`, shared
  by background quota adjustment and foreground write throttling
- Pressure fraction:
  a score mapped to `[0, 1]` via `score::pressure_fraction` against a
  caller-supplied `(start, end)` threshold range for background throttling
- Read group generation:
  a stable scheduler identity spanning ordinary policy updates; deletion
  retires it and a later create obtains a new identity

## Related Components

- `src/server/service/kv.rs` consumes resource-group context at the RPC edge.
- `src/storage` uses resource-control metadata and limiters during scheduling.
- `src/read_pool.rs` consumes stable read policies and background admission
  helpers from `ResourceGroupManager`; its queue owns read fairness and CPU
  admission.
- `components/batch-system` integrates with priority-aware execution.
