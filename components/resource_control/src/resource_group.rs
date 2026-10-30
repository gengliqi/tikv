// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    cmp::{max, min},
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use collections::HashMap;
use dashmap::{DashMap, mapref::one::Ref};
use fail::fail_point;
use kvproto::{
    kvrpcpb::{CommandPri, ResourceControlContext},
    resource_manager::{GroupMode, ResourceGroup as PbResourceGroup},
};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use tikv_util::{
    config::VersionTrack,
    info,
    resource_control::{DEFAULT_RESOURCE_GROUP_NAME, TaskMetadata, TaskPriority},
    time::Instant,
    yatp_pool::fair_queue::{GroupPolicy, GroupProvider},
};
use yatp::queue::priority::TaskPriorityProvider;

use crate::{
    config::Config,
    metrics,
    metrics::deregister_metrics,
    resource_limiter::{ResourceLimiter, ResourceType},
};

// a read task cost at least 50us.
const DEFAULT_PRIORITY_PER_READ_TASK: u64 = 50;
// extra task schedule factor
const TASK_EXTRA_FACTOR_BY_LEVEL: [u64; 3] = [0, 20, 100];
/// duration to update the minimal priority value of each resource group.
pub const MIN_PRIORITY_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
/// default value of max RU quota.
const DEFAULT_MAX_RU_QUOTA: u64 = 10_000;
/// The maximum RU quota that can be configured.
const MAX_RU_QUOTA: u64 = i32::MAX as u64;

#[cfg(test)]
const LOW_PRIORITY: u32 = 1;
const MEDIUM_PRIORITY: u32 = 8;
#[cfg(test)]
const HIGH_PRIORITY: u32 = 16;

// the global maximum of virtual time is u64::MAX / 16, so when the virtual
// time of all groups are bigger than half of this value, we rest them to avoid
// virtual time overflow.
const RESET_VT_THRESHOLD: u64 = (u64::MAX >> 4) / 2;

/// Fraction of `fg_cpu_throttle_threshold` below which foreground CPU
/// pressure counts as cleared — a dead zone so tighten/release don't flap
/// when `cpu_score` hovers near the threshold.
///
/// Must stay strictly less than [`THROTTLE_DECREASE_FACTOR`]: the tighten
/// step below shrinks whatever's currently enforced by
/// `1 - THROTTLE_DECREASE_FACTOR` each tick, so the leeway gap
/// (`1 - LEEWAY_THRESHOLD_RATIO`) must stay larger than that per-tick step —
/// otherwise a single tick could swing straight through the dead zone and
/// the engaged/disengaged state — and the throttle it drives — would just
/// toggle every tick instead of settling.
const LEEWAY_THRESHOLD_RATIO: f64 = 0.85;

/// Per-tick multiplicative step used to tighten the per-group CPU rate limit.
/// See [`LEEWAY_THRESHOLD_RATIO`] for the invariant relative to the recovery
/// threshold.
const THROTTLE_DECREASE_FACTOR: f64 = 0.9;

/// Duration of each bucket in the RuTracker ring buffer.
const RU_BUCKET_SECS: u64 = 30;

/// Sliding-window consumption tracker for foreground write admission control.
///
/// Tracks synchronous poll duration in µs across reads and writes, as recorded
/// by `LimitedFuture`, in a configurable ring buffer of 30-second buckets.
/// Window size is set from `historical_usage_window_mins` (× 2 buckets per
/// minute). This shared history does not affect the unified read scheduler.
pub struct RuTracker {
    /// Ring buffer of completed 30-second bucket totals (oldest at `head`).
    buckets: Vec<u64>,
    /// RU accumulated in the currently-open (incomplete) bucket.
    /// Atomic so that `record_ru_consumption` can add without taking the Mutex.
    current_bucket: AtomicU64,
    /// Unix seconds at which the current bucket started.
    bucket_start_secs: u64,
    /// Index of the oldest completed bucket.
    head: usize,
    /// Number of completed buckets (≤ buckets.len()).
    completed: usize,
    /// Cached historical rate (RU/s), updated by `online_adjust_resource_quota`
    /// every ~10s.
    cached_historical_rate: f64,
    /// Consecutive ramp-up epochs where new_limit >= 2x hist. Must reach
    /// MIN_RAMP_UP_EPOCHS before the limit is fully lifted to INFINITY.
    ramp_up_epochs: u32,
}

impl RuTracker {
    /// Create a new tracker with `num_buckets` 30-second slots.
    pub fn new(now_secs: u64, num_buckets: usize) -> Self {
        let num_buckets = num_buckets.max(2); // need at least 2 to warm up
        Self {
            buckets: vec![0u64; num_buckets],
            current_bucket: AtomicU64::new(0),
            bucket_start_secs: now_secs,
            head: 0,
            completed: 0,
            cached_historical_rate: 0.0,
            ramp_up_epochs: 0,
        }
    }

    pub fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    #[inline]
    fn num_buckets(&self) -> usize {
        self.buckets.len()
    }

    /// Record `ru` in the current bucket (lock-free atomic add).
    pub fn record(&self, ru: u64) {
        self.current_bucket.fetch_add(ru, Ordering::Relaxed);
    }

    /// Advance to `now_secs` and record `ru`. Used by tests and callers
    /// that hold exclusive access.
    #[cfg(test)]
    pub fn is_warmed_up(&self) -> bool {
        self.completed >= 2
    }

    #[cfg(test)]
    pub fn record_at(&mut self, ru: u64, now_secs: u64) {
        self.advance(now_secs);
        self.current_bucket.fetch_add(ru, Ordering::Relaxed);
    }

    fn advance(&mut self, now_secs: u64) {
        let n = self.num_buckets();
        let elapsed = now_secs.saturating_sub(self.bucket_start_secs);
        let buckets_to_advance = (elapsed / RU_BUCKET_SECS) as usize;
        if buckets_to_advance == 0 {
            return;
        }
        if buckets_to_advance >= n {
            // Gap larger than the window: everything aged out.
            self.buckets.iter_mut().for_each(|b| *b = 0);
            self.head = 0;
            self.completed = 0;
            self.current_bucket.store(0, Ordering::Relaxed);
        } else {
            // Commit the current bucket to the ring, then zero the rest.
            let write_pos = (self.head + self.completed) % n;
            self.buckets[write_pos] = self.current_bucket.swap(0, Ordering::Relaxed);
            if self.completed < n {
                self.completed += 1;
            } else {
                self.head = (self.head + 1) % n;
            }
            // Zero out the remaining (buckets_to_advance - 1) slots.
            for _ in 1..buckets_to_advance {
                let slot = (self.head + self.completed) % n;
                self.buckets[slot] = 0;
                if self.completed < n {
                    self.completed += 1;
                } else {
                    self.head = (self.head + 1) % n;
                }
            }
        }
        self.bucket_start_secs += buckets_to_advance as u64 * RU_BUCKET_SECS;
    }

    /// RU/s rate estimated from the current partial bucket and the most recent
    /// completed bucket. Using both avoids stale readings — the last completed
    /// bucket alone can be up to 60s old at the end of the current 30s window.
    ///
    /// rate = (current_bucket + last_completed) / (elapsed_in_current + 30s)
    pub fn current_rate(&self, now_secs: u64) -> f64 {
        let elapsed = now_secs
            .saturating_sub(self.bucket_start_secs)
            .min(RU_BUCKET_SECS) as f64;
        let last_completed = if self.completed > 0 {
            let newest_slot = (self.head + self.completed - 1) % self.num_buckets();
            self.buckets[newest_slot]
        } else {
            0
        };
        let window = elapsed + RU_BUCKET_SECS as f64;
        (self.current_bucket.load(Ordering::Relaxed) + last_completed) as f64 / window
    }

    /// Average RU/s rate over all completed buckets (long-term baseline).
    /// Average RU/s baseline. If the system has been up longer than the full
    /// historical window, always divide by the full window (not just completed
    /// buckets). This means a tracker that just started (e.g. spike with no
    /// prior traffic) has historical=0 and any traffic is immediately detected
    /// as over baseline, rather than letting new traffic inflate its own
    /// baseline.
    pub fn historical_rate(&self, system_start_secs: u64, now_secs: u64) -> f64 {
        let window_secs = self.num_buckets() as u64 * RU_BUCKET_SECS;
        let system_uptime = now_secs.saturating_sub(system_start_secs);
        if system_uptime >= window_secs {
            // System older than window — use full window as denominator.
            // Missing buckets count as zero, diluting any fresh traffic.
            if self.completed == 0 {
                return 0.0;
            }
            let total: u64 = self.buckets.iter().take(self.completed).sum();
            total as f64 / (self.num_buckets() as f64 * RU_BUCKET_SECS as f64)
        } else {
            if self.completed == 0 {
                return 0.0;
            }
            let total: u64 = self.buckets.iter().take(self.completed).sum();
            // Divide by elapsed system uptime (not just filled buckets) so the
            // historical rate ramps up smoothly rather than jumping immediately
            // to the full rate after the first bucket completes.
            let denom = (system_uptime as f64).max(RU_BUCKET_SECS as f64);
            total as f64 / denom
        }
    }

    /// Returns true if no RU has been recorded: all completed buckets and the
    /// current bucket are zero. Used to garbage-collect stale ru_trackers
    /// entries.
    pub fn is_idle(&self) -> bool {
        self.current_bucket.load(Ordering::Relaxed) == 0 && self.buckets.iter().all(|&b| b == 0)
    }

    /// Refresh the cached historical rate. Called periodically from
    /// `online_adjust_resource_quota` (~every 10s) so the inline hot path
    /// avoids iterating all buckets.
    pub fn refresh_cached_historical_rate(&mut self, system_start_secs: u64, now_secs: u64) {
        self.cached_historical_rate = self.historical_rate(system_start_secs, now_secs);
    }
}

pub enum ResourceConsumeType {
    CpuTime(Duration),
    IoBytes(u64),
}

/// Result of the Tier-1 admission control check for a single request.
#[derive(Debug, PartialEq)]
pub enum AdmissionDecision {
    /// No throttling needed; let the request proceed immediately.
    Allow,
    /// Delay the request by the given duration. The caller **must** call
    /// [`ResourceGroupManager::release_delay_slot`] once the delay is over
    /// (or the delayed future is dropped/cancelled).
    Delay(Duration),
    /// Reject the request outright (SchedTooBusy). Returned when the number
    /// of concurrently delayed requests exceeds `admission_max_delayed_count`.
    Reject,
}

/// RAII guard that releases an admission-control delay slot on drop.
/// Ensures the `delayed_req_count` counter is decremented even if the
/// future is cancelled during the sleep.
pub struct DelaySlotGuard {
    mgr: Option<Arc<ResourceGroupManager>>,
}

impl DelaySlotGuard {
    fn new(mgr: Arc<ResourceGroupManager>) -> Self {
        Self { mgr: Some(mgr) }
    }

    /// Explicitly release the slot and disarm the guard so Drop is a no-op.
    pub fn release(&mut self) {
        if let Some(mgr) = self.mgr.take() {
            mgr.release_delay_slot();
        }
    }
}

impl Drop for DelaySlotGuard {
    fn drop(&mut self) {
        self.release();
    }
}

struct ReadGroupPolicy {
    policy: Arc<GroupPolicy>,
    quota: u64,
}

/// Stable read-scheduler identities, independent of the legacy VT controllers.
/// Metadata mutations hold this lock through publication in `resource_groups`;
/// the queue only clones policy handles and never calls back while holding it.
#[derive(Default)]
struct ReadGroupPolicies {
    groups: HashMap<String, ReadGroupPolicy>,
    next_id: u64,
}

impl ReadGroupPolicies {
    fn is_special(name: &str, quota: u64) -> bool {
        quota == 0 || (name == DEFAULT_RESOURCE_GROUP_NAME && quota == MAX_RU_QUOTA)
    }

    fn upsert(&mut self, name: String, quota: u64, priority: u32) {
        let quota = quota.min(MAX_RU_QUOTA);
        let priority = if priority == 0 {
            MEDIUM_PRIORITY
        } else {
            priority.clamp(1, 16)
        };
        let weight = if Self::is_special(&name, quota) {
            self.groups
                .iter()
                .filter(|(other_name, group)| {
                    *other_name != &name && !Self::is_special(other_name, group.quota)
                })
                .map(|(_, group)| group.quota)
                .max()
                .unwrap_or(0)
                .max(DEFAULT_MAX_RU_QUOTA)
                .saturating_mul(10)
                .min(MAX_RU_QUOTA)
        } else {
            quota
        };
        if let Some(group) = self.groups.get_mut(&name) {
            group.quota = quota;
            group.policy.update(priority, weight);
        } else {
            self.next_id = self
                .next_id
                .checked_add(1)
                .expect("resource group generation overflow");
            self.groups.insert(
                name.clone(),
                ReadGroupPolicy {
                    policy: Arc::new(GroupPolicy::new(self.next_id, name, priority, weight)),
                    quota,
                },
            );
        }
        self.refresh_special_weights();
    }

    fn remove(&mut self, name: &str) {
        if let Some(group) = self.groups.remove(name) {
            group.policy.retire();
        }
    }

    fn refresh_special_weights(&self) {
        let max_quota = self
            .groups
            .iter()
            .filter(|(name, group)| !Self::is_special(name, group.quota))
            .map(|(_, group)| group.quota)
            .max()
            .unwrap_or(0)
            .max(DEFAULT_MAX_RU_QUOTA);
        let weight = max_quota.saturating_mul(10).min(MAX_RU_QUOTA);
        for (name, group) in &self.groups {
            if Self::is_special(name, group.quota) {
                group.policy.update(group.policy.priority(), weight);
            }
        }
    }
}

/// ResourceGroupManager manages the metadata of each resource group.
pub struct ResourceGroupManager {
    pub(crate) resource_groups: DashMap<String, ResourceGroup>,
    read_group_policies: RwLock<ReadGroupPolicies>,
    // the count of all groups, a fast path because call `DashMap::len` is a little slower.
    group_count: AtomicU64,
    registry: RwLock<Vec<Arc<ResourceController>>>,
    // the shared resource limiter of each priority
    priority_limiters: [Arc<ResourceLimiter>; TaskPriority::PRIORITY_COUNT],
    bg_limiter: Arc<ResourceLimiter>,
    // cached: true when at least one group has background settings configured.
    has_background: AtomicBool,
    // lastest config.
    config: Arc<VersionTrack<Config>>,
    // Shared read/write usage history and the foreground write-admission
    // limiter. The unified read pool records usage but does not wait on this
    // limiter's historical CPU debt.
    // The Arc allows handing out limiter references to LimitedFuture wrappers
    // in the read/write pools without copying the limiter state.
    ru_trackers: DashMap<String, Mutex<(RuTracker, Arc<ResourceLimiter>)>>,
    // Number of requests currently held in the admission-control delay phase.
    delayed_req_count: AtomicI64,
    // Unix seconds when this manager was created. Used to determine whether
    // the system has been up long enough that new trackers should be treated
    // as if they missed the entire historical window (baseline = 0).
    start_secs: u64,
    // True when background CPU budget is at the minimum floor (1 core) AND
    // background consumption is within that floor. Foreground throttling
    // only engages when this flag is set, ensuring background is fully
    // squeezed before foreground traffic is touched.
    bg_cpu_at_floor: AtomicBool,
}

impl Default for ResourceGroupManager {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

impl ResourceGroupManager {
    pub fn new(config: Config) -> Self {
        let priority_limiters = TaskPriority::priorities().map(|p| {
            Arc::new(ResourceLimiter::new(
                p.as_str().to_owned(),
                f64::INFINITY,
                f64::INFINITY,
                0,
                false,
            ))
        });
        let bg_limiter = Arc::new(ResourceLimiter::new(
            DEFAULT_RESOURCE_GROUP_NAME.to_owned(),
            f64::INFINITY,
            f64::INFINITY,
            0,
            true,
        ));
        let start_secs = RuTracker::now_secs();
        let manager = Self {
            resource_groups: Default::default(),
            read_group_policies: RwLock::new(ReadGroupPolicies::default()),
            group_count: AtomicU64::new(0),
            registry: Default::default(),
            delayed_req_count: AtomicI64::new(0),
            priority_limiters,
            bg_limiter,
            has_background: AtomicBool::new(false),
            config: Arc::new(VersionTrack::new(config)),
            ru_trackers: Default::default(),
            start_secs,
            bg_cpu_at_floor: AtomicBool::new(false),
        };

        // init the default resource group by default.
        let mut default_group = PbResourceGroup::new();
        default_group.name = DEFAULT_RESOURCE_GROUP_NAME.into();
        default_group.priority = MEDIUM_PRIORITY;
        default_group.mode = GroupMode::RuMode;
        default_group
            .mut_r_u_settings()
            .mut_r_u()
            .mut_settings()
            .fill_rate = MAX_RU_QUOTA;
        manager.add_resource_group(default_group);

        manager
    }

    #[inline]
    pub fn get_group_count(&self) -> u64 {
        self.group_count.load(Ordering::Relaxed)
    }

    fn get_ru_setting(rg: &PbResourceGroup, is_read: bool) -> u64 {
        match (rg.get_mode(), is_read) {
            // RU mode, read and write use the same setting.
            (GroupMode::RuMode, _) => rg
                .get_r_u_settings()
                .get_r_u()
                .get_settings()
                .get_fill_rate(),
            // TODO: currently we only consider the cpu usage in the read path, we may also take
            // io read bytes into account later.
            (GroupMode::RawMode, true) => rg
                .get_raw_resource_settings()
                .get_cpu()
                .get_settings()
                .get_fill_rate(),
            (GroupMode::RawMode, false) => rg
                .get_raw_resource_settings()
                .get_io_write()
                .get_settings()
                .get_fill_rate(),
            // return a default value for unsupported config.
            (GroupMode::Unknown, _) => 1,
        }
    }

    pub fn add_resource_group(&self, rg: PbResourceGroup) {
        let group_name = rg.get_name().to_ascii_lowercase();
        let mut read_policies = self.read_group_policies.write();
        read_policies.upsert(
            group_name.clone(),
            Self::get_ru_setting(&rg, true),
            rg.priority,
        );
        self.registry.read().iter().for_each(|controller| {
            let ru_quota = Self::get_ru_setting(&rg, controller.is_read);
            controller.add_resource_group(group_name.clone().into_bytes(), ru_quota, rg.priority);
        });
        info!("add resource group"; "name"=> &rg.name, "ru" => rg.get_r_u_settings().get_r_u().get_settings().get_fill_rate());
        let limiter = self.build_resource_limiter(&rg);

        if self
            .resource_groups
            .insert(group_name, ResourceGroup::new(rg, limiter))
            .is_none()
        {
            self.group_count.fetch_add(1, Ordering::Relaxed);
        }
        self.update_has_background();
    }

    fn update_has_background(&self) {
        let any_has_bg = self
            .resource_groups
            .iter()
            .any(|g| !g.background_source_types.is_empty());
        let prev = self.has_background.swap(any_has_bg, Ordering::Release);
        // When the last background group is removed, reset the shared limiter to
        // unlimited so that a later re-add does not inherit stale throttled rates.
        if prev && !any_has_bg {
            use crate::resource_limiter::ResourceType;
            self.bg_limiter
                .get_limiter(ResourceType::Cpu)
                .set_rate_limit(f64::INFINITY);
            self.bg_limiter
                .get_limiter(ResourceType::Io)
                .set_rate_limit(f64::INFINITY);
            self.bg_limiter
                .get_write_io_limiter()
                .set_rate_limit(f64::INFINITY);
        }
    }

    fn build_resource_limiter(&self, rg: &PbResourceGroup) -> Option<Arc<ResourceLimiter>> {
        if !rg.get_background_settings().get_job_types().is_empty() {
            Some(self.bg_limiter.clone())
        } else {
            None
        }
    }

    pub fn remove_resource_group(&self, name: &str) {
        let group_name = name.to_ascii_lowercase();
        // The fallback identity must remain available even if PD deletes its
        // customized definition. Reset its policy without retiring the handle.
        if group_name == DEFAULT_RESOURCE_GROUP_NAME {
            let mut group = PbResourceGroup::new();
            group.name = group_name;
            group.priority = MEDIUM_PRIORITY;
            group.mode = GroupMode::RuMode;
            // Zero is the legacy controller's reset sentinel and maps to the
            // same finite special weight in the read policy registry.
            group.mut_r_u_settings().mut_r_u().mut_settings().fill_rate = 0;
            self.add_resource_group(group);
            return;
        }
        let mut read_policies = self.read_group_policies.write();
        read_policies.remove(&group_name);
        read_policies.refresh_special_weights();
        self.registry.read().iter().for_each(|controller| {
            controller.remove_resource_group(group_name.as_bytes());
        });
        if self.resource_groups.remove(&group_name).is_some() {
            info!("remove resource group"; "name"=> name);
            self.group_count.fetch_sub(1, Ordering::Relaxed);
            deregister_metrics(&group_name);
        }
        self.update_has_background();
    }

    pub fn retain(&self, mut f: impl FnMut(&String, &PbResourceGroup) -> bool) {
        let mut read_policies = self.read_group_policies.write();
        let mut removed_names = vec![];
        self.resource_groups.retain(|k, v| {
            // avoid remove default group.
            if k == DEFAULT_RESOURCE_GROUP_NAME {
                return true;
            }
            let ret = f(k, &v.group);
            if !ret {
                removed_names.push(k.clone());
            }
            ret
        });
        if !removed_names.is_empty() {
            for name in &removed_names {
                read_policies.remove(name);
                deregister_metrics(name);
            }
            read_policies.refresh_special_weights();
            self.registry.read().iter().for_each(|controller| {
                for name in &removed_names {
                    controller.remove_resource_group(name.as_bytes());
                }
            });
            self.group_count
                .fetch_sub(removed_names.len() as u64, Ordering::Relaxed);
            self.update_has_background();
        }
    }

    pub(crate) fn get_resource_group(&self, name: &str) -> Option<Ref<'_, String, ResourceGroup>> {
        self.resource_groups.get(&name.to_ascii_lowercase())
    }

    pub fn get_config(&self) -> &Arc<VersionTrack<Config>> {
        &self.config
    }

    pub fn get_all_resource_groups(&self) -> Vec<PbResourceGroup> {
        self.resource_groups
            .iter()
            .map(|g| g.group.clone())
            .collect()
    }

    pub fn derive_controller(&self, name: String, is_read: bool) -> Arc<ResourceController> {
        let controller = Arc::new(ResourceController::new(name, is_read));
        self.registry.write().push(controller.clone());
        for g in &self.resource_groups {
            let ru_quota = Self::get_ru_setting(&g.value().group, controller.is_read);
            controller.add_resource_group(g.key().clone().into_bytes(), ru_quota, g.group.priority);
        }
        controller
    }

    pub fn advance_min_virtual_time(&self) {
        for controller in self.registry.read().iter() {
            controller.update_min_virtual_time();
        }
    }

    pub fn consume_penalty(&self, ctx: &ResourceControlContext) {
        for controller in self.registry.read().iter() {
            // FIXME: Should consume CPU time for read controller and write bytes for write
            // controller, once CPU process time of scheduler worker is tracked. Currently,
            // we consume write bytes for read controller as the
            // order of magnitude of CPU time and write bytes is similar.
            controller.consume(
                ctx.resource_group_name.as_bytes(),
                ResourceConsumeType::CpuTime(Duration::from_nanos(
                    (ctx.get_penalty().total_cpu_time_ms * 1_000_000.0) as u64,
                )),
            );
            controller.consume(
                ctx.resource_group_name.as_bytes(),
                ResourceConsumeType::IoBytes(ctx.get_penalty().write_bytes as u64),
            );
        }
        // RU tracking for foreground admission control is handled by
        // LimitedFuture (measure-only mode) which calls record_ru_consumption
        // with actual CPU measured per poll.
    }

    /// Record `ru` units consumed by `group` into the sliding-window tracker
    /// and consume tokens from the group's rate limiter.
    pub fn record_ru_consumption(&self, group: &str, ru: u64) {
        let now = RuTracker::now_secs();
        // 2 buckets per minute (30s each) to match RU_BUCKET_SECS.
        let num_buckets = (self.config.value().historical_usage_window_mins.max(2) as usize) * 2;
        let entry = self.ru_trackers.entry(group.to_owned()).or_insert_with(|| {
            Mutex::new((
                RuTracker::new(now, num_buckets),
                Arc::new(ResourceLimiter::new(
                    group.to_owned(),
                    f64::INFINITY,
                    f64::INFINITY,
                    0,
                    false,
                )),
            ))
        });
        // Lock-free: only atomically adds to current_bucket.
        // advance() is called separately by online_adjust_resource_quota under the
        // lock.
        entry.lock().unwrap().0.record(ru);
    }

    /// Called by `PriorityLimiterAdjustWorker` every ~1 second with the
    /// latest process-level CPU utilisation percentage.
    ///
    /// Throttle-down: linearly reduces the allowed fraction of historical rate
    /// for groups that are over their baseline. At `fg_cpu_throttle_threshold`
    /// (70%) fraction = 1.0 (no throttle), at 90% CPU fraction = 0.8
    /// (target). Called by the background adjust worker after computing the
    /// new background CPU budget. `at_floor` should be true when the budget
    /// has been clamped to the minimum floor AND background consumption
    /// is within that floor.
    pub fn set_bg_cpu_at_floor(&self, at_floor: bool) {
        self.bg_cpu_at_floor.store(at_floor, Ordering::Relaxed);
    }

    /// Returns true when background CPU is fully throttled (at floor)
    /// and its consumption is within the floor budget.
    pub fn is_bg_cpu_at_floor(&self) -> bool {
        self.bg_cpu_at_floor.load(Ordering::Relaxed)
    }

    /// `cpu_score` is the common CPU-utilization score (0-100) computed by
    /// [`crate::score::compute_resource_scores`]: the max of process and grpc
    /// normalized utilization.
    pub fn online_adjust_resource_quota(&self, cpu_score: f64) {
        let now = RuTracker::now_secs();
        self.adjust_group_throttling(cpu_score, now);
    }

    /// Per-group CPU rate-limit throttling. Only groups whose current rate
    /// exceeds their historical rate are limited.
    ///
    /// Ramp-up: when CPU drops below threshold, recover ×1.1/tick until
    /// NO_LIMIT is restored.
    fn adjust_group_throttling(&self, cpu_score: f64, now: u64) {
        const RAMP_FACTOR: f64 = 1.1;
        const MIN_RAMP_UP_EPOCHS: u32 = 2;

        let throttle_threshold = self.config.value().fg_cpu_throttle_threshold;
        let leeway_threshold = throttle_threshold * LEEWAY_THRESHOLD_RATIO;
        let burst_factor = 1.0 + self.config.value().baseline_burst_pct / 100.0;

        // Advance all trackers to `now` and refresh cached historical rates
        // up front so the throttling logic below sees fresh baseline data.
        for entry in &self.ru_trackers {
            let mut guard = entry.lock().unwrap();
            guard.0.advance(now);
            guard.0.refresh_cached_historical_rate(self.start_secs, now);
            let name = guard.1.name();

            metrics::GROUP_RU_HISTORICAL_RATE
                .with_label_values(&[name])
                .set((guard.0.cached_historical_rate / 1_000_000.0) * 100.0);
            metrics::GROUP_RU_CURRENT_RATE
                .with_label_values(&[name])
                .set((guard.0.current_rate(now) / 1_000_000.0) * 100.0);
        }

        let engaged = cpu_score > throttle_threshold && self.is_bg_cpu_at_floor();
        if engaged {
            for entry in &self.ru_trackers {
                let mut guard = entry.lock().unwrap();
                let hist = guard.0.cached_historical_rate;
                let burst_target = hist * burst_factor;
                if hist > 0.0 {
                    let current_limit = guard.1.get_limiter(ResourceType::Cpu).get_rate_limit();
                    let base = if current_limit.is_infinite() {
                        guard.0.current_rate(now)
                    } else {
                        current_limit
                    };
                    if base > burst_target {
                        let rate = (base * THROTTLE_DECREASE_FACTOR).max(burst_target);
                        guard.0.ramp_up_epochs = 0;
                        guard
                            .1
                            .get_limiter(ResourceType::Cpu)
                            .set_rate_limit(rate.max(1.0));
                    }
                }
            }
        } else if cpu_score < leeway_threshold {
            // CPU below start threshold — ramp up by RAMP_FACTOR each tick.
            // Only lift to INFINITY after MIN_RAMP_UP_EPOCHS consecutive epochs
            // where the limit has grown past 2x hist, to avoid premature release.
            for entry in &self.ru_trackers {
                let mut guard = entry.lock().unwrap();
                let current_limit = guard.1.get_limiter(ResourceType::Cpu).get_rate_limit();
                if current_limit.is_finite() {
                    let hist = guard.0.cached_historical_rate;
                    let new_limit = current_limit * RAMP_FACTOR;
                    if hist <= 0.0 || new_limit >= 2.0 * hist {
                        guard.0.ramp_up_epochs += 1;
                        if guard.0.ramp_up_epochs >= MIN_RAMP_UP_EPOCHS {
                            guard.0.ramp_up_epochs = 0;
                            guard
                                .1
                                .get_limiter(ResourceType::Cpu)
                                .set_rate_limit(f64::INFINITY);
                        }
                    } else {
                        guard.0.ramp_up_epochs = 0;
                        guard
                            .1
                            .get_limiter(ResourceType::Cpu)
                            .set_rate_limit(new_limit);
                    }
                }
            }
        }

        // Emit current rate limit per resource group.
        for entry in &self.ru_trackers {
            let guard = entry.lock().unwrap();
            let limit = guard.1.get_limiter(ResourceType::Cpu).get_rate_limit();
            let val = if limit.is_finite() {
                (limit / 1_000_000.0) * 100.0
            } else {
                0.0
            };
            metrics::GROUP_QUOTA_LIMIT_VEC
                .with_label_values(&[guard.1.name(), "cpu"])
                .set(val);
        }

        // Evict idle ru_trackers entries to prevent unbounded growth from
        // removed or renamed groups. Advance first so stale partial buckets
        // are flushed before the idle check.
        self.ru_trackers.retain(|_, entry| {
            let inner = entry.get_mut().unwrap();
            inner.0.advance(now);
            !inner.0.is_idle()
        });
    }

    /// Returns the token-bucket debt delay for `group`, or `None` if no
    /// throttling is active.
    ///
    /// Background requests consult their limiter directly. Foreground writes
    /// consult historical CPU debt only when `enable_write_admission_control`
    /// is enabled. Foreground reads always bypass this historical limiter.
    pub fn compute_admission_delay(
        &self,
        resource_limiter: &ResourceLimiter,
        is_read: bool,
    ) -> Option<Duration> {
        // Reads share accounting with writes, but their admission budget is
        // owned by the read queue. Do not reset the shared limiter: writes can
        // still be waiting on its debt. Background limiters remain independent
        // and apply to both reads and writes.
        if !resource_limiter.is_background()
            && (is_read || !self.config.value().enable_write_admission_control)
        {
            return None;
        }
        let delay = resource_limiter.admission_delay(is_read);
        if delay.is_zero() { None } else { Some(delay) }
    }

    /// Unified admission-control decision for a request from `group`.
    ///
    /// Combines token-bucket rate-limiter debt (`resource_limiter`) with
    /// RU-baseline overage delay into a single pre-pool sleep duration.
    /// Covers background, low-priority, and resource-group throttling for
    /// both reads (`is_read=true`) and writes (`is_read=false`).
    ///
    /// Returns:
    /// - [`AdmissionDecision::Allow`] — no throttling needed.
    /// - [`AdmissionDecision::Delay(d)`] — caller should sleep `d` then
    ///   proceed, and **must** call [`release_delay_slot`] afterwards.
    /// - [`AdmissionDecision::Reject`] — too many requests are already delayed;
    ///   reject immediately.
    pub fn admission_decision(
        &self,
        is_read: bool,
        resource_limiter: &ResourceLimiter,
    ) -> AdmissionDecision {
        let group = resource_limiter.name();
        let delay = self
            .compute_admission_delay(resource_limiter, is_read)
            .unwrap_or(std::time::Duration::ZERO);
        if delay.is_zero() {
            return AdmissionDecision::Allow;
        }
        let metric_label = if resource_limiter.is_background() {
            "background"
        } else {
            group
        };
        let max = self.config.value().admission_max_delayed_count;
        let prev = self.delayed_req_count.fetch_add(1, Ordering::Relaxed);
        metrics::ADMISSION_CURRENTLY_DELAYED.set(prev + 1);
        if max > 0 && prev >= max as i64 {
            self.delayed_req_count.fetch_sub(1, Ordering::Relaxed);
            metrics::ADMISSION_CURRENTLY_DELAYED.set(prev);
            crate::metrics::ADMISSION_REJECTED_REQUESTS
                .with_label_values(&[metric_label])
                .inc();
            return AdmissionDecision::Reject;
        }
        crate::metrics::ADMISSION_DELAYED_REQUESTS
            .with_label_values(&[metric_label])
            .inc();
        crate::metrics::ADMISSION_DELAY_DURATION
            .with_label_values(&[metric_label])
            .observe(delay.as_secs_f64());
        AdmissionDecision::Delay(delay)
    }

    /// Release a delay slot acquired by [`admission_decision`] returning
    /// [`AdmissionDecision::Delay`]. Must be called exactly once per `Delay`
    /// decision, whether the request completes normally or is cancelled.
    pub fn release_delay_slot(&self) {
        let prev = self.delayed_req_count.fetch_sub(1, Ordering::Relaxed);
        metrics::ADMISSION_CURRENTLY_DELAYED.set(prev - 1);
    }

    /// Returns an RAII guard that calls [`release_delay_slot`] on drop.
    /// Use this to ensure the slot is released even if the future is
    /// cancelled during the admission-control sleep.
    pub fn delay_slot_guard(self: &Arc<Self>) -> DelaySlotGuard {
        DelaySlotGuard::new(Arc::clone(self))
    }

    /// Returns the per-group admission-control limiter for `group`, creating
    /// the entry if it does not yet exist. Used by `with_resource_limiter`
    /// (measure-only mode) in the read/write pools so `LimitedFuture` can
    /// build token-bucket debt for pre-pool `admission_decision`.
    pub fn get_foreground_group_limiter(&self, group: &str) -> Arc<ResourceLimiter> {
        let now = RuTracker::now_secs();
        // 2 buckets per minute (30s each) to match RU_BUCKET_SECS.
        let num_buckets = (self.config.value().historical_usage_window_mins.max(2) as usize) * 2;
        self.ru_trackers
            .entry(group.to_owned())
            .or_insert_with(|| {
                Mutex::new((
                    RuTracker::new(now, num_buckets),
                    Arc::new(ResourceLimiter::new(
                        group.to_owned(),
                        f64::INFINITY,
                        f64::INFINITY,
                        0,
                        false,
                    )),
                ))
            })
            .lock()
            .unwrap()
            .1
            .clone()
    }

    /// Returns the appropriate `ResourceLimiter` for `with_resource_limiter`:
    /// - Background tasks → background limiter
    /// - Foreground tasks (any priority) → per-group limiter from `ru_trackers`
    pub fn get_resource_limiter(
        &self,
        rg: &str,
        request_source: &str,
        _override_priority: u64,
    ) -> Option<Arc<ResourceLimiter>> {
        let (limiter, _) = self.get_background_resource_limiter_with_priority(rg, request_source);
        if limiter.is_some() {
            return limiter;
        }
        // Only create a foreground limiter for known groups; unknown or removed
        // groups fall back to "default" to avoid leaking ru_trackers entries.
        let group_name = if self.resource_groups.contains_key(rg) {
            rg
        } else {
            DEFAULT_RESOURCE_GROUP_NAME
        };
        Some(self.get_foreground_group_limiter(group_name))
    }

    // return a ResourceLimiter for background tasks only.
    // Returns None if request_source does not match a configured background job
    // type.
    pub fn get_background_resource_limiter(
        &self,
        rg: &str,
        request_source: &str,
    ) -> Option<Arc<ResourceLimiter>> {
        if request_source.is_empty() {
            return None;
        }
        self.get_background_resource_limiter_with_priority(rg, request_source)
            .0
    }

    fn get_background_resource_limiter_with_priority(
        &self,
        rg: &str,
        request_source: &str,
    ) -> (Option<Arc<ResourceLimiter>>, u32) {
        fail_point!("only_check_source_task_name", |name| {
            assert_eq!(&name.unwrap(), request_source);
            (None, 8)
        });
        let mut group_priority = None;
        if let Some(group) = self.resource_groups.get(rg) {
            group_priority = Some(group.group.priority);
            if !group.fallback_default {
                return (
                    group.get_background_resource_limiter(request_source),
                    group.group.priority,
                );
            }
        }

        let default_group = self
            .resource_groups
            .get(DEFAULT_RESOURCE_GROUP_NAME)
            .unwrap();
        (
            default_group.get_background_resource_limiter(request_source),
            group_priority.unwrap_or(default_group.group.priority),
        )
    }

    #[inline]
    pub fn get_priority_resource_limiters(
        &self,
    ) -> &[Arc<ResourceLimiter>; TaskPriority::PRIORITY_COUNT] {
        &self.priority_limiters
    }

    pub fn get_background_limiter(&self) -> Arc<ResourceLimiter> {
        self.bg_limiter.clone()
    }

    pub fn has_background_groups(&self) -> bool {
        self.has_background.load(Ordering::Acquire)
    }
}

impl GroupProvider for ResourceGroupManager {
    fn group(&self, name: &str) -> Arc<GroupPolicy> {
        let groups = self.read_group_policies.read();
        groups
            .groups
            .get(name)
            .or_else(|| groups.groups.get(&name.to_ascii_lowercase()))
            .unwrap_or_else(|| {
                groups
                    .groups
                    .get(DEFAULT_RESOURCE_GROUP_NAME)
                    .expect("default read resource group must exist")
            })
            .policy
            .clone()
    }
}

pub(crate) struct ResourceGroup {
    pub group: PbResourceGroup,
    pub limiter: Option<Arc<ResourceLimiter>>,
    background_source_types: HashSet<String>,
    // whether to fallback background resource control to `default` group.
    fallback_default: bool,
}

impl ResourceGroup {
    fn new(group: PbResourceGroup, limiter: Option<Arc<ResourceLimiter>>) -> Self {
        let background_source_types =
            HashSet::from_iter(group.get_background_settings().get_job_types().to_owned());
        let fallback_default =
            !group.has_background_settings() && group.name != DEFAULT_RESOURCE_GROUP_NAME;
        Self {
            group,
            limiter,
            background_source_types,
            fallback_default,
        }
    }

    #[cfg(test)]
    pub fn get_ru_quota(&self) -> u64 {
        self.group
            .get_r_u_settings()
            .get_r_u()
            .get_settings()
            .get_fill_rate()
    }

    fn get_background_resource_limiter(
        &self,
        request_source: &str,
    ) -> Option<Arc<ResourceLimiter>> {
        self.limiter.as_ref().and_then(|limiter| {
            // the source task name is the last part of `request_source` separated by "_"
            // the request_source is
            // {extrenal|internal}_{tidb_req_source}_{source_task_name}
            let source_task_name = request_source.rsplit('_').next().unwrap_or("");
            if !source_task_name.is_empty()
                && self.background_source_types.contains(source_task_name)
            {
                Some(limiter.clone())
            } else {
                None
            }
        })
    }
}

pub struct ResourceController {
    // resource controller name is not used currently.
    #[allow(dead_code)]
    name: String,
    // We handle the priority differently between read and write request:
    // 1. the priority factor is calculated based on read/write RU settings.
    // 2. for read request, we increase a constant virtual time delta at each `get_priority` call
    //    because the cost can't be calculated at start, so we only increase a constant delta and
    //    increase the real cost after task is executed; but don't increase it at write because the
    //    cost is known so we just pre-consume it.
    is_read: bool,
    // Track the maximum ru quota used to calculate the factor of each resource group.
    // factor = max_ru_quota / group_ru_quota * 10.0
    // We use mutex here to ensure when we need to change this value and do adjust all resource
    // groups' factors, it can't be changed concurrently.
    // NOTE: because the ru config for "default" group is very large, and it can cause very big
    // group weight, we will not count this value by default.
    max_ru_quota: Mutex<u64>,
    // record consumption of each resource group, name --> resource_group
    resource_consumptions: RwLock<HashMap<Vec<u8>, GroupPriorityTracker>>,
    // the latest min vt, this value is used to init new added group vt
    last_min_vt: AtomicU64,
    // the last time min vt is overflow
    last_rest_vt_time: Cell<Instant>,
    // whether the settings are customized by user
    customized: AtomicBool,
}

// we are ensure to visit the `last_rest_vt_time` by only 1 thread so it's
// thread safe.
unsafe impl Send for ResourceController {}
unsafe impl Sync for ResourceController {}

impl ResourceController {
    fn new(name: String, is_read: bool) -> Self {
        Self {
            name,
            is_read,
            resource_consumptions: RwLock::new(HashMap::default()),
            last_min_vt: AtomicU64::new(0),
            max_ru_quota: Mutex::new(DEFAULT_MAX_RU_QUOTA),
            last_rest_vt_time: Cell::new(Instant::now_coarse()),
            customized: AtomicBool::new(false),
        }
    }

    pub fn new_for_test(name: String, is_read: bool) -> Self {
        let controller = Self::new(name, is_read);
        // add the "default" resource group.
        controller.add_resource_group(
            DEFAULT_RESOURCE_GROUP_NAME.as_bytes().to_owned(),
            0,
            MEDIUM_PRIORITY,
        );
        controller
    }

    fn calculate_factor(max_quota: u64, quota: u64) -> u64 {
        // we don't adjust the max_quota if it's the "default" group's default
        // value(u32::MAX), so here it is possible that the quota is bigger than
        // the max quota
        if quota == 0 || quota > max_quota {
            1
        } else {
            // we use max_quota / quota as the resource group factor, but because we need to
            // cast the value to integer, so we times it by 10 to ensure the accuracy is
            // enough.
            let max_quota = min(max_quota * 10, MAX_RU_QUOTA);
            (max_quota as f64 / quota as f64).round() as u64
        }
    }

    fn add_resource_group(&self, name: Vec<u8>, mut ru_quota: u64, mut group_priority: u32) {
        if group_priority == 0 {
            // map 0 to medium priority(default priority)
            group_priority = MEDIUM_PRIORITY;
        }
        if ru_quota > MAX_RU_QUOTA {
            ru_quota = MAX_RU_QUOTA;
        }

        let mut max_ru_quota = self.max_ru_quota.lock().unwrap();
        // skip to adjust max ru if it is the "default" group and the ru config eq
        // MAX_RU_QUOTA
        if ru_quota > *max_ru_quota
            && (name != DEFAULT_RESOURCE_GROUP_NAME.as_bytes() || ru_quota < MAX_RU_QUOTA)
        {
            *max_ru_quota = ru_quota;
            // adjust all group weight because the current value is too small.
            self.adjust_all_resource_group_factors(ru_quota);
        }
        let weight = Self::calculate_factor(*max_ru_quota, ru_quota);

        let vt_delta_for_get = if self.is_read {
            DEFAULT_PRIORITY_PER_READ_TASK * weight
        } else {
            0
        };
        let group = GroupPriorityTracker {
            ru_quota,
            group_priority,
            weight,
            virtual_time: AtomicU64::new(self.last_min_vt.load(Ordering::Acquire)),
            vt_delta_for_get,
        };

        // maybe update existed group
        self.resource_consumptions.write().insert(name, group);
        self.check_customized();
    }

    fn check_customized(&self) {
        let groups = self.resource_consumptions.read();
        if groups.len() == 1 && groups.get(DEFAULT_RESOURCE_GROUP_NAME.as_bytes()).is_some() {
            self.customized.store(false, Ordering::Release);
            return;
        }
        self.customized.store(true, Ordering::Release);
    }

    // we calculate the weight of each resource group based on the currently maximum
    // ru quota, if a incoming resource group has a bigger quota, we need to
    // adjust all the existing groups. As we expect this won't happen very
    // often, and iterate 10k entry cost less than 5ms, so the performance is
    // acceptable.
    fn adjust_all_resource_group_factors(&self, max_ru_quota: u64) {
        self.resource_consumptions
            .write()
            .iter_mut()
            .for_each(|(_, tracker)| {
                tracker.weight = Self::calculate_factor(max_ru_quota, tracker.ru_quota);
            });
    }

    fn remove_resource_group(&self, name: &[u8]) {
        // do not remove the default resource group, reset to default setting instead.
        if DEFAULT_RESOURCE_GROUP_NAME.as_bytes() == name {
            self.add_resource_group(
                DEFAULT_RESOURCE_GROUP_NAME.as_bytes().to_owned(),
                0,
                MEDIUM_PRIORITY,
            );
            self.check_customized();
            return;
        }
        self.resource_consumptions.write().remove(name);
        self.check_customized();
    }

    pub fn is_customized(&self) -> bool {
        self.customized.load(Ordering::Acquire)
    }

    #[inline]
    fn resource_group(&self, name: &[u8]) -> MappedRwLockReadGuard<'_, GroupPriorityTracker> {
        let guard = self.resource_consumptions.read();
        RwLockReadGuard::map(guard, |m| {
            if let Some(g) = m.get(name) {
                g
            } else {
                m.get(DEFAULT_RESOURCE_GROUP_NAME.as_bytes()).unwrap()
            }
        })
    }

    pub fn consume(&self, name: &[u8], resource: ResourceConsumeType) {
        self.resource_group(name).consume(resource)
    }

    pub fn update_min_virtual_time(&self) {
        let start = Instant::now_coarse();
        let mut min_vt = u64::MAX;
        let mut max_vt = 0;
        self.resource_consumptions
            .read()
            .iter()
            .for_each(|(_, tracker)| {
                let vt = tracker.current_vt();
                min_vt = min(min_vt, vt);
                max_vt = max(max_vt, vt);
            });

        // TODO: use different threshold for different resource type
        // needn't do update if the virtual different is less than 100ms/100KB.
        if min_vt >= max_vt.saturating_sub(100_000) && max_vt < RESET_VT_THRESHOLD {
            return;
        }

        fail_point!("increase_vt_duration_update_min_vt");

        let near_overflow = min_vt > RESET_VT_THRESHOLD;
        self.resource_consumptions
            .read()
            .iter()
            .for_each(|(_, tracker)| {
                let vt = tracker.current_vt();
                // NOTE: this decrease vt is not atomic across all resource groups,
                // but it should be ok as this operation should be extremely rare
                // and the impact is not big.
                if near_overflow {
                    tracker.decrease_vt(RESET_VT_THRESHOLD);
                } else if vt < max_vt {
                    // TODO: is increase by half is a good choice.
                    tracker.increase_vt((max_vt - vt) / 2);
                }
            });
        if near_overflow {
            let end = Instant::now_coarse();
            info!("all resource groups' virtual time are near overflow, do reset";
                "min" => min_vt, "max" => max_vt, "dur" => ?end.duration_since(start),
                "reset_dur" => ?end.duration_since(self.last_rest_vt_time.get()));
            max_vt -= RESET_VT_THRESHOLD;
            self.last_rest_vt_time.set(end);
        }
        // max_vt is actually a little bigger than the current min vt, but we don't
        // need totally accurate here.
        self.last_min_vt.store(max_vt, Ordering::Relaxed);
    }

    pub fn get_priority(&self, name: &[u8], pri: CommandPri) -> u64 {
        let level = Self::command_pri_to_level(pri);
        self.resource_group(name).get_priority(level, None, true)
    }

    /// Returns the priority for the given task metadata without incrementing
    /// virtual time. Used for pre-spawn eviction comparison.
    pub fn peek_priority_of(&self, metadata: &TaskMetadata<'_>, pri: CommandPri) -> u64 {
        let level = Self::command_pri_to_level(pri);
        let group = self.resource_group(metadata.group_name());
        let override_priority = if metadata.override_priority() == 0 {
            None
        } else {
            Some(metadata.override_priority())
        };
        group.get_priority(level, override_priority, false)
    }

    fn command_pri_to_level(pri: CommandPri) -> usize {
        match pri {
            CommandPri::High => 0,
            CommandPri::Normal => 1,
            CommandPri::Low => 2,
        }
    }
}

impl TaskPriorityProvider for ResourceController {
    fn priority_of(&self, extras: &yatp::queue::Extras) -> u64 {
        let metadata = TaskMetadata::from(extras.metadata());
        self.resource_group(metadata.group_name()).get_priority(
            extras.current_level() as usize,
            if metadata.override_priority() == 0 {
                None
            } else {
                Some(metadata.override_priority())
            },
            true,
        )
    }
}

fn concat_priority_vt(group_priority: u32, vt: u64) -> u64 {
    assert!((1..=16).contains(&group_priority));

    // map group_priority from [1, 16] to [0, 15] to limit it 4 bits and get bitwise
    // negation to replace leading 4 bits of vt. So that the priority is ordered in
    // the descending order by group_priority first, then by vt in ascending order.
    vt | (!((group_priority - 1) as u64) << 60)
}

struct GroupPriorityTracker {
    // the ru setting of this group.
    ru_quota: u64,
    group_priority: u32,
    weight: u64,
    virtual_time: AtomicU64,
    // the constant delta value for each `get_priority` call,
    vt_delta_for_get: u64,
}

impl GroupPriorityTracker {
    /// Computes the scheduling priority for a task at the given level.
    ///
    /// When `advance_vt` is true, atomically increments the virtual time
    /// (used when actually scheduling a task). When false, reads virtual
    /// time without advancing it (used for priority comparison only).
    fn get_priority(&self, level: usize, override_priority: Option<u32>, advance_vt: bool) -> u64 {
        let task_extra_priority = TASK_EXTRA_FACTOR_BY_LEVEL[level] * 1000 * self.weight;
        let priority = override_priority.unwrap_or(self.group_priority);
        let vt_delta = self.vt_delta_for_get;

        let vt = if advance_vt && vt_delta > 0 {
            self.virtual_time.fetch_add(vt_delta, Ordering::Relaxed) + vt_delta
        } else {
            self.virtual_time.load(Ordering::Relaxed) + vt_delta
        } + task_extra_priority;

        concat_priority_vt(priority, vt)
    }

    #[inline]
    fn current_vt(&self) -> u64 {
        self.virtual_time.load(Ordering::Relaxed)
    }

    #[inline]
    fn increase_vt(&self, vt_delta: u64) {
        self.virtual_time.fetch_add(vt_delta, Ordering::Relaxed);
    }

    #[inline]
    fn decrease_vt(&self, vt_delta: u64) {
        self.virtual_time.fetch_sub(vt_delta, Ordering::Relaxed);
    }

    // TODO: make it delta type as generic to avoid mixed consume different types.
    #[inline]
    fn consume(&self, resource: ResourceConsumeType) {
        let vt_delta = match resource {
            ResourceConsumeType::CpuTime(dur) => dur.as_micros() as u64,
            ResourceConsumeType::IoBytes(bytes) => bytes,
        } * self.weight;
        self.increase_vt(vt_delta);
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use file_system::IoBytes;
    use yatp::queue::Extras;

    use super::*;
    use crate::resource_limiter::ResourceType::{Cpu, Io};

    pub fn new_resource_group_ru(name: String, ru: u64, group_priority: u32) -> PbResourceGroup {
        new_resource_group(name, true, ru, ru, group_priority)
    }

    pub fn new_background_resource_group_ru(
        name: String,
        ru: u64,
        group_priority: u32,
        task_types: Vec<String>,
    ) -> PbResourceGroup {
        let mut rg = new_resource_group(name, true, ru, ru, group_priority);
        rg.mut_background_settings()
            .set_job_types(task_types.into());
        rg
    }

    pub fn new_resource_group(
        name: String,
        is_ru_mode: bool,
        read_tokens: u64,
        write_tokens: u64,
        group_priority: u32,
    ) -> PbResourceGroup {
        use kvproto::resource_manager::{GroupRawResourceSettings, GroupRequestUnitSettings};

        let mut group = PbResourceGroup::new();
        group.set_name(name);
        let mode = if is_ru_mode {
            GroupMode::RuMode
        } else {
            GroupMode::RawMode
        };
        group.set_mode(mode);
        group.set_priority(group_priority);
        if is_ru_mode {
            assert!(read_tokens == write_tokens);
            let mut ru_setting = GroupRequestUnitSettings::new();
            ru_setting
                .mut_r_u()
                .mut_settings()
                .set_fill_rate(read_tokens);
            group.set_r_u_settings(ru_setting);
        } else {
            let mut resource_setting = GroupRawResourceSettings::new();
            resource_setting
                .mut_cpu()
                .mut_settings()
                .set_fill_rate(read_tokens);
            resource_setting
                .mut_io_write()
                .mut_settings()
                .set_fill_rate(write_tokens);
            group.set_raw_resource_settings(resource_setting);
        }
        group
    }

    #[test]
    fn test_resource_group() {
        let resource_manager = ResourceGroupManager::default();
        assert_eq!(resource_manager.resource_groups.len(), 1);

        let group1 = new_resource_group_ru("TEST".into(), 100, 0);
        resource_manager.add_resource_group(group1);

        assert!(resource_manager.get_resource_group("test1").is_none());
        let group = resource_manager.get_resource_group("test").unwrap();
        assert_eq!(group.get_ru_quota(), 100);
        drop(group);
        assert_eq!(resource_manager.resource_groups.len(), 2);

        let group1 = new_resource_group_ru("Test".into(), 200, LOW_PRIORITY);
        resource_manager.add_resource_group(group1);
        let group = resource_manager.get_resource_group("test").unwrap();
        assert_eq!(group.get_ru_quota(), 200);
        assert_eq!(group.value().group.get_priority(), 1);
        drop(group);
        assert_eq!(resource_manager.resource_groups.len(), 2);

        let group2 = new_resource_group_ru("test2".into(), 400, 0);
        resource_manager.add_resource_group(group2);
        assert_eq!(resource_manager.resource_groups.len(), 3);

        let resource_ctl = resource_manager.derive_controller("test_read".into(), true);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 3);

        let group1 = resource_ctl.resource_group(b"test");
        let group2 = resource_ctl.resource_group(b"test2");
        assert_eq!(group1.weight, group2.weight * 2);
        assert_eq!(group1.current_vt(), 0);

        resource_ctl.consume(
            b"test",
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );
        resource_ctl.consume(
            b"test2",
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );

        assert_eq!(group1.current_vt(), group1.weight * 10000);
        assert_eq!(group1.current_vt(), group2.current_vt() * 2);

        // test update all group vts
        resource_manager.advance_min_virtual_time();
        let group1_vt = group1.current_vt();
        let group1_weight = group1.weight;
        assert_eq!(group1_vt, group1.weight * 10000);
        assert!(group2.current_vt() >= group1.current_vt() * 3 / 4);
        assert!(resource_ctl.resource_group(b"default").current_vt() >= group1.current_vt() / 2);

        drop(group1);
        drop(group2);

        // test add 1 new resource group
        let new_group = new_resource_group_ru("new_group".into(), 600, HIGH_PRIORITY);
        resource_manager.add_resource_group(new_group);

        assert_eq!(resource_ctl.resource_consumptions.read().len(), 4);
        let group3 = resource_ctl.resource_group("new_group".as_bytes());
        assert!(group1_weight - 10 <= group3.weight * 3 && group3.weight * 3 <= group1_weight + 10);
        assert!(group3.current_vt() >= group1_vt / 2);
        drop(group3);

        // test resource group resource limiter.
        let group1 = resource_manager.get_resource_group("test").unwrap();
        assert!(group1.limiter.is_none());
        assert!(
            resource_manager
                .get_resource_group("default")
                .unwrap()
                .limiter
                .is_none()
        );
        let new_default = new_background_resource_group_ru(
            "default".into(),
            10000,
            MEDIUM_PRIORITY,
            vec!["br".into()],
        );
        resource_manager.add_resource_group(new_default);
        let default_group = resource_manager.get_resource_group("default").unwrap();
        let limiter = default_group.limiter.as_ref().unwrap().clone();
        assert!(limiter.get_limiter(Cpu).get_rate_limit().is_infinite());
        assert!(limiter.get_limiter(Io).get_rate_limit().is_infinite());
        limiter.get_limiter(Cpu).set_rate_limit(100.0);
        limiter.get_limiter(Io).set_rate_limit(200.0);
        drop(group1);
        drop(default_group);

        let new_default = new_background_resource_group_ru(
            "default".into(),
            100,
            LOW_PRIORITY,
            vec!["lightning".into()],
        );
        resource_manager.add_resource_group(new_default);
        let default_group = resource_manager.get_resource_group("default").unwrap();
        assert_eq!(default_group.get_ru_quota(), 100);
        let new_limiter = default_group.limiter.as_ref().unwrap().clone();
        // check rate_limiter is not changed.
        assert_eq!(new_limiter.get_limiter(Cpu).get_rate_limit(), 100.0);
        assert_eq!(new_limiter.get_limiter(Io).get_rate_limit(), 200.0);
        assert_eq!(&*new_limiter as *const _, &*limiter as *const _);
        drop(default_group);

        // remove background setting, quota limiter should be none.
        let new_default = new_resource_group_ru("default".into(), 100, LOW_PRIORITY);
        resource_manager.add_resource_group(new_default);
        assert!(
            resource_manager
                .get_resource_group("default")
                .unwrap()
                .limiter
                .is_none()
        );
    }

    #[test]
    fn test_resource_group_crud() {
        let resource_manager = ResourceGroupManager::default();
        assert_eq!(resource_manager.get_group_count(), 1);

        let group1 = new_resource_group_ru("test1".into(), 100, HIGH_PRIORITY);
        resource_manager.add_resource_group(group1);
        assert_eq!(resource_manager.get_group_count(), 2);

        let group2 = new_resource_group_ru("test2".into(), 200, LOW_PRIORITY);
        resource_manager.add_resource_group(group2);
        assert_eq!(resource_manager.get_group_count(), 3);

        let group1 = new_resource_group_ru("test1".into(), 150, HIGH_PRIORITY);
        resource_manager.add_resource_group(group1.clone());
        assert_eq!(resource_manager.get_group_count(), 3);
        assert_eq!(
            resource_manager.get_resource_group("test1").unwrap().group,
            group1
        );

        resource_manager.remove_resource_group("test2");
        assert!(resource_manager.get_resource_group("test2").is_none());
        assert_eq!(resource_manager.get_group_count(), 2);

        resource_manager.remove_resource_group("test2");
        assert_eq!(resource_manager.get_group_count(), 2);
    }

    #[test]
    fn test_read_group_policy_preserves_identity_until_deleted() {
        let manager = ResourceGroupManager::default();
        manager.add_resource_group(new_resource_group_ru("Tenant".into(), 100, LOW_PRIORITY));
        let policy = manager.group("tenant");
        assert_eq!(policy.name(), "tenant");
        assert_eq!(policy.weight(), 100);
        assert_eq!(policy.priority(), LOW_PRIORITY);
        assert!(Arc::ptr_eq(&policy, &manager.group("TENANT")));

        // A reload and a real quota/priority update both keep scheduler state
        // attached to the same identity.
        manager.add_resource_group(new_resource_group_ru("tenant".into(), 100, LOW_PRIORITY));
        assert!(Arc::ptr_eq(&policy, &manager.group("tenant")));
        manager.add_resource_group(new_resource_group_ru("tenant".into(), 400, HIGH_PRIORITY));
        assert!(Arc::ptr_eq(&policy, &manager.group("tenant")));
        assert_eq!(policy.weight(), 400);
        assert_eq!(policy.priority(), HIGH_PRIORITY);

        manager.remove_resource_group("tenant");
        assert!(policy.is_retired());
        assert!(Arc::ptr_eq(
            &manager.group("tenant"),
            &manager.group("default")
        ));
        manager.add_resource_group(new_resource_group_ru("tenant".into(), 200, 0));
        let recreated = manager.group("tenant");
        assert_ne!(policy.id(), recreated.id());
        assert!(!recreated.is_retired());
        assert_eq!(recreated.priority(), MEDIUM_PRIORITY);
    }

    #[test]
    fn test_read_group_policy_special_weights_do_not_rescale_finite_groups() {
        let manager = ResourceGroupManager::default();
        let default = manager.group("default");
        assert_eq!(default.weight(), DEFAULT_MAX_RU_QUOTA * 10);
        manager.add_resource_group(new_resource_group_ru("unlimited".into(), 0, 0));
        let unlimited = manager.group("unlimited");
        manager.add_resource_group(new_resource_group_ru("small".into(), 10, 0));
        let small = manager.group("small");
        manager.add_resource_group(new_resource_group_ru("large".into(), 50_000, 0));
        assert_eq!(default.weight(), 500_000);
        assert_eq!(unlimited.weight(), 500_000);
        assert_eq!(small.weight(), 10);

        manager.remove_resource_group("large");
        assert_eq!(default.weight(), DEFAULT_MAX_RU_QUOTA * 10);
        assert_eq!(unlimited.weight(), DEFAULT_MAX_RU_QUOTA * 10);
        assert_eq!(small.weight(), 10);

        // An explicitly finite default quota is an ordinary weight. Raw-mode
        // read policies use CPU quota rather than the write-IO quota.
        manager.add_resource_group(new_resource_group_ru("default".into(), 100, 0));
        manager.add_resource_group(new_resource_group("raw".into(), false, 20, 90_000, 0));
        assert_eq!(default.weight(), 100);
        assert_eq!(manager.group("raw").weight(), 20);
        manager.add_resource_group(new_resource_group_ru("maximum".into(), u64::MAX, 0));
        assert_eq!(manager.group("maximum").weight(), MAX_RU_QUOTA);
        assert_eq!(unlimited.weight(), MAX_RU_QUOTA);
        assert_eq!(small.weight(), 10);
    }

    #[test]
    fn test_read_group_policy_retain_and_default_fallback() {
        let manager = ResourceGroupManager::default();
        let default = manager.group("default");
        manager.add_resource_group(new_resource_group_ru("kept".into(), 100, 0));
        manager.add_resource_group(new_resource_group_ru("removed".into(), 200, 0));
        let kept = manager.group("kept");
        let removed = manager.group("removed");
        manager.retain(|name, _| name == "kept");
        assert!(removed.is_retired());
        assert!(!kept.is_retired());
        assert!(Arc::ptr_eq(&default, &manager.group("missing")));
        assert!(Arc::ptr_eq(&kept, &manager.group("kept")));

        manager.add_resource_group(new_resource_group_ru("default".into(), 10, LOW_PRIORITY));
        manager.remove_resource_group("default");
        assert!(Arc::ptr_eq(&default, &manager.group("default")));
        assert!(!default.is_retired());
        assert_eq!(default.priority(), MEDIUM_PRIORITY);
        assert_eq!(default.weight(), DEFAULT_MAX_RU_QUOTA * 10);
        assert_eq!(manager.get_group_count(), 2);
    }

    #[test]
    fn test_resource_group_priority() {
        let resource_manager = ResourceGroupManager::default();
        let group1 = new_resource_group_ru("test1".into(), 200, LOW_PRIORITY);
        resource_manager.add_resource_group(group1);
        let group2 = new_resource_group_ru("test2".into(), 400, 0);
        resource_manager.add_resource_group(group2);
        assert_eq!(resource_manager.resource_groups.len(), 3);

        let resource_ctl = resource_manager.derive_controller("test".into(), true);

        let group1 = resource_ctl.resource_group("test1".as_bytes());
        let group2 = resource_ctl.resource_group("test2".as_bytes());
        assert_eq!(group1.weight, group2.weight * 2);
        assert_eq!(group1.current_vt(), 0);

        let mut extras1 = Extras::single_level();
        extras1.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "test1".to_string(),
                override_priority: 0,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras1),
            concat_priority_vt(LOW_PRIORITY, group1.weight * 50)
        );
        assert_eq!(group1.current_vt(), group1.weight * 50);

        let mut extras2 = Extras::single_level();
        extras2.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "test2".to_string(),
                override_priority: 0,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras2),
            concat_priority_vt(MEDIUM_PRIORITY, group2.weight * 50)
        );
        assert_eq!(group2.current_vt(), group2.weight * 50);

        // test override priority
        let mut extras2_override = Extras::single_level();
        extras2_override.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "test2".to_string(),
                override_priority: LOW_PRIORITY as u64,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras2_override),
            concat_priority_vt(LOW_PRIORITY, group2.weight * 100)
        );
        assert_eq!(group2.current_vt(), group2.weight * 100);

        let mut extras3 = Extras::single_level();
        extras3.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "unknown_group".to_string(),
                override_priority: 0,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras3),
            concat_priority_vt(MEDIUM_PRIORITY, 50)
        );
        assert_eq!(
            resource_ctl
                .resource_group("default".as_bytes())
                .current_vt(),
            50
        );
    }

    #[test]
    fn test_reset_resource_group_vt() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_write".into(), false);

        let group1 = new_resource_group_ru("g1".into(), i32::MAX as u64, 1);
        resource_manager.add_resource_group(group1);
        let group2 = new_resource_group_ru("g2".into(), 1, 16);
        resource_manager.add_resource_group(group2);

        let g1 = resource_ctl.resource_group(b"g1");
        let g2 = resource_ctl.resource_group(b"g2");
        let threshold = 1 << 59;
        let mut last_g2_vt = 0;
        for i in 0..8 {
            resource_ctl.consume(b"g2", ResourceConsumeType::IoBytes(1 << 25));
            resource_manager.advance_min_virtual_time();
            if i < 7 {
                assert!(g2.current_vt() < threshold);
            }
            // after 8 round, g1's vt still under the threshold and is still increasing.
            assert!(g1.current_vt() < threshold && g1.current_vt() > last_g2_vt);
            last_g2_vt = g2.current_vt();
        }

        resource_ctl.consume(b"g2", ResourceConsumeType::IoBytes(1 << 25));
        resource_manager.advance_min_virtual_time();
        assert!(g1.current_vt() > threshold);

        // adjust again, the virtual time of each group should decrease
        resource_manager.advance_min_virtual_time();
        let g1_vt = g1.current_vt();
        let g2_vt = g2.current_vt();
        assert!(g2_vt < threshold / 2);
        assert!(g1_vt < threshold / 2 && g1_vt < g2_vt);
        assert_eq!(resource_ctl.last_min_vt.load(Ordering::Relaxed), g2_vt);
    }

    #[test]
    fn test_adjust_resource_group_weight() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_read".into(), true);
        let resource_ctl_write = resource_manager.derive_controller("test_write".into(), false);
        assert_eq!(resource_ctl.is_customized(), false);
        assert_eq!(resource_ctl_write.is_customized(), false);
        let group1 = new_resource_group_ru("test1".into(), 5000, 0);
        resource_manager.add_resource_group(group1);
        assert_eq!(resource_ctl.resource_group(b"test1").weight, 20);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 20);
        assert_eq!(resource_ctl.is_customized(), true);
        assert_eq!(resource_ctl_write.is_customized(), true);

        // add a resource group with big ru
        let group1 = new_resource_group_ru("test2".into(), 50000, 0);
        resource_manager.add_resource_group(group1);
        assert_eq!(*resource_ctl.max_ru_quota.lock().unwrap(), 50000);
        assert_eq!(resource_ctl.resource_group(b"test1").weight, 100);
        assert_eq!(resource_ctl.resource_group(b"test2").weight, 10);
        // resource_ctl_write should be unchanged.
        assert_eq!(*resource_ctl_write.max_ru_quota.lock().unwrap(), 50000);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 100);
        assert_eq!(resource_ctl_write.resource_group(b"test2").weight, 10);

        // add the default "default" group, the ru weight should not change.
        // add a resource group with big ru
        let group = new_resource_group_ru("default".into(), u32::MAX as u64, 0);
        resource_manager.add_resource_group(group);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 100);
        assert_eq!(resource_ctl_write.resource_group(b"default").weight, 1);

        // change the default group to another value, it can impact the ru then.
        let group = new_resource_group_ru("default".into(), 100000, 0);
        resource_manager.add_resource_group(group);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 200);
        assert_eq!(resource_ctl_write.resource_group(b"default").weight, 10);
    }

    #[test]
    fn test_background_settings_do_not_customize_resource_controller() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_write".into(), false);

        let default_group = new_background_resource_group_ru(
            DEFAULT_RESOURCE_GROUP_NAME.into(),
            MAX_RU_QUOTA,
            MEDIUM_PRIORITY,
            vec!["br".into()],
        );
        resource_manager.add_resource_group(default_group);

        assert!(!resource_ctl.is_customized());

        let group = new_resource_group_ru("test".into(), 5000, MEDIUM_PRIORITY);
        resource_manager.add_resource_group(group);
        assert!(resource_ctl.is_customized());

        resource_manager.remove_resource_group("test");
        assert!(!resource_ctl.is_customized());
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_reset_resource_group_vt_overflow() {
        use rand::{RngCore, thread_rng};
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_write".into(), false);
        let mut rng = thread_rng();

        let mut min_delta = u64::MAX;
        let mut max_delta = 0;
        for i in 0..10 {
            let name = format!("g{}", i);
            let g = new_resource_group_ru(name.clone(), 100, 1);
            resource_manager.add_resource_group(g);
            let delta = rng.next_u64() % 10000 + 1;
            min_delta = delta.min(min_delta);
            max_delta = delta.max(max_delta);
            resource_ctl
                .resource_group(name.as_bytes())
                .increase_vt(RESET_VT_THRESHOLD + delta);
        }
        resource_ctl
            .resource_group(b"default")
            .increase_vt(RESET_VT_THRESHOLD + 1);

        let old_max_vt = resource_ctl
            .resource_consumptions
            .read()
            .iter()
            .fold(0, |v, (_, g)| v.max(g.current_vt()));
        let resource_ctl_cloned = resource_ctl.clone();
        fail::cfg_callback("increase_vt_duration_update_min_vt", move || {
            resource_ctl_cloned
                .resource_consumptions
                .read()
                .iter()
                .enumerate()
                .for_each(|(i, (_, tracker))| {
                    if i % 2 == 0 {
                        tracker.increase_vt(max_delta - min_delta);
                    }
                });
        })
        .unwrap();
        resource_ctl.update_min_virtual_time();
        fail::remove("increase_vt_duration_update_min_vt");

        let new_max_vt = resource_ctl
            .resource_consumptions
            .read()
            .iter()
            .fold(0, |v, (_, g)| v.max(g.current_vt()));
        // check all vt has decreased by RESET_VT_THRESHOLD.
        assert!(new_max_vt < max_delta * 2);
        // check fail-point takes effect, the `new_max_vt` has increased.
        assert!(old_max_vt - RESET_VT_THRESHOLD < new_max_vt);
    }

    #[test]
    fn test_retain_resource_groups() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_read".into(), true);
        let resource_ctl_write = resource_manager.derive_controller("test_write".into(), false);

        for i in 0..5 {
            let group1 = new_resource_group_ru(format!("test{}", i), 100, 0);
            resource_manager.add_resource_group(group1);
            // add a resource group with big ru
            let group1 = new_resource_group_ru(format!("group{}", i), 100, 0);
            resource_manager.add_resource_group(group1);
        }
        // consume for default group
        resource_ctl.consume(
            b"default",
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );
        resource_ctl_write.consume(b"default", ResourceConsumeType::IoBytes(10000));

        // 10 + 1(default)
        assert_eq!(resource_manager.get_all_resource_groups().len(), 11);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 11);
        assert_eq!(resource_ctl_write.resource_consumptions.read().len(), 11);

        resource_manager.retain(|k, _v| k.starts_with("test"));
        assert_eq!(resource_manager.get_all_resource_groups().len(), 6);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 6);
        assert_eq!(resource_ctl_write.resource_consumptions.read().len(), 6);
        assert!(resource_manager.get_resource_group("group1").is_none());
        // should use the virtual time of default group for non-exist group
        assert_ne!(resource_ctl.resource_group(b"group2").current_vt(), 0);
        assert_ne!(resource_ctl_write.resource_group(b"group2").current_vt(), 0);
    }

    #[test]
    fn test_concat_priority_vt() {
        let v1 = concat_priority_vt(MEDIUM_PRIORITY, 1000);
        let v2 = concat_priority_vt(MEDIUM_PRIORITY, 1111);
        assert!(v1 < v2);

        let v3 = concat_priority_vt(LOW_PRIORITY, 1000);
        assert!(v1 < v3);

        let v4 = concat_priority_vt(MEDIUM_PRIORITY, 1111);
        assert_eq!(v2, v4);

        let v5 = concat_priority_vt(HIGH_PRIORITY, 10);
        assert!(v5 < v1);
    }

    #[test]
    fn test_adjust_group_throttling_decreases_by_10_percent_per_tick() {
        // fg_cpu_throttle_threshold=70, baseline_burst_pct=20 (defaults) ->
        // burst_factor = 1.2.
        let mgr = ResourceGroupManager::default();
        mgr.add_resource_group(new_resource_group_ru("g1".into(), 1000, HIGH_PRIORITY));
        let limiter = mgr.get_foreground_group_limiter("g1");

        // Seed one completed bucket (historical) and a large spike in the
        // still-open bucket (current), so hist > 0 and current is well
        // above burst_target = hist * 1.2.
        let t0 = RuTracker::now_secs();
        {
            let entry = mgr.ru_trackers.get("g1").unwrap();
            let mut guard = entry.lock().unwrap();
            guard.0.record_at(6000, t0 + 30);
            guard.0.record_at(0, t0 + 60); // closes bucket: hist ~= 6000/30 = 200/s
            guard.0.record_at(120_000, t0 + 65); // open-bucket spike: current >> burst_target
        }
        mgr.set_bg_cpu_at_floor(true);
        let now = t0 + 90;

        // First tick: no limit set yet (starts at INFINITY), so the base is
        // the measured current rate, tightened by 10% — not an interpolated
        // jump straight to burst_target.
        mgr.adjust_group_throttling(90.0, now);
        let after_tick1 = limiter.get_limiter(ResourceType::Cpu).get_rate_limit();
        let current_rate = {
            let entry = mgr.ru_trackers.get("g1").unwrap();
            entry.lock().unwrap().0.current_rate(now)
        };
        assert!(
            (after_tick1 - current_rate * 0.9).abs() < current_rate * 0.01,
            "first tick should tighten 10% below measured current rate, got {after_tick1}, \
             expected ~{}",
            current_rate * 0.9
        );

        // Second tick, same inputs: base is now the persisted current_limit
        // from tick 1 (not a freshly measured/interpolated value), so it
        // tightens another 10% relative to itself rather than staying put
        // or jumping to burst_target.
        mgr.adjust_group_throttling(90.0, now);
        let after_tick2 = limiter.get_limiter(ResourceType::Cpu).get_rate_limit();
        assert!(
            (after_tick2 - after_tick1 * 0.9).abs() < after_tick1 * 0.01,
            "second tick should tighten another 10% relative to the previous tick's limit, \
             got {after_tick2}, expected ~{}",
            after_tick1 * 0.9
        );

        // Repeated ticks converge to and stop at burst_target = hist * 1.2,
        // never going below it.
        for _ in 0..60 {
            mgr.adjust_group_throttling(90.0, now);
        }
        let floored = limiter.get_limiter(ResourceType::Cpu).get_rate_limit();
        let hist = {
            let entry = mgr.ru_trackers.get("g1").unwrap();
            entry.lock().unwrap().0.cached_historical_rate
        };
        let burst_target = hist * 1.2;
        assert!(
            (floored - burst_target).abs() < burst_target * 0.01,
            "should converge to and stop at burst_target ({burst_target}), got {floored}"
        );
    }

    #[test]
    fn test_get_resource_limiter() {
        let mgr = ResourceGroupManager::default();

        let default_group = new_background_resource_group_ru(
            "default".into(),
            200,
            MEDIUM_PRIORITY,
            vec!["br".into(), "stats".into()],
        );
        mgr.add_resource_group(default_group);
        let default_limiter = mgr
            .get_resource_group("default")
            .unwrap()
            .limiter
            .clone()
            .unwrap();

        // Even with only 1 group (default), foreground returns the
        // per-group limiter regardless of priority level.
        let fg_default_limiter = mgr.get_resource_limiter("default", "query", 0).unwrap();
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("default", "query", HIGH_PRIORITY as u64)
                .unwrap(),
            &fg_default_limiter,
        ));
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("default", "query", LOW_PRIORITY as u64)
                .unwrap(),
            &fg_default_limiter,
        ));

        let group1 = new_resource_group("test1".into(), true, 100, 100, HIGH_PRIORITY);
        mgr.add_resource_group(group1);

        let bg_group = new_background_resource_group_ru(
            "bg".into(),
            50,
            LOW_PRIORITY,
            vec!["ddl".into(), "stats".into()],
        );
        mgr.add_resource_group(bg_group);
        let bg_limiter = mgr
            .get_resource_group("bg")
            .unwrap()
            .limiter
            .clone()
            .unwrap();

        assert!(
            mgr.get_background_resource_limiter("test1", "ddl")
                .is_none()
        );
        assert!(Arc::ptr_eq(
            &mgr.get_background_resource_limiter("test1", "stats")
                .unwrap(),
            &default_limiter
        ));

        assert!(Arc::ptr_eq(
            &mgr.get_background_resource_limiter("bg", "stats").unwrap(),
            &bg_limiter
        ));
        assert!(mgr.get_background_resource_limiter("bg", "br").is_none());
        assert!(
            mgr.get_background_resource_limiter("bg", "invalid")
                .is_none()
        );

        assert!(Arc::ptr_eq(
            &mgr.get_background_resource_limiter("unknown", "stats")
                .unwrap(),
            &default_limiter
        ));

        // Background path still takes priority for "stats" source.
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("test1", "stats", 0).unwrap(),
            &default_limiter
        ));

        // Multiple groups: all foreground priorities get the per-group limiter.
        // The same limiter is returned regardless of priority level.
        let fg_limiter = mgr
            .get_resource_limiter("test1", "query", LOW_PRIORITY as u64)
            .unwrap();
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("test1", "query", HIGH_PRIORITY as u64)
                .unwrap(),
            &fg_limiter,
        ));
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("test1", "query", 0).unwrap(),
            &fg_limiter,
        ));
    }

    #[test]
    fn test_ru_tracker() {
        let t0: u64 = 1_000_000;

        const BUCKETS: usize = 15;
        let mut tracker = RuTracker::new(t0, BUCKETS);
        assert!(!tracker.is_warmed_up());
        // No data at all: current_rate = (0 + 0) / (0 + 60) = 0.
        assert_eq!(tracker.current_rate(t0), 0.0);
        assert_eq!(tracker.historical_rate(t0, t0), 0.0);

        // Record 6000 RU in the first 30s bucket.
        tracker.record_at(6000, t0 + 15);

        // Advance past the first bucket boundary (30s) — completes bucket 0.
        tracker.record_at(0, t0 + 30);
        assert_eq!(tracker.completed, 1);
        // At t0+30: current_bucket=0, elapsed=0, last_completed=6000
        // rate = (0 + 6000) / (0 + 30) = 200 RU/s
        assert!((tracker.current_rate(t0 + 30) - 200.0).abs() < 0.01);
        assert!(!tracker.is_warmed_up()); // needs ≥2 buckets

        // Advance another 30s with 3000 RU — completes bucket 1.
        tracker.record_at(3000, t0 + 45);
        tracker.record_at(0, t0 + 60);
        assert_eq!(tracker.completed, 2);
        assert!(tracker.is_warmed_up());
        // At t0+60: current_bucket=0, elapsed=0, last_completed=3000
        // rate = (0 + 3000) / (0 + 30) = 100 RU/s
        assert!((tracker.current_rate(t0 + 60) - 100.0).abs() < 0.01);
        // At t0+75 (15s into next bucket, no new RU): current_bucket=0, elapsed=15
        // rate = (0 + 3000) / (15 + 30) = 66.67 RU/s
        assert!((tracker.current_rate(t0 + 75) - 66.67).abs() < 0.1);
        // historical_rate = (6000+3000) / (2*30) = 150 RU/s
        assert!((tracker.historical_rate(t0, t0 + 60) - 150.0).abs() < 0.01);

        // Ring buffer: advance 20 more minutes (fully evicts all data).
        let t_far = t0 + 60 * 20;
        tracker.record_at(0, t_far);
        // Gap exceeds window → ring reset, no completed buckets.
        assert_eq!(tracker.completed, 0);
        assert_eq!(tracker.current_bucket.load(Ordering::Relaxed), 0);

        // Re-populate after the big gap and fill the entire ring.
        for i in 1..=BUCKETS {
            tracker.record_at(100, t_far + (i as u64) * RU_BUCKET_SECS);
        }
        assert_eq!(tracker.completed, BUCKETS);
    }

    #[test]
    fn test_read_admission_bypasses_foreground_history_but_not_background_limits() {
        let manager = ResourceGroupManager::new(Config {
            // Deprecated read settings must not reactivate historical delay.
            enable_read_admission_control: true,
            enable_fair_scheduling: true,
            enable_write_admission_control: true,
            ..Config::default()
        });
        let foreground = manager.get_foreground_group_limiter("default");
        foreground.get_limiter(Cpu).set_rate_limit(1.0);
        foreground.consume(Duration::from_secs(1), IoBytes::default(), false, true);

        assert_eq!(
            manager.admission_decision(true, &foreground),
            AdmissionDecision::Allow
        );
        assert_eq!(foreground.get_limiter(Cpu).get_rate_limit(), 1.0);
        assert!(matches!(
            manager.admission_decision(false, &foreground),
            AdmissionDecision::Delay(_)
        ));
        manager.release_delay_slot();

        let background = manager.get_background_limiter();
        background.get_limiter(Cpu).set_rate_limit(1.0);
        background.consume(Duration::from_secs(1), IoBytes::default(), false, true);
        assert!(matches!(
            manager.admission_decision(true, &background),
            AdmissionDecision::Delay(_)
        ));
        manager.release_delay_slot();
        assert_eq!(manager.delayed_req_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_write_admission_decision() {
        let mut cfg = Config::default();
        cfg.enable_write_admission_control = true;
        cfg.admission_max_delayed_count = 10; // low limit to test rejection path
        let mgr = ResourceGroupManager::new(cfg);
        // Add a second group so the code path is active.
        mgr.add_resource_group(new_resource_group_ru("spike".into(), 1000, HIGH_PRIORITY));
        let spike_limiter = mgr.get_foreground_group_limiter("spike");

        // Seed initial RU so the tracker is not idle (prevents eviction by
        // online_adjust_resource_quota's retain call).
        let t0 = RuTracker::now_secs();
        mgr.record_ru_consumption("spike", 1);

        // CPU below threshold → Allow (stays NO_LIMIT, no throttling).
        mgr.online_adjust_resource_quota(50.0); // below 80%
        assert_eq!(
            mgr.admission_decision(false, &spike_limiter),
            AdmissionDecision::Allow
        );

        // CPU above threshold but no warmed-up tracker data → stays NO_LIMIT → Allow.
        mgr.online_adjust_resource_quota(90.0);
        assert_eq!(
            mgr.admission_decision(false, &spike_limiter),
            AdmissionDecision::Allow
        );

        // Warm up the tracker: 2 completed buckets. Set up BEFORE calling
        // online_adjust_resource_quota so historical_rate() is non-zero when the
        // limiter rate is configured.
        {
            let entry = mgr.ru_trackers.get("spike").unwrap();
            let mut guard = entry.lock().unwrap();
            guard.0.record_at(6000, t0 + 30);
            guard.0.record_at(0, t0 + 60); // close bucket 0: 6000 RU (baseline)
            guard.0.record_at(12000, t0 + 90); // open bucket: 12000 RU spike (left open)
            // historical ≈ 6000/30 = 200 RU/s (only completed buckets), current
            // ≈ 400
        }
        // Now set CPU — first throttle entry: initializes fraction from spike ratio,
        // then sets absolute rate = historical × fraction per group.
        // bg_cpu_at_floor must be true for the throttle branch to fire.
        mgr.set_bg_cpu_at_floor(true);
        mgr.online_adjust_resource_quota(90.0);
        // Consume a burst well above the rate to build token-bucket debt.
        {
            let entry = mgr.ru_trackers.get("spike").unwrap();
            let guard = entry.lock().unwrap();
            // 10_000 µs consumed against ~133 RU/s rate → several seconds of debt.
            guard.1.consume(
                Duration::from_micros(10_000),
                IoBytes::default(),
                false,
                true,
            );
        }
        // Debt is non-zero → Delay.
        assert!(matches!(
            mgr.admission_decision(false, &spike_limiter),
            AdmissionDecision::Delay(_)
        ));
        // One slot acquired above; release it.
        mgr.release_delay_slot();

        // Exhaust the delay slots: acquire 10 (the configured max).
        for _ in 0..10 {
            assert!(matches!(
                mgr.admission_decision(false, &spike_limiter),
                AdmissionDecision::Delay(_)
            ));
        }
        // 11th request: over the limit → Reject.
        assert_eq!(
            mgr.admission_decision(false, &spike_limiter),
            AdmissionDecision::Reject
        );
        // Release all slots.
        for _ in 0..10 {
            mgr.release_delay_slot();
        }

        // Drop CPU into leeway zone (72-80%): multiplier holds, still delays.
        mgr.online_adjust_resource_quota(75.0);
        assert!(matches!(
            mgr.admission_decision(false, &spike_limiter),
            AdmissionDecision::Delay(_)
        ));

        // Drop CPU below leeway_start (72%): fraction ramps up ×1.1/tick.
        // After enough ticks it reaches 5× → NO_LIMIT → rates set to infinity
        // → token bucket debt clears → Allow.
        for _ in 0..60 {
            mgr.online_adjust_resource_quota(50.0);
        }
        assert_eq!(
            mgr.admission_decision(false, &spike_limiter),
            AdmissionDecision::Allow
        );
    }
}
