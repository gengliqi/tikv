// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::TryFrom,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc::SyncSender,
    },
    time::Duration,
};

use file_system::{IoType, set_io_type};
use futures::{
    channel::oneshot,
    future::{BoxFuture, FutureExt, TryFutureExt},
};
use kvproto::{errorpb, kvrpcpb::CommandPri};
use online_config::{ConfigChange, ConfigManager, ConfigValue, Result as CfgResult};
use prometheus::{Histogram, IntCounter, IntGauge, core::Metric};
use resource_control::{
    AdmissionDecision, READ_POOL_CPU_VEC, ResourceGroupManager, ResourceLimiter, TaskPriority,
    with_resource_limiter,
};
use thiserror::Error;
use tikv_util::{
    resource_control::TaskMetadata,
    sys::{SysQuota, cpu_time::ProcessStat},
    thread_name_prefix::{UNIFIED_READ_POOL_THREAD, matches_thread_name_prefix},
    time::Instant,
    worker::{Runnable, RunnableWithTimer, Scheduler, Worker},
    yatp_pool::{
        self, CleanupMethod, FuturePool, PoolTicker, YatpPoolBuilder,
        fair_queue::{FairCancellation, FairQueue, FairQueueConfig},
    },
};
use tracker::TlsTrackedFuture;
use yatp::{
    metrics::MULTILEVEL_LEVEL_ELAPSED, pool::Remote, queue::Extras, task::future::TaskCell,
};

use self::metrics::*;
use crate::{
    config::{UNIFIED_READPOOL_MIN_CONCURRENCY, UnifiedReadPoolConfig},
    storage::kv::{Engine, FlowStatsReporter, destroy_tls_engine, set_tls_engine},
};

// the duration to check auto-scale unified-thread-pool's thread
const READ_POOL_THREAD_CHECK_DURATION: Duration = Duration::from_secs(10);
// consider scale out read pool size if the average thread cpu usage is higher
// than this threshold.
const READ_POOL_THREAD_HIGH_THRESHOLD: f64 = 0.8;
// consider scale in read pool size if the average thread cpu usage is lower
// than this threshold.
const READ_POOL_THREAD_LOW_THRESHOLD: f64 = 0.7;
// avg running tasks per-thread that indicates read-pool is busy
const RUNNING_TASKS_PER_THREAD_THRESHOLD: i64 = 3;

pub enum ReadPool {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        pool: yatp::ThreadPool<TaskCell>,
        running_tasks: [IntGauge; TaskPriority::PRIORITY_COUNT],
        running_threads: IntGauge,
        max_tasks: usize,
        pool_size: usize,
        fair_queue: Option<Arc<FairQueue>>,
        resource_manager: Option<Arc<ResourceGroupManager>>,
        time_slice_inspector: Arc<TimeSliceInspector>,
    },
}

impl ReadPool {
    pub fn handle(&self) -> ReadPoolHandle {
        match self {
            ReadPool::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => ReadPoolHandle::FuturePools {
                read_pool_high: read_pool_high.clone(),
                read_pool_normal: read_pool_normal.clone(),
                read_pool_low: read_pool_low.clone(),
            },
            ReadPool::Yatp {
                pool,
                running_tasks,
                running_threads,
                max_tasks,
                pool_size,
                fair_queue,
                resource_manager,
                time_slice_inspector,
            } => ReadPoolHandle::Yatp {
                remote: pool.remote().clone(),
                running_tasks: running_tasks.clone(),
                running_threads: running_threads.clone(),
                max_tasks: *max_tasks,
                pool_size: *pool_size,
                fair_queue: fair_queue.clone(),
                resource_manager: resource_manager.clone(),
                time_slice_inspector: time_slice_inspector.clone(),
            },
        }
    }
}

#[derive(Clone)]
pub enum ReadPoolHandle {
    FuturePools {
        read_pool_high: FuturePool,
        read_pool_normal: FuturePool,
        read_pool_low: FuturePool,
    },
    Yatp {
        remote: Remote<TaskCell>,
        running_tasks: [IntGauge; TaskPriority::PRIORITY_COUNT],
        running_threads: IntGauge,
        max_tasks: usize,
        pool_size: usize,
        fair_queue: Option<Arc<FairQueue>>,
        resource_manager: Option<Arc<ResourceGroupManager>>,
        time_slice_inspector: Arc<TimeSliceInspector>,
    },
}

/// Keeps metrics tied to the entire live task, including queued and IO-waiting
/// work. Dropping a rejected, evicted, or canceled task releases the count too.
struct RunningTaskGuard(IntGauge);

impl RunningTaskGuard {
    fn new(gauge: IntGauge) -> Self {
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for RunningTaskGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

#[derive(Default)]
struct CancellationState {
    cancelled: bool,
    handle: Option<FairCancellation>,
}

type CancellationSlot = Arc<Mutex<CancellationState>>;

/// Cancels an RC read task when its caller stops waiting for the response.
/// Non-RC pools retain their existing detached-task behavior.
pub struct ReadPoolCancellationGuard(Option<CancellationSlot>);

impl ReadPoolCancellationGuard {
    fn disarm(&mut self) {
        self.0.take();
    }
}

impl Drop for ReadPoolCancellationGuard {
    fn drop(&mut self) {
        let Some(slot) = self.0.as_ref() else {
            return;
        };
        // Cancellation can remove and drop a queued task. Do not call it while
        // holding the slot lock.
        let cancellation = {
            let mut state = slot.lock().unwrap();
            state.cancelled = true;
            state.handle.take()
        };
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
    }
}

/// Preserve background admission before taking live-task capacity. Foreground
/// historical admission is bypassed by ResourceGroupManager on the read path.
async fn admit_read_task(
    resource_manager: Option<&Arc<ResourceGroupManager>>,
    resource_limiter: Option<&ResourceLimiter>,
) -> Result<(), ReadPoolError> {
    if let (Some(manager), Some(limiter)) = (resource_manager, resource_limiter) {
        match manager.admission_decision(true, limiter) {
            AdmissionDecision::Reject => return Err(ReadPoolError::Rejected),
            AdmissionDecision::Delay(delay) => {
                let _guard = manager.delay_slot_guard();
                futures_timer::Delay::new(delay).await;
            }
            AdmissionDecision::Allow => {}
        }
    }
    Ok(())
}

impl ReadPoolHandle {
    pub fn spawn<F>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
    ) -> BoxFuture<'static, Result<(), ReadPoolError>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_impl(f, priority, task_id, metadata, resource_limiter, None)
    }

    /// Submits a task and returns a separate guard for the response lifetime.
    /// Keep the guard until the entire response future or stream is dropped;
    /// completing the admission future alone does not complete the task.
    pub fn spawn_with_cancellation<F>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
    ) -> (
        BoxFuture<'static, Result<(), ReadPoolError>>,
        ReadPoolCancellationGuard,
    )
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let cancellation = match self {
            Self::Yatp {
                fair_queue: Some(_),
                ..
            } => Some(Arc::new(Mutex::new(CancellationState::default()))),
            _ => None,
        };
        let guard = ReadPoolCancellationGuard(cancellation.clone());
        (
            self.spawn_impl(
                f,
                priority,
                task_id,
                metadata,
                resource_limiter,
                cancellation,
            ),
            guard,
        )
    }

    fn spawn_impl<F>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
        cancellation: Option<CancellationSlot>,
    ) -> BoxFuture<'static, Result<(), ReadPoolError>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_high,
                read_pool_normal,
                read_pool_low,
            } => {
                let pool = match priority {
                    CommandPri::High => read_pool_high,
                    CommandPri::Normal => read_pool_normal,
                    CommandPri::Low => read_pool_low,
                };
                let res = pool.spawn(f).map_err(ReadPoolError::from);
                futures::future::ready(res).boxed()
            }
            ReadPoolHandle::Yatp {
                remote,
                running_tasks,
                max_tasks,
                fair_queue,
                resource_manager,
                ..
            } => {
                let task_priority = TaskPriority::from(metadata.override_priority());
                let gauge = running_tasks[task_priority as usize].clone();
                let is_background = resource_limiter.as_ref().is_some_and(|l| l.is_background());
                let fixed_level = if is_background {
                    Some(2)
                } else {
                    match priority {
                        CommandPri::High => Some(0),
                        CommandPri::Normal => None,
                        CommandPri::Low => Some(2),
                    }
                };
                let metadata = metadata.to_vec();
                let remote = remote.clone();
                let max_tasks = *max_tasks;
                let fair_queue = fair_queue.clone();
                let resource_manager = resource_manager.clone();
                // Capture the request's tracker before returning the admission
                // future: that future may be polled on a different thread.
                let f = TlsTrackedFuture::new(with_resource_limiter(
                    f,
                    resource_limiter.clone(),
                    true,
                    true, // retain usage/debt statistics without in-pool sleep
                    resource_manager.clone(),
                    0,
                ));
                async move {
                    if cancellation
                        .as_ref()
                        .is_some_and(|slot| slot.lock().unwrap().cancelled)
                    {
                        return Err(ReadPoolError::TaskCanceled);
                    }
                    admit_read_task(resource_manager.as_ref(), resource_limiter.as_deref()).await?;
                    if cancellation
                        .as_ref()
                        .is_some_and(|slot| slot.lock().unwrap().cancelled)
                    {
                        return Err(ReadPoolError::TaskCanceled);
                    }
                    let mut extras = Extras::new_multilevel(task_id, fixed_level);
                    let task_cell = if let Some(queue) = fair_queue {
                        // Acquisition includes global and RG limits, and may evict only
                        // queued tasks with a strictly lower business priority.
                        let permit = queue
                            .try_acquire(&TaskMetadata::from(metadata.as_slice()))
                            .ok_or(ReadPoolError::UnifiedReadPoolFull)?;
                        extras.set_metadata(permit.metadata(metadata));
                        if let Some(slot) = cancellation {
                            let handle = permit.cancellation();
                            let mut state = slot.lock().unwrap();
                            if state.cancelled {
                                drop(state);
                                handle.cancel();
                                return Err(ReadPoolError::TaskCanceled);
                            }
                            state.handle = Some(handle);
                        }
                        let guard = RunningTaskGuard::new(gauge);
                        let future = async move {
                            let _guard = guard;
                            f.await;
                        };
                        // The permit owns the only new read-pool scheduling charge;
                        // ControlledFuture must not charge the legacy controller too.
                        TaskCell::new(permit.wrap(future), extras)
                    } else {
                        // Preserve the non-RC pool's per-priority capacity semantics.
                        if gauge.get() as usize >= max_tasks {
                            return Err(ReadPoolError::UnifiedReadPoolFull);
                        }
                        extras.set_metadata(metadata);
                        let guard = RunningTaskGuard::new(gauge);
                        TaskCell::new(
                            async move {
                                let _guard = guard;
                                f.await;
                            },
                            extras,
                        )
                    };
                    remote.spawn(task_cell);
                    Ok(())
                }
                .boxed()
            }
        }
    }

    pub fn spawn_handle<F, T>(
        &self,
        f: F,
        priority: CommandPri,
        task_id: u64,
        metadata: TaskMetadata<'_>,
        resource_limiter: Option<Arc<ResourceLimiter>>,
    ) -> impl Future<Output = Result<T, ReadPoolError>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel::<T>();
        let (spawn_fut, cancel_guard) = self.spawn_with_cancellation(
            f.map(move |res| {
                let _ = tx.send(res);
            }),
            priority,
            task_id,
            metadata,
            resource_limiter,
        );
        async move {
            let mut guard = cancel_guard;
            spawn_fut.await?;
            let result = rx.map_err(ReadPoolError::from).await;
            // The task has completed or has already been dropped by the queue.
            guard.disarm();
            result
        }
    }

    pub fn get_normal_pool_size(&self) -> usize {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_normal, ..
            } => read_pool_normal.get_pool_size(),
            ReadPoolHandle::Yatp { pool_size, .. } => *pool_size,
        }
    }

    pub fn get_queue_size_per_worker(&self) -> usize {
        match self {
            ReadPoolHandle::FuturePools {
                read_pool_normal, ..
            } => read_pool_normal.get_running_task_count() / read_pool_normal.get_pool_size(),
            ReadPoolHandle::Yatp {
                running_tasks,
                pool_size,
                ..
            } => running_tasks.iter().map(|r| r.get()).sum::<i64>() as usize / *pool_size,
        }
    }

    pub fn scale_pool_size(&mut self, max_thread_count: usize) {
        match self {
            ReadPoolHandle::FuturePools { .. } => {
                unreachable!()
            }
            ReadPoolHandle::Yatp {
                remote,
                running_threads,
                max_tasks,
                pool_size,
                fair_queue,
                ..
            } => {
                remote.scale_workers(max_thread_count);
                if fair_queue.is_none() {
                    *max_tasks = max_tasks
                        .saturating_div(*pool_size)
                        .saturating_mul(max_thread_count);
                }
                running_threads.set(max_thread_count as i64);
                *pool_size = max_thread_count;
            }
        }
    }

    pub fn set_max_tasks_per_worker(&mut self, tasks_per_thread: usize) {
        match self {
            ReadPoolHandle::FuturePools { .. } => {
                unreachable!()
            }
            ReadPoolHandle::Yatp {
                max_tasks,
                pool_size,
                fair_queue,
                ..
            } => {
                *max_tasks = tasks_per_thread.saturating_mul(*pool_size);
                if let Some(queue) = fair_queue {
                    queue.set_max_tasks(*max_tasks);
                }
            }
        }
    }

    pub fn get_ewma_time_slice(&self) -> Option<Duration> {
        match self {
            ReadPoolHandle::FuturePools { .. } => None,
            ReadPoolHandle::Yatp {
                time_slice_inspector,
                ..
            } => Some(time_slice_inspector.get_ewma_time_slice()),
        }
    }

    pub fn update_ewma_time_slice(&self) {
        if let ReadPoolHandle::Yatp {
            time_slice_inspector,
            ..
        } = self
        {
            time_slice_inspector.update();
        }
    }

    pub fn get_estimated_wait_duration(&self) -> Option<Duration> {
        self.get_ewma_time_slice()
            .map(|s| s * (self.get_queue_size_per_worker() as u32))
    }

    pub fn check_busy_threshold(
        &self,
        busy_threshold: Duration,
    ) -> Result<(), errorpb::ServerIsBusy> {
        if busy_threshold.is_zero() {
            return Ok(());
        }
        let estimated_wait = match self.get_estimated_wait_duration() {
            Some(estimated_wait) if estimated_wait > busy_threshold => estimated_wait,
            _ => return Ok(()),
        };
        // TODO: Get applied_index from the raftstore and check memory locks. Then, we
        // can skip read index in replica read. But now the difficulty is that we don't
        // have access to the the local reader in gRPC threads.
        let mut busy_err = errorpb::ServerIsBusy::default();
        busy_err.set_reason("estimated wait time exceeds threshold".to_owned());
        busy_err.estimated_wait_ms = u32::try_from(estimated_wait.as_millis()).unwrap_or(u32::MAX);
        warn!("Already many pending tasks in the read queue, task is rejected";
            "busy_threshold" => ?&busy_threshold,
            "busy_err" => ?&busy_err,
        );
        Err(busy_err)
    }
}

pub const UPDATE_EWMA_TIME_SLICE_INTERVAL: Duration = Duration::from_millis(200);

pub struct TimeSliceInspector {
    // `atomic_ewma_nanos` is a mirror of `inner.ewma` provided for fast access. It is updated in
    // the `update` method.
    atomic_ewma_nanos: AtomicU64,
    inner: Mutex<TimeSliceInspectorInner>,
}

struct TimeSliceInspectorInner {
    time_slice_hist: [Histogram; 3],
    ewma: Duration,

    last_sum: Duration,
    last_count: u64,
}

impl TimeSliceInspector {
    pub fn new(name: &str) -> Self {
        let time_slice_hist = [
            yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "0"]),
            yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "1"]),
            yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "2"]),
        ];
        let inner = TimeSliceInspectorInner {
            time_slice_hist,
            ewma: Duration::default(),
            last_sum: Duration::default(),
            last_count: 0,
        };
        Self {
            atomic_ewma_nanos: AtomicU64::default(),
            inner: Mutex::new(inner),
        }
    }

    pub fn update(&self) {
        // new_ewma = WEIGHT * new_val + (1 - WEIGHT) * old_ewma
        const WEIGHT: f64 = 0.3;
        // If the accumulated time slice is less than 100ms, the EWMA is not updated.
        const MIN_TIME_DIFF: Duration = Duration::from_millis(100);

        let mut inner = self.inner.lock().unwrap();
        let mut new_sum = Duration::default();
        let mut new_count = 0;
        // Now, we simplify the problem by merging samples from all levels. If we want
        // more accurate answer in the future, calculate for each level separately.
        for hist in &inner.time_slice_hist {
            // Call `metric` to get a consistent snapshot of sum and count.
            let metric_proto = hist.metric();
            let hist_proto = metric_proto.get_histogram();
            new_sum += Duration::from_secs_f64(hist_proto.get_sample_sum());
            new_count += hist_proto.get_sample_count();
        }
        let time_diff = new_sum.saturating_sub(inner.last_sum);
        let count_diff = new_count.saturating_sub(inner.last_count);
        if time_diff < MIN_TIME_DIFF || count_diff == 0 {
            return;
        }
        let new_val = time_diff / ((new_count - inner.last_count) as u32);
        let new_ewma = new_val.mul_f64(WEIGHT) + inner.ewma.mul_f64(1.0 - WEIGHT);
        inner.ewma = new_ewma;
        inner.last_sum = new_sum;
        inner.last_count = new_count;

        self.atomic_ewma_nanos
            .store(new_ewma.as_nanos() as u64, Ordering::Release);
    }

    pub fn get_ewma_time_slice(&self) -> Duration {
        Duration::from_nanos(self.atomic_ewma_nanos.load(Ordering::Acquire))
    }
}

#[derive(Clone)]
pub struct ReporterTicker<R: FlowStatsReporter> {
    reporter: R,
}

impl<R: FlowStatsReporter> PoolTicker for ReporterTicker<R> {
    fn on_tick(&mut self) {
        self.flush_metrics_on_tick();
    }
}

impl<R: FlowStatsReporter> ReporterTicker<R> {
    fn flush_metrics_on_tick(&mut self) {
        crate::storage::metrics::tls_flush(&self.reporter);
        crate::coprocessor::metrics::tls_flush(&self.reporter);
    }
}

#[cfg(test)]
fn get_unified_read_pool_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "unified-read-pool-test-{}",
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(not(test))]
fn get_unified_read_pool_name() -> String {
    UNIFIED_READ_POOL_THREAD.to_string()
}

#[inline]
pub fn build_yatp_read_pool<E: Engine, R: FlowStatsReporter>(
    config: &UnifiedReadPoolConfig,
    reporter: R,
    engine: E,
    resource_manager: Option<Arc<ResourceGroupManager>>,
    cleanup_method: CleanupMethod,
    enable_task_wait_metrics: bool,
) -> ReadPool {
    let unified_read_pool_name = get_unified_read_pool_name();
    build_yatp_read_pool_with_name(
        config,
        reporter,
        engine,
        resource_manager,
        cleanup_method,
        unified_read_pool_name,
        enable_task_wait_metrics,
    )
}

pub fn build_yatp_read_pool_with_name<E: Engine, R: FlowStatsReporter>(
    config: &UnifiedReadPoolConfig,
    reporter: R,
    engine: E,
    resource_manager: Option<Arc<ResourceGroupManager>>,
    cleanup_method: CleanupMethod,
    unified_read_pool_name: String,
    enable_task_wait_metrics: bool,
) -> ReadPool {
    let raftkv = Arc::new(Mutex::new(engine));
    let builder = YatpPoolBuilder::new(ReporterTicker { reporter })
        .name_prefix(&unified_read_pool_name)
        .cleanup_method(cleanup_method)
        .stack_size(config.stack_size.0 as usize)
        .thread_count(
            1, // min_thread_count is controlled by readPoolConfigRunner
            config.max_thread_count,
            std::cmp::max(
                std::cmp::max(
                    UNIFIED_READPOOL_MIN_CONCURRENCY,
                    SysQuota::cpu_cores_quota() as usize,
                ),
                config.max_thread_count,
            ),
        )
        .after_start(move || {
            let engine = raftkv.lock().unwrap().clone();
            set_tls_engine(engine);
            set_io_type(IoType::ForegroundRead);
        })
        .before_stop(|| unsafe {
            destroy_tls_engine::<E>();
        })
        .enable_task_wait_metrics(enable_task_wait_metrics);

    let max_tasks = config
        .max_tasks_per_worker
        .saturating_mul(config.max_thread_count);
    let fair_queue = resource_manager.as_ref().map(|manager| {
        if config.cpu_threshold != 0.0 {
            warn!("readpool.unified.cpu-threshold is ignored with resource control; use read-cpu-ratio");
        }
        FairQueue::new(
            manager.clone(),
            FairQueueConfig {
                max_tasks,
                max_tasks_per_group: config.max_tasks_per_resource_group,
                cpu_cores: SysQuota::cpu_cores_quota(),
                cpu_ratio: config.read_cpu_ratio,
                slice_precharge: config.slice_precharge.0,
                max_time_slice: config.max_time_slice.0,
            },
        )
    });
    let pool = if let Some(queue) = fair_queue.as_ref() {
        builder.build_fair_pool(queue.clone())
    } else {
        builder.build_multi_level_pool()
    };
    let time_slice_inspector = Arc::new(TimeSliceInspector::new(&unified_read_pool_name));
    ReadPool::Yatp {
        pool,
        running_tasks: TaskPriority::priorities().map(|p| {
            UNIFIED_READ_POOL_RUNNING_TASKS
                .with_label_values(&[&unified_read_pool_name, p.as_str()])
        }),
        running_threads: {
            let running_threads =
                UNIFIED_READ_POOL_RUNNING_THREADS.with_label_values(&[&unified_read_pool_name]);
            running_threads.set(config.max_thread_count as _);
            running_threads
        },
        max_tasks,
        pool_size: config.max_thread_count,
        fair_queue,
        resource_manager,
        time_slice_inspector,
    }
}

impl From<Vec<FuturePool>> for ReadPool {
    fn from(mut v: Vec<FuturePool>) -> ReadPool {
        assert_eq!(v.len(), 3);
        let read_pool_high = v.remove(2);
        let read_pool_normal = v.remove(1);
        let read_pool_low = v.remove(0);
        ReadPool::FuturePools {
            read_pool_high,
            read_pool_normal,
            read_pool_low,
        }
    }
}

struct ReadPoolCpuTimeTracker {
    yatp_total_time_elapsed: IntCounter,
    // the total time duration of each thread busy with handling tasks. This time also includes
    // the time when the threads are off-cpu, so it might be much higher than the actual cpu time.
    prev_total_task_handling_time_us: u64,
    prev_thread_check_time: Instant,
    prev_thread_usage_per_sec: f64,
    prev_total_cpu_time: u64,
    prev_cpu_check_time: Instant,
    prev_cpu_usage_per_second: f64,
}

impl ReadPoolCpuTimeTracker {
    fn new(pool_name: &str) -> Self {
        let now = Instant::now_coarse();
        let yatp_total_time_elapsed = MULTILEVEL_LEVEL_ELAPSED
            .get_metric_with_label_values(&[pool_name, "total"])
            .unwrap();
        let prev_total_task_handling_time_us = yatp_total_time_elapsed.get();

        Self {
            yatp_total_time_elapsed,
            prev_total_task_handling_time_us,
            prev_thread_check_time: now,
            prev_thread_usage_per_sec: 0.0,
            prev_total_cpu_time: 0,
            prev_cpu_check_time: now,
            prev_cpu_usage_per_second: 0.0,
        }
    }

    /// Get actual CPU usage of unified read pool threads using kernel thread
    /// stats.
    fn get_unified_read_pool_cpu(&mut self) -> f64 {
        #[cfg(test)]
        {
            // In test mode, return the manually set value if available
            if self.prev_cpu_usage_per_second > 0.0 {
                return self.prev_cpu_usage_per_second;
            }
        }
        use tikv_util::sys::thread::{full_thread_stat, ticks_per_second};

        let check_time = Instant::now_coarse();
        let duration = check_time.saturating_duration_since(self.prev_cpu_check_time);

        // Minimum duration check to avoid noise - if too soon, return prev value
        if duration < Duration::from_millis(500) {
            return self.prev_cpu_usage_per_second;
        }

        let mut current_total_cpu_time = 0i64;
        let pid = tikv_util::sys::thread::process_id();
        let tids: Vec<_> = tikv_util::sys::thread::thread_ids(pid).unwrap();
        // Collect CPU stats for each cached read pool worker thread
        for &tid in &tids {
            if let Ok(stat) = full_thread_stat(pid, tid) {
                // Look for unified read pool thread name pattern
                if matches_thread_name_prefix(&stat.command, UNIFIED_READ_POOL_THREAD) {
                    // Sum utime + stime (user + system time)
                    current_total_cpu_time += stat.utime + stat.stime;
                }
            }
        }

        // Calculate CPU time difference since last check
        let cpu_time_diff = current_total_cpu_time.saturating_sub(self.prev_total_cpu_time as i64);

        let cpu_utilization = if duration.as_secs_f64() > 0.0 && cpu_time_diff > 0 {
            // Convert CPU time to seconds using ticks_per_second
            let cpu_seconds = (cpu_time_diff as f64) / (ticks_per_second() as f64);
            let wall_seconds = duration.as_secs_f64();
            cpu_seconds / wall_seconds
        } else {
            0.0
        };

        self.prev_total_cpu_time = current_total_cpu_time as u64;
        self.prev_cpu_check_time = check_time;
        self.prev_cpu_usage_per_second = cpu_utilization;

        cpu_utilization
    }

    #[cfg(test)]
    fn set_test_cpu_utilization(&mut self, cpu: f64) {
        self.prev_cpu_usage_per_second = cpu;
    }

    /// Baseline thread usage measurement using yatp metrics (includes off-CPU
    /// time)
    fn prev_avg_thread_usage(&mut self) -> f64 {
        let check_time = Instant::now_coarse();
        let duration = check_time.saturating_duration_since(self.prev_thread_check_time);
        // if the check duration is too small, just return the latest cached value.
        if duration < Duration::from_millis(100) {
            return self.prev_thread_usage_per_sec;
        }
        let total_thread_time = self.yatp_total_time_elapsed.get();
        let total_thread_usage_per_sec = (total_thread_time - self.prev_total_task_handling_time_us)
            as f64
            / duration.as_micros() as f64;
        self.prev_total_task_handling_time_us = total_thread_time;
        self.prev_thread_check_time = check_time;
        self.prev_thread_usage_per_sec = total_thread_usage_per_sec;
        total_thread_usage_per_sec
    }
}
struct ReadPoolConfigRunner {
    interval: Duration,
    sender: SyncSender<usize>,
    handle: ReadPoolHandle,
    cpu_time_tracker: ReadPoolCpuTimeTracker,
    process_stats: ProcessStat,
    min_thread_count: usize,
    // configed thread pool size, it's the min thread count to be scale. It is set to
    // max_thread_count
    core_thread_count: usize,
    // the max thread count can be scaled
    max_thread_count: usize,
    // the current active thread count
    cur_thread_count: usize,
    auto_adjust: bool,
    // CPU threshold from configuration (0 means disabled, RFC 0114)
    cpu_threshold: f64,
}

impl Runnable for ReadPoolConfigRunner {
    type Task = Task;
    fn run(&mut self, task: Self::Task) {
        match task {
            Task::PoolSize(s) => {
                if s != self.core_thread_count {
                    let fair_tasks_per_worker = match &self.handle {
                        ReadPoolHandle::Yatp {
                            max_tasks,
                            fair_queue: Some(_),
                            ..
                        } => Some(max_tasks.saturating_div(self.core_thread_count)),
                        _ => None,
                    };
                    self.handle.scale_pool_size(s);
                    // Explicit configuration changes update shared capacity;
                    // transient worker scaling does not shrink the RC limit.
                    if let Some(tasks_per_worker) = fair_tasks_per_worker {
                        self.handle.set_max_tasks_per_worker(tasks_per_worker);
                    }
                    self.core_thread_count = s;
                    self.cur_thread_count = s;
                    self.notify_pool_size_change(s);
                }
            }
            Task::AutoAdjust(s) => {
                self.auto_adjust = s;
            }
            Task::MaxTasksPerWorker(s) => {
                self.handle.set_max_tasks_per_worker(s);
            }
            Task::CpuThreshold(s) => {
                self.cpu_threshold = s;
                if matches!(
                    &self.handle,
                    ReadPoolHandle::Yatp {
                        fair_queue: Some(_),
                        ..
                    }
                ) {
                    warn!(
                        "readpool.unified.cpu-threshold is ignored with resource control; use read-cpu-ratio"
                    );
                }
            }
        }
    }
}

impl RunnableWithTimer for ReadPoolConfigRunner {
    fn get_interval(&self) -> Duration {
        self.interval
    }

    fn on_timeout(&mut self) {
        self.adjust_pool_size();
    }
}

impl ReadPoolConfigRunner {
    fn running_tasks(&self) -> i64 {
        match &self.handle {
            ReadPoolHandle::Yatp { running_tasks, .. } => {
                running_tasks.iter().map(|r| r.get()).sum()
            }
            _ => unreachable!(),
        }
    }

    // Adjust pool size using based on thread utilization or cpu utilization.
    fn adjust_pool_size(&mut self) {
        // The shared CPU gate owns the RC read budget. Keep enough configured
        // workers to use that budget instead of interpreting gate-induced idle
        // time as a reason to repeatedly shrink the pool.
        let fair_cpu_budget = match &self.handle {
            ReadPoolHandle::Yatp {
                fair_queue: Some(queue),
                ..
            } => Some(queue.cpu_budget_cores()),
            _ => None,
        };
        let read_pool_cpu = self.cpu_time_tracker.get_unified_read_pool_cpu();
        let target_cpu_cores = fair_cpu_budget.unwrap_or_else(|| {
            if self.cpu_threshold > 0.0 {
                self.cpu_threshold * SysQuota::cpu_cores_quota()
            } else {
                SysQuota::cpu_cores_quota()
            }
        });
        // Keep observability current even when worker autoscaling is disabled.
        // In RC mode the target reflects the queue's fixed CPU gate budget;
        // this periodic measurement does not adjust that budget.
        READ_POOL_CPU_VEC
            .with_label_values(&["current"])
            .set(read_pool_cpu * 100.0);
        READ_POOL_CPU_VEC
            .with_label_values(&["target"])
            .set(target_cpu_cores * 100.0);
        if !self.auto_adjust || fair_cpu_budget.is_some() {
            self.reset_thread_count();
            return;
        }

        let thread_usage = self.cpu_time_tracker.prev_avg_thread_usage();
        let running_tasks = self.running_tasks();
        let process_cpu = match self.process_stats.cpu_usage() {
            Ok(p) => p,
            Err(e) => {
                warn!("fetch process cpu usage failed"; "err" => ?e);
                return;
            }
        };

        // Thread-utilization ladder (process CPU, thread usage, queue depth).
        let busy_thread_scale_out = self.cur_thread_count < self.max_thread_count
            && process_cpu * (self.cur_thread_count as f64 + 1.0) / (self.cur_thread_count as f64)
                < target_cpu_cores
            && thread_usage > self.cur_thread_count as f64 * READ_POOL_THREAD_HIGH_THRESHOLD
            && running_tasks > self.cur_thread_count as i64 * RUNNING_TASKS_PER_THREAD_THRESHOLD;

        let busy_thread_scale_in = self.cur_thread_count > self.min_thread_count
            && thread_usage < (self.cur_thread_count - 1) as f64 * READ_POOL_THREAD_LOW_THRESHOLD
            && running_tasks < self.cur_thread_count as i64 * RUNNING_TASKS_PER_THREAD_THRESHOLD;

        let leeway = 0.1;
        let busy_cpu_scale_in = read_pool_cpu > (leeway + 1.0) * target_cpu_cores;
        let busy_cpu_scale_out = read_pool_cpu < (1.0 - leeway) * target_cpu_cores
            && self.cur_thread_count < self.core_thread_count;

        let new_thread_count = if busy_cpu_scale_in {
            // CPU threshold takes precedence over busy thread scaling conditions
            std::cmp::max(
                std::cmp::max(
                    (target_cpu_cores).floor() as usize,
                    ((self.cur_thread_count as f64) * target_cpu_cores / read_pool_cpu).floor()
                        as usize,
                ),
                1, // minimum 1 running thread
            )
        } else if busy_cpu_scale_out {
            self.cur_thread_count + 1
        } else if busy_thread_scale_in {
            self.cur_thread_count - 1
        } else if busy_thread_scale_out {
            self.cur_thread_count + 1
        } else {
            self.cur_thread_count
        };

        self.set_thread_count(new_thread_count);
    }

    /// Resizes the pool, no-op if it is already at `new_thread_count`.
    fn set_thread_count(&mut self, new_thread_count: usize) {
        if new_thread_count != self.cur_thread_count {
            self.handle.scale_pool_size(new_thread_count);
            self.notify_pool_size_change(new_thread_count);
            self.cur_thread_count = new_thread_count;
        }
    }

    /// Restores the pool to its configured size, used when auto-adjustment is
    /// turned off and the ladder is no longer allowed to move it.
    fn reset_thread_count(&mut self) {
        self.set_thread_count(self.core_thread_count);
    }

    fn notify_pool_size_change(&self, new_thread_count: usize) {
        // it's unlikely to send failed.
        if let Err(e) = self.sender.try_send(new_thread_count) {
            warn!("notify read pool thread count change failed"; "err" => ?e);
        }
    }
}

enum Task {
    PoolSize(usize),
    AutoAdjust(bool),
    MaxTasksPerWorker(usize),
    CpuThreshold(f64),
}

impl std::fmt::Display for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Task::PoolSize(s) => write!(f, "PoolSize({})", *s),
            Task::AutoAdjust(s) => write!(f, "AutoAdjust({})", *s),
            Task::MaxTasksPerWorker(s) => write!(f, "MaxTasksPerWorker({})", *s),
            Task::CpuThreshold(s) => write!(f, "CpuThreshold({})", *s),
        }
    }
}

pub struct ReadPoolConfigManager {
    scheduler: Scheduler<Task>,
}

impl ReadPoolConfigManager {
    pub fn new(
        handle: ReadPoolHandle,
        sender: SyncSender<usize>,
        worker: &Worker,
        min_thread_count: usize,
        max_thread_count: usize,
        auto_adjust: bool,
        cpu_threshold: f64,
    ) -> Self {
        let runner = ReadPoolConfigRunner {
            interval: READ_POOL_THREAD_CHECK_DURATION,
            sender,
            handle,
            cpu_time_tracker: ReadPoolCpuTimeTracker::new(&get_unified_read_pool_name()),
            process_stats: ProcessStat::cur_proc_stat().unwrap(),
            min_thread_count,
            core_thread_count: max_thread_count,
            cur_thread_count: max_thread_count,
            max_thread_count,
            auto_adjust,
            cpu_threshold,
        };
        let scheduler = worker.start_with_timer("read-pool-config-worker", runner);

        Self { scheduler }
    }
}

impl Drop for ReadPoolConfigManager {
    fn drop(&mut self) {
        self.scheduler.stop();
    }
}

impl ConfigManager for ReadPoolConfigManager {
    fn dispatch(&mut self, change: ConfigChange) -> CfgResult<()> {
        if let Some(ConfigValue::Module(unified)) = change.get("unified") {
            if let Some(ConfigValue::Usize(max_thread_count)) = unified.get("max_thread_count") {
                self.scheduler.schedule(Task::PoolSize(*max_thread_count))?;
            }
            if let Some(ConfigValue::Bool(b)) = unified.get("auto_adjust_pool_size") {
                self.scheduler.schedule(Task::AutoAdjust(*b))?;
            }
            if let Some(ConfigValue::Usize(max_tasks)) = unified.get("max_tasks_per_worker") {
                self.scheduler
                    .schedule(Task::MaxTasksPerWorker(*max_tasks))?;
            }
            if let Some(ConfigValue::F64(cpu_threshold)) = unified.get("cpu_threshold") {
                self.scheduler
                    .schedule(Task::CpuThreshold(*cpu_threshold))?;
            }
        }
        info!(
            "readpool config changed";
            "change" => ?change,
        );
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ReadPoolError {
    #[error("{0}")]
    FuturePoolFull(#[from] yatp_pool::Full),

    #[error("Unified read pool is full")]
    UnifiedReadPoolFull,

    #[error("Request rejected by admission control")]
    Rejected,

    #[error("Read pool task was canceled before submission")]
    TaskCanceled,

    #[error("{0}")]
    Canceled(#[from] oneshot::Canceled),
}

mod metrics {
    use lazy_static::lazy_static;
    use prometheus::*;

    lazy_static! {
        pub static ref UNIFIED_READ_POOL_RUNNING_TASKS: IntGaugeVec = register_int_gauge_vec!(
            "tikv_unified_read_pool_running_tasks",
            "The number of running tasks in the unified read pool",
            &["name", "priority"]
        )
        .unwrap();
        pub static ref UNIFIED_READ_POOL_RUNNING_THREADS: IntGaugeVec = register_int_gauge_vec!(
            "tikv_unified_read_pool_thread_count",
            "The number of running threads in the unified read pool",
            &["name"]
        )
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use std::{thread, time::Duration};

    use futures::channel::oneshot;
    use futures_executor::block_on;
    use kvproto::kvrpcpb::ResourceControlContext;
    use raftstore::store::{ReadStats, WriteStats};
    use resource_control::ResourceGroupManager;

    use super::*;
    use crate::storage::TestEngineBuilder;

    #[derive(Clone)]
    struct DummyReporter;

    impl FlowStatsReporter for DummyReporter {
        fn report_read_stats(&self, _read_stats: ReadStats) {}
        fn report_write_stats(&self, _write_stats: WriteStats) {}
    }

    #[test]
    fn test_yatp_full() {
        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            max_tasks_per_worker: 1,
            ..Default::default()
        };
        // max running tasks number should be 2*1 = 2

        let engine = TestEngineBuilder::new().build().unwrap();
        let name = "test-yatp-full";
        let pool = build_yatp_read_pool_with_name(
            &config,
            DummyReporter,
            engine,
            None,
            CleanupMethod::InPlace,
            name.to_owned(),
            false,
        );

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let handle = pool.handle();
        let (task1, tx1) = gen_task();
        let (task2, _tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();

        block_on(handle.spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None))
            .unwrap();
        block_on(handle.spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None))
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match block_on(handle.spawn(task3, CommandPri::Normal, 3, TaskMetadata::default(), None)) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
        tx1.send(()).unwrap();

        thread::sleep(Duration::from_millis(300));
        block_on(handle.spawn(task4, CommandPri::Normal, 4, TaskMetadata::default(), None))
            .unwrap();
        assert_eq!(
            UNIFIED_READ_POOL_RUNNING_TASKS
                .with_label_values(&[name, "medium"])
                .get(),
            2
        );
    }

    #[test]
    fn test_yatp_scale_up() {
        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            max_tasks_per_worker: 1,
            ..Default::default()
        };
        // max running tasks number should be 2*1 = 2

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            engine,
            None,
            CleanupMethod::InPlace,
            false,
        );

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let mut handle = pool.handle();
        let (task1, _tx1) = gen_task();
        let (task2, _tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();
        let (task5, _tx5) = gen_task();

        block_on(handle.spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None))
            .unwrap();
        block_on(handle.spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None))
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match block_on(handle.spawn(task3, CommandPri::Normal, 3, TaskMetadata::default(), None)) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }

        handle.scale_pool_size(3);
        assert_eq!(handle.get_normal_pool_size(), 3);

        block_on(handle.spawn(task4, CommandPri::Normal, 4, TaskMetadata::default(), None))
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match block_on(handle.spawn(task5, CommandPri::Normal, 5, TaskMetadata::default(), None)) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
    }

    #[test]
    fn test_yatp_scale_down() {
        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            max_tasks_per_worker: 1,
            ..Default::default()
        };
        // max running tasks number for each priority should be 2*1 = 2

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            engine,
            None,
            CleanupMethod::InPlace,
            false,
        );

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let mut handle = pool.handle();
        let (task1, tx1) = gen_task();
        let (task2, tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();
        let (task5, _tx5) = gen_task();

        block_on(handle.spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None))
            .unwrap();
        block_on(handle.spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None))
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match block_on(handle.spawn(task3, CommandPri::Normal, 3, TaskMetadata::default(), None)) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }

        // spawn a high-priority task, should not return Full error.
        let (task_high, tx_h) = gen_task();
        let mut ctx = ResourceControlContext::default();
        ctx.override_priority = 16; // high priority
        let metadata = TaskMetadata::from_ctx(&ctx);
        let f = handle.spawn_handle(task_high, CommandPri::Normal, 6, metadata, None);
        tx_h.send(()).unwrap();
        block_on(f).unwrap();

        tx1.send(()).unwrap();
        tx2.send(()).unwrap();
        thread::sleep(Duration::from_millis(300));

        handle.scale_pool_size(1);
        assert_eq!(handle.get_normal_pool_size(), 1);

        block_on(handle.spawn(task4, CommandPri::Normal, 4, TaskMetadata::default(), None))
            .unwrap();

        thread::sleep(Duration::from_millis(300));
        match block_on(handle.spawn(task5, CommandPri::Normal, 5, TaskMetadata::default(), None)) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            _ => panic!("should return full error"),
        }
    }

    #[test]
    fn test_time_slice_inspector_ewma() {
        const MARGIN: f64 = 1e-5; // 10us

        let name = "test_time_slice_inspector_ewma";
        let inspector = TimeSliceInspector::new(name);
        let hist = yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, "0"]);

        // avg: 0.055, prev_ewma: 0 => new_ewma = 0.0165
        for i in 1..=10 {
            hist.observe(i as f64 * 0.01);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.0165).abs() < MARGIN);

        // avg: 0.0125, prev_ewma: 0.0165 => new_ewma = 0.0153
        for i in 5..=20 {
            hist.observe(i as f64 * 0.001);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.0153).abs() < MARGIN);

        // sum: 55ms, don't update ewma
        for i in 1..=10 {
            hist.observe(i as f64 * 0.001);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.0153).abs() < MARGIN);

        // avg: 0.00786, prev_ewma: 0.0153 => new_ewma = 0.01307
        for i in 5..=15 {
            hist.observe(i as f64 * 0.001);
        }
        inspector.update();
        let ewma = inspector.get_ewma_time_slice().as_secs_f64();
        assert!((ewma - 0.01307).abs() < MARGIN);
    }

    #[test]
    fn test_config_validation_cpu_threshold() {
        use tikv_util::config::ReadableSize;
        // Valid cpu_threshold
        let valid_config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            stack_size: ReadableSize::mb(2),
            max_tasks_per_worker: 2,
            auto_adjust_pool_size: true,
            cpu_threshold: 0.7,
            ..Default::default()
        };
        // Just verify config can be created
        assert_eq!(valid_config.cpu_threshold, 0.7);

        // Test disabled threshold (0.0)
        let disabled_config = UnifiedReadPoolConfig {
            cpu_threshold: 0.0,
            ..valid_config
        };
        assert_eq!(disabled_config.cpu_threshold, 0.0);
    }

    #[test]
    fn test_cpu_threshold_scale_down_and_up() {
        use tikv_util::worker::Worker;

        let min_thread_count = (0.8 * SysQuota::cpu_cores_quota()) as usize;
        let max_thread_count = SysQuota::cpu_cores_quota() as usize;
        let config = UnifiedReadPoolConfig {
            min_thread_count,
            max_thread_count,
            max_tasks_per_worker: 4,
            cpu_threshold: 0.6, // 60% threshold
            auto_adjust_pool_size: true,
            ..Default::default()
        };

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            engine,
            None,
            CleanupMethod::InPlace,
            false,
        );

        let handle = pool.handle();
        let worker = Worker::new("test-worker");

        // Create ReadPoolConfigRunner with a real CPU tracker first
        let mut runner = ReadPoolConfigRunner {
            interval: Duration::from_secs(10),
            sender: std::sync::mpsc::sync_channel(10).0,
            handle: handle.clone(),
            cpu_time_tracker: ReadPoolCpuTimeTracker::new("test-pool"),
            process_stats: ProcessStat::cur_proc_stat().unwrap(),
            min_thread_count: config.min_thread_count,
            core_thread_count: config.min_thread_count,
            cur_thread_count: config.min_thread_count,
            max_thread_count: config.max_thread_count,
            auto_adjust: true,
            cpu_threshold: config.cpu_threshold,
        };

        assert_eq!(runner.cur_thread_count, min_thread_count);

        // Test 1: Set high CPU utilization using test helper
        runner
            .cpu_time_tracker
            .set_test_cpu_utilization(0.8 * SysQuota::cpu_cores_quota());

        let initial_threads = runner.cur_thread_count;
        runner.adjust_pool_size(); // Call the REAL adjust_pool_size method

        assert!(
            runner.cur_thread_count < initial_threads,
            "Thread count should decrease when CPU usage is high. Before: {}, After: {}",
            initial_threads,
            runner.cur_thread_count
        );

        // Test 2: Set low CPU utilization to test scale up
        runner
            .cpu_time_tracker
            .set_test_cpu_utilization(0.3 * num_cpus::get() as f64);
        let before_scale_up = runner.cur_thread_count;

        runner.adjust_pool_size();

        // Should not scale down further when CPU is low
        if before_scale_up < runner.core_thread_count {
            assert!(
                runner.cur_thread_count >= before_scale_up,
                "Thread count should not decrease further when CPU is low"
            );
        }

        worker.stop();
    }

    fn test_runner(
        auto_adjust: bool,
        resource_manager: Option<Arc<ResourceGroupManager>>,
    ) -> (ReadPoolConfigRunner, tikv_util::worker::Worker) {
        use tikv_util::worker::Worker;

        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 8,
            max_tasks_per_worker: 4,
            auto_adjust_pool_size: auto_adjust,
            ..Default::default()
        };
        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            engine,
            resource_manager,
            CleanupMethod::InPlace,
            false,
        );
        let handle = pool.handle();
        let worker = Worker::new("test-worker");
        let runner = ReadPoolConfigRunner {
            interval: Duration::from_secs(10),
            sender: std::sync::mpsc::sync_channel(10).0,
            handle,
            cpu_time_tracker: ReadPoolCpuTimeTracker::new("test-pool"),
            process_stats: ProcessStat::cur_proc_stat().unwrap(),
            min_thread_count: config.min_thread_count,
            core_thread_count: 8,
            cur_thread_count: 8,
            max_thread_count: config.max_thread_count,
            auto_adjust,
            cpu_threshold: config.cpu_threshold,
        };
        (runner, worker)
    }

    #[test]
    fn test_raw_auto_scaling_without_resource_manager() {
        // Auto-adjust on with no resource manager still runs the original
        // thread-utilization ladder; with it off the pool must not move. Same
        // conditions either way, so the only variable is the gate itself.
        let (mut runner, worker) = test_runner(true, None);
        let before = runner.cur_thread_count;
        runner.adjust_pool_size();
        assert_eq!(
            runner.cur_thread_count,
            before - 1,
            "an idle pool should scale in by one step"
        );
        worker.stop();

        let (mut runner, worker) = test_runner(false, None);
        let before = runner.cur_thread_count;
        runner.adjust_pool_size();
        assert_eq!(
            runner.cur_thread_count, before,
            "auto-adjust off must leave the pool at its configured size"
        );
        worker.stop();
    }

    #[test]
    fn test_admission_future_preserves_request_tracker() {
        for controlled in [false, true] {
            let config = UnifiedReadPoolConfig {
                max_thread_count: 1,
                ..Default::default()
            };
            let manager = controlled.then(|| Arc::new(ResourceGroupManager::default()));
            let pool = build_yatp_read_pool(
                &config,
                DummyReporter,
                TestEngineBuilder::new().build().unwrap(),
                manager,
                CleanupMethod::InPlace,
                false,
            );
            let handle = pool.handle();
            let token =
                tracker::GLOBAL_TRACKERS.insert(tracker::Tracker::new(tracker::RequestInfo::new(
                    &kvproto::kvrpcpb::Context::default(),
                    tracker::RequestType::Unknown,
                    0,
                )));
            tracker::set_tls_tracker_token(token);
            let task = handle.spawn_handle(
                async { tracker::get_tls_tracker_token() },
                CommandPri::Normal,
                1,
                TaskMetadata::default(),
                None,
            );
            tracker::clear_tls_tracker_token();
            let observed = match block_on(futures::future::select(
                Box::pin(task),
                Box::pin(futures_timer::Delay::new(Duration::from_secs(10))),
            )) {
                futures::future::Either::Left((result, _)) => result.unwrap(),
                futures::future::Either::Right(((), _pending_task)) => {
                    panic!(
                        "tracker propagation task did not finish within 10s: controlled={controlled}"
                    );
                }
            };
            tracker::GLOBAL_TRACKERS.remove(token);
            assert_eq!(observed, token);
            drop(handle);
            drop(pool);
        }
    }

    #[test]
    fn test_fair_pool_keeps_configured_workers() {
        let manager = Arc::new(ResourceGroupManager::default());
        let (mut runner, worker) = test_runner(true, Some(manager));
        runner.cpu_threshold = 0.01;
        runner.cpu_time_tracker.set_test_cpu_utilization(100.0);
        // Neither a low legacy CPU threshold nor apparent idle time caused by
        // the CPU gate should change this pool's configured parallelism.
        for _ in 0..3 {
            runner.adjust_pool_size();
            assert_eq!(runner.cur_thread_count, runner.core_thread_count);
        }
        worker.stop();
    }

    #[test]
    fn test_cancellation_guard_dropped_before_admission() {
        for controlled in [false, true] {
            let config = UnifiedReadPoolConfig {
                max_thread_count: 1,
                ..Default::default()
            };
            let pool = build_yatp_read_pool(
                &config,
                DummyReporter,
                TestEngineBuilder::new().build().unwrap(),
                controlled.then(|| Arc::new(ResourceGroupManager::default())),
                CleanupMethod::InPlace,
                false,
            );
            let handle = pool.handle();
            let (tx, rx) = oneshot::channel();
            let (admission, guard) = handle.spawn_with_cancellation(
                async move {
                    let _ = tx.send(());
                },
                CommandPri::Normal,
                1,
                TaskMetadata::default(),
                None,
            );
            drop(guard);
            if controlled {
                assert!(matches!(
                    block_on(admission),
                    Err(ReadPoolError::TaskCanceled)
                ));
                assert!(block_on(rx).is_err());
                if let ReadPoolHandle::Yatp {
                    fair_queue: Some(queue),
                    ..
                } = handle
                {
                    assert_eq!(queue.snapshot().live_tasks, 0);
                    assert_eq!(queue.snapshot().queued, 0);
                }
            } else {
                // The public guard must not alter non-RC detached-task behavior.
                block_on(admission).unwrap();
                block_on(rx).unwrap();
            }
        }
    }

    #[test]
    fn test_fair_pool_cancel_queued_task_releases_capacity() {
        let config = UnifiedReadPoolConfig {
            max_thread_count: 1,
            max_tasks_per_worker: 2,
            ..Default::default()
        };
        let manager = Arc::new(ResourceGroupManager::default());
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            TestEngineBuilder::new().build().unwrap(),
            Some(manager),
            CleanupMethod::InPlace,
            false,
        );
        let handle = pool.handle();
        let queue = match &handle {
            ReadPoolHandle::Yatp {
                fair_queue: Some(queue),
                ..
            } => queue.clone(),
            _ => unreachable!(),
        };
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        block_on(handle.spawn(
            async move {
                started_tx.send(()).unwrap();
                // Bound even the failure path so a failed assertion cannot
                // leave pool shutdown waiting on a blocked test worker.
                let _ = release_rx.recv_timeout(Duration::from_secs(5));
            },
            CommandPri::Normal,
            1,
            TaskMetadata::default(),
            None,
        ))
        .unwrap();
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let mut result = Box::pin(handle.spawn_handle(
            futures::future::pending::<()>(),
            CommandPri::Normal,
            2,
            TaskMetadata::default(),
            None,
        ));
        let first_poll = block_on(futures::future::poll_fn(|cx| {
            std::task::Poll::Ready(result.as_mut().poll(cx))
        }));
        assert!(first_poll.is_pending());
        assert_eq!(queue.snapshot().live_tasks, 2);
        assert_eq!(queue.snapshot().queued, 1);
        drop(result);
        // Caller cancellation removes queued work immediately, without waiting
        // for another worker dispatch or for the global CPU gate to reopen.
        assert_eq!(queue.snapshot().live_tasks, 1);
        assert_eq!(queue.snapshot().queued, 0);
        let running = match &handle {
            ReadPoolHandle::Yatp { running_tasks, .. } => {
                running_tasks.iter().map(|g| g.get()).sum::<i64>()
            }
            _ => unreachable!(),
        };
        assert_eq!(running, 1);
        release_tx.send(()).unwrap();
    }

    #[test]
    fn test_fair_pool_capacity_update_reaches_existing_handles() {
        let config = UnifiedReadPoolConfig {
            max_thread_count: 1,
            max_tasks_per_worker: 2,
            ..Default::default()
        };
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            TestEngineBuilder::new().build().unwrap(),
            Some(Arc::new(ResourceGroupManager::default())),
            CleanupMethod::InPlace,
            false,
        );
        let existing_handle = pool.handle();
        let mut config_handle = pool.handle();
        config_handle.set_max_tasks_per_worker(1);
        let (tx1, rx1) = oneshot::channel::<()>();
        block_on(existing_handle.spawn(
            async move {
                let _ = rx1.await;
            },
            CommandPri::Normal,
            1,
            TaskMetadata::default(),
            None,
        ))
        .unwrap();
        assert!(matches!(
            block_on(existing_handle.spawn(
                async {},
                CommandPri::Normal,
                2,
                TaskMetadata::default(),
                None,
            )),
            Err(ReadPoolError::UnifiedReadPoolFull)
        ));
        config_handle.set_max_tasks_per_worker(2);
        let (tx2, rx2) = oneshot::channel::<()>();
        block_on(existing_handle.spawn(
            async move {
                let _ = rx2.await;
            },
            CommandPri::Normal,
            3,
            TaskMetadata::default(),
            None,
        ))
        .unwrap();
        let queue = match &existing_handle {
            ReadPoolHandle::Yatp {
                fair_queue: Some(queue),
                ..
            } => queue,
            _ => unreachable!(),
        };
        assert_eq!(queue.snapshot().live_tasks, 2);
        drop((tx1, tx2));
    }

    #[test]
    fn test_yatp_task_poll_duration_metric() {
        let count_metric = |name: &str| -> u64 {
            let mut sum = 0;
            for i in 0..=2 {
                let hist =
                    yatp::metrics::TASK_POLL_DURATION.with_label_values(&[name, &format!("{}", i)]);
                sum += hist.get_sample_count();
            }
            sum
        };

        for control in [false, true] {
            let name = format!("test_yatp_task_poll_duration_metric_{}", control);
            let resource_manager = control.then(|| Arc::new(ResourceGroupManager::default()));
            let config = UnifiedReadPoolConfig {
                min_thread_count: 1,
                max_thread_count: 2,
                max_tasks_per_worker: 1,
                ..Default::default()
            };

            let engine = TestEngineBuilder::new().build().unwrap();

            let pool = build_yatp_read_pool_with_name(
                &config,
                DummyReporter,
                engine,
                resource_manager,
                CleanupMethod::InPlace,
                name.clone(),
                false,
            );

            let gen_task = || {
                let (tx, rx) = oneshot::channel::<()>();
                let task = async move {
                    // sleep the thread 100ms to trigger flushing the metrics.
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    let _ = rx.await;
                };
                (task, tx)
            };

            let handle = pool.handle();
            let (task1, tx1) = gen_task();
            let (task2, tx2) = gen_task();

            block_on(handle.spawn(task1, CommandPri::Normal, 1, TaskMetadata::default(), None))
                .unwrap();
            block_on(handle.spawn(task2, CommandPri::Normal, 2, TaskMetadata::default(), None))
                .unwrap();

            tx1.send(()).unwrap();
            tx2.send(()).unwrap();

            // Worker startup and metric flushing are asynchronous. A fixed
            // sleep flakes when the other pool tests saturate the host.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while count_metric(&name) < 2 && std::time::Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            assert_eq!(count_metric(&name), 2, "pool={name}");
            drop(pool);
        }
    }

    // Duplicated from resource_control::resource_group::tests which is
    // #[cfg(test)] pub(crate) and not accessible from this crate.
    fn new_resource_group_ru(
        name: String,
        ru: u64,
        group_priority: u32,
    ) -> kvproto::resource_manager::ResourceGroup {
        use kvproto::resource_manager::{GroupMode, GroupRequestUnitSettings, ResourceGroup};
        let mut group = ResourceGroup::new();
        group.set_name(name);
        group.set_mode(GroupMode::RuMode);
        group.set_priority(group_priority);
        let mut ru_setting = GroupRequestUnitSettings::new();
        ru_setting.mut_r_u().mut_settings().set_fill_rate(ru);
        group.set_r_u_settings(ru_setting);
        group
    }

    #[test]
    fn test_yatp_eviction() {
        // Test that when the read pool is full, a higher-priority incoming task
        // can evict the lowest-priority queued task.
        //
        // Strategy: Use 1 worker thread and max_tasks_per_worker=4 (total=4).
        // Spawn 1 blocking task to occupy the only worker thread, then spawn
        // 3 low-priority tasks that will sit in the queue. The pool is now
        // "full" (4 running_tasks). A high-priority task should evict one of
        // the queued low-priority tasks.
        //
        // Note on the two priority systems:
        // - `override_priority` (in ResourceControlContext) determines the TaskPriority
        //   bucket (High/Medium/Low) used for running_tasks counters. Both groups use 0
        //   here, so all tasks are "medium".
        // - Resource group priority (1 vs 16) controls the strict business-priority
        //   comparison. Only the higher-priority group may evict queued work.
        let resource_manager = Arc::new(ResourceGroupManager::default());
        let low_group = new_resource_group_ru("low_group".into(), 5000, 1);
        resource_manager.add_resource_group(low_group);
        let high_group = new_resource_group_ru("high_group".into(), 5000, 16);
        resource_manager.add_resource_group(high_group);

        let name = "test-yatp-eviction";

        let config = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 1,
            max_tasks_per_worker: 4,
            ..Default::default()
        };

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool_with_name(
            &config,
            DummyReporter,
            engine,
            Some(resource_manager),
            CleanupMethod::InPlace,
            name.to_owned(),
            false,
        );

        let gen_task = || {
            let (tx, rx) = oneshot::channel::<()>();
            let task = async move {
                let _ = rx.await;
            };
            (task, tx)
        };

        let handle = pool.handle();

        let low_ctx = ResourceControlContext {
            resource_group_name: "low_group".to_string(),
            override_priority: 0,
            ..Default::default()
        };

        // Task 1: synchronously blocks the only worker thread so that it
        // cannot pop any further tasks from the global priority queue. An
        // async-only future (oneshot::channel::await) would return Pending
        // immediately, letting the worker loop back and drain tasks 2-4 from
        // the queue before the eviction attempt — causing a race.
        let (block_tx, block_rx) = std::sync::mpsc::channel::<()>();
        let task1 = async move {
            let _ = block_rx.recv();
        };
        block_on(handle.spawn(
            task1,
            CommandPri::Normal,
            1,
            TaskMetadata::from_ctx(&low_ctx),
            None,
        ))
        .unwrap();

        // Wait for task1 to be picked up and block the worker.
        thread::sleep(Duration::from_millis(300));

        // Tasks 2-4: these will sit in the global queue since the worker is
        // blocked by task1.
        let (task2, _tx2) = gen_task();
        let (task3, _tx3) = gen_task();
        let (task4, _tx4) = gen_task();

        block_on(handle.spawn(
            task2,
            CommandPri::Normal,
            2,
            TaskMetadata::from_ctx(&low_ctx),
            None,
        ))
        .unwrap();
        block_on(handle.spawn(
            task3,
            CommandPri::Normal,
            3,
            TaskMetadata::from_ctx(&low_ctx),
            None,
        ))
        .unwrap();
        block_on(handle.spawn(
            task4,
            CommandPri::Normal,
            4,
            TaskMetadata::from_ctx(&low_ctx),
            None,
        ))
        .unwrap();

        // Verify pool is full: spawning another low-priority task should fail.
        let (task_low5, _tx_low5) = gen_task();
        match block_on(handle.spawn(
            task_low5,
            CommandPri::Normal,
            5,
            TaskMetadata::from_ctx(&low_ctx),
            None,
        )) {
            Err(ReadPoolError::UnifiedReadPoolFull) => {}
            other => panic!(
                "expected UnifiedReadPoolFull for low-priority task, got {:?}",
                other.err()
            ),
        }

        // Now spawn a high-priority task — should succeed via eviction of a
        // queued low-priority task.
        let (task_high, _tx_high) = gen_task();
        let high_ctx = ResourceControlContext {
            resource_group_name: "high_group".to_string(),
            override_priority: 0,
            ..Default::default()
        };

        block_on(handle.spawn(
            task_high,
            CommandPri::High,
            6,
            TaskMetadata::from_ctx(&high_ctx),
            None,
        ))
        .expect("high-priority task should succeed via eviction");

        // The eviction metric should have been incremented.
        assert!(
            match &handle {
                ReadPoolHandle::Yatp {
                    fair_queue: Some(queue),
                    ..
                } => queue.snapshot().evicted_tasks >= 1,
                _ => false,
            },
            "eviction counter should be incremented"
        );

        // Unblock task1 so the worker thread can resume and the pool can
        // shut down cleanly.
        let _ = block_tx.send(());
        thread::sleep(Duration::from_millis(300));
    }

    #[test]
    fn test_auto_adjust_disable_notifies_pool_size_change() {
        use std::sync::mpsc::sync_channel;

        let config = UnifiedReadPoolConfig {
            min_thread_count: 2,
            max_thread_count: 8,
            max_tasks_per_worker: 10,
            ..Default::default()
        };

        let engine = TestEngineBuilder::new().build().unwrap();
        let pool = build_yatp_read_pool(
            &config,
            DummyReporter,
            engine,
            None,
            CleanupMethod::InPlace,
            false,
        );
        let handle = pool.handle();

        let (tx, rx) = sync_channel(10);

        let core_thread_count = config.max_thread_count;
        let max_thread_count = config.max_thread_count;
        let process_stats = match tikv_util::sys::cpu_time::ProcessStat::cur_proc_stat() {
            Ok(process_stats) => process_stats,
            Err(_) => return,
        };

        let mut runner = ReadPoolConfigRunner {
            interval: READ_POOL_THREAD_CHECK_DURATION,
            sender: tx,
            handle,
            cpu_time_tracker: ReadPoolCpuTimeTracker::new("test"),
            process_stats,
            min_thread_count: core_thread_count,
            core_thread_count,
            max_thread_count,
            cur_thread_count: core_thread_count,
            auto_adjust: true,
            cpu_threshold: READ_POOL_THREAD_HIGH_THRESHOLD,
        };

        runner.cur_thread_count = 5;
        runner.run(Task::AutoAdjust(false));

        // The config change only records the flag now; sizing the pool belongs
        // to adjust_pool_size.
        assert_eq!(runner.cur_thread_count, 5);
        assert!(
            rx.try_recv().is_err(),
            "disabling auto-adjust should not resize the pool by itself"
        );

        // With no resource manager this takes the early-return path, which
        // restores the configured size.
        runner.adjust_pool_size();

        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(size) => assert_eq!(size, core_thread_count),
            Err(_) => {
                panic!("No pool size change notification received when auto-adjust was disabled.")
            }
        }

        assert_eq!(runner.cur_thread_count, core_thread_count);
    }
}
