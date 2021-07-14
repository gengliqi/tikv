// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::store::config::Config;
use crate::store::fsm::RaftRouter;
use crate::store::local_metrics::{AsyncWriteMetrics, RaftSendMessageMetrics};
use crate::store::metrics::*;
use crate::store::transport::Transport;
use crate::store::{PeerMsg, SignificantMsg};
use crate::Result;

use collections::{HashMap, HashSet};
use engine_traits::{
    KvEngine, PerfContext, PerfContextKind, RaftEngine, RaftLogBatch, WriteBatch, WriteOptions,
};
use error_code::ErrorCodeExt;
use fail::fail_point;
use kvproto::raft_serverpb::{RaftLocalState, RaftMessage};
use raft::eraftpb::Entry;
use tikv_util::time::duration_to_sec;
use tikv_util::{box_err, debug, thd_name, warn};

const KV_WB_SHRINK_SIZE: usize = 1024 * 1024;
const RAFT_WB_SHRINK_SIZE: usize = 10 * 1024 * 1024;

/// Notify the event to the specified region.
pub trait Notifier: Clone + Send + 'static {
    fn notify_persisted(&self, region_id: u64, peer_id: u64, ready_number: u64, now: Instant);
    fn notify_unreachable(&self, region_id: u64, to_peer_id: u64);
}

impl<EK, ER> Notifier for RaftRouter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn notify_persisted(
        &self,
        region_id: u64,
        peer_id: u64,
        ready_number: u64,
        send_time: Instant,
    ) {
        if let Err(e) = self.force_send(
            region_id,
            PeerMsg::Persisted {
                peer_id,
                ready_number,
                send_time,
            },
        ) {
            warn!(
                "failed to send noop to trigger persisted ready";
                "region_id" => region_id,
                "peer_id" => peer_id,
                "ready_number" => ready_number,
                "error" => ?e,
            );
        }
    }

    fn notify_unreachable(&self, region_id: u64, to_peer_id: u64) {
        let msg = SignificantMsg::Unreachable {
            region_id,
            to_peer_id,
        };
        if let Err(e) = self.force_send(region_id, PeerMsg::SignificantMsg(msg)) {
            warn!(
                "failed to send unreachable";
                "region_id" => region_id,
                "unreachable_peer_id" => to_peer_id,
                "error" => ?e,
            );
        }
    }
}

/// AsyncWriteTask contains write tasks which need to be persisted to kv db and raft db.
#[derive(Debug)]
pub struct AsyncWriteTask<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    region_id: u64,
    peer_id: u64,
    ready_number: u64,
    pub send_time: Instant,
    pub kv_wb: Option<EK::WriteBatch>,
    pub raft_wb: Option<ER::LogBatch>,
    pub entries: Vec<Entry>,
    pub cut_logs: Option<(u64, u64)>,
    pub raft_state: Option<RaftLocalState>,
    pub messages: Vec<RaftMessage>,
    pub proposal_times: Vec<Instant>,
}

impl<EK, ER> AsyncWriteTask<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(region_id: u64, peer_id: u64, ready_number: u64) -> Self {
        Self {
            region_id,
            peer_id,
            ready_number,
            send_time: Instant::now(),
            kv_wb: None,
            raft_wb: None,
            entries: vec![],
            cut_logs: None,
            raft_state: None,
            messages: vec![],
            proposal_times: vec![],
        }
    }

    pub fn has_data(&self) -> bool {
        !(self.raft_state.is_none()
            && self.entries.is_empty()
            && self.cut_logs.is_none()
            && self.kv_wb.as_ref().map_or(true, |wb| wb.is_empty())
            && self.raft_wb.as_ref().map_or(true, |wb| wb.is_empty()))
    }

    /// Sanity check for robustness.
    pub fn valid(&self) -> Result<()> {
        if self.region_id == 0 || self.peer_id == 0 || self.ready_number == 0 {
            return Err(box_err!(
                "invalid id, region_id {}, peer_id {}, ready_number {}",
                self.region_id,
                self.peer_id,
                self.ready_number
            ));
        }
        let last_index = self.entries.last().map_or(0, |e| e.get_index());
        if let Some((from, _)) = self.cut_logs {
            if from != last_index + 1 {
                // Entries are put and deleted in the same writebatch.
                return Err(box_err!(
                    "invalid cut logs, last_index {}, cut_logs {:?}",
                    last_index,
                    self.cut_logs
                ));
            }
        }
        if last_index != 0
            && self
                .raft_state
                .as_ref()
                .map_or(true, |r| r.get_last_index() != last_index)
        {
            return Err(box_err!(
                "invalid raft state, last_index {}, raft_state {:?}",
                last_index,
                self.raft_state
            ));
        }
        Ok(())
    }
}

/// AsyncWriteBatch is used for combining several AsyncWriteTask into one.
pub struct AsyncWriteBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub kv_wb: EK::WriteBatch,
    pub raft_wb: ER::LogBatch,
    // region_id -> (peer_id, ready_number)
    pub readies: HashMap<u64, (u64, u64)>,
    pub last_readies: HashMap<u64, (u64, u64)>,
    // Write raft state once for a region everytime writing to disk
    pub raft_states: HashMap<u64, RaftLocalState>,
    pub state_size: usize,
    pub tasks: Vec<AsyncWriteTask<EK, ER>>,
    metrics: AsyncWriteMetrics,
    waterfall_metrics: bool,
}

impl<EK, ER> AsyncWriteBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn new(kv_wb: EK::WriteBatch, raft_wb: ER::LogBatch, waterfall_metrics: bool) -> Self {
        Self {
            kv_wb,
            raft_wb,
            readies: HashMap::default(),
            last_readies: HashMap::default(),
            raft_states: HashMap::default(),
            tasks: vec![],
            state_size: 0,
            metrics: Default::default(),
            waterfall_metrics,
        }
    }

    /// Add write task to this batch
    pub fn add_write_task(&mut self, mut task: AsyncWriteTask<EK, ER>) {
        if let Err(e) = task.valid() {
            panic!("task is not valid: {:?}", e);
        }
        self.readies
            .insert(task.region_id, (task.peer_id, task.ready_number));
        if let Some(kv_wb) = task.kv_wb.take() {
            self.kv_wb.merge(kv_wb);
        }
        if let Some(raft_wb) = task.raft_wb.take() {
            self.raft_wb.merge(raft_wb);
        }

        let entries = std::mem::take(&mut task.entries);
        self.raft_wb.append(task.region_id, entries).unwrap();
        if let Some((from, to)) = task.cut_logs {
            self.raft_wb.cut_logs(task.region_id, from, to);
        }
        if let Some(raft_state) = task.raft_state.take() {
            if self
                .raft_states
                .insert(task.region_id, raft_state)
                .is_none()
            {
                self.state_size += std::mem::size_of::<RaftLocalState>();
            }
        }
        self.tasks.push(task);
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    fn clear(&mut self) {
        // raft_wb doesn't have clear interface but it should be consumed by raft db before
        self.kv_wb.clear();
        self.readies.clear();
        self.raft_states.clear();
        self.tasks.clear();
        self.state_size = 0;
    }

    #[inline]
    fn get_raft_size(&self) -> usize {
        self.state_size + self.raft_wb.persist_size()
    }

    fn before_write_to_db(&mut self, now: Instant) {
        // Put raft state to raft writebatch
        let raft_states = std::mem::take(&mut self.raft_states);
        for (region_id, state) in raft_states {
            self.raft_wb.put_raft_state(region_id, &state).unwrap();
        }
        self.state_size = 0;
        if self.waterfall_metrics {
            for task in &self.tasks {
                for ts in &task.proposal_times {
                    self.metrics.to_write.observe(duration_to_sec(now - *ts));
                }
            }
        }
    }

    fn after_write_to_kv_db(&mut self, now: Instant) {
        if self.waterfall_metrics {
            for task in &self.tasks {
                for ts in &task.proposal_times {
                    self.metrics.kvdb_end.observe(duration_to_sec(now - *ts));
                }
            }
        }
    }

    fn after_write_to_db(&mut self, now: Instant) {
        if self.waterfall_metrics {
            for task in &self.tasks {
                for ts in &task.proposal_times {
                    self.metrics.write_end.observe(duration_to_sec(now - *ts))
                }
            }
        }
    }

    #[inline]
    fn flush_metrics(&mut self) {
        if self.waterfall_metrics {
            self.metrics.flush();
        }
    }
}

pub struct AsyncFlipWriteBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    current: usize,
    flip_batchs: [Option<AsyncWriteBatch<EK, ER>>; 2],
    stopped: bool,
}

impl<EK, ER> AsyncFlipWriteBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn new(kv_engine: &EK, raft_engine: &ER, waterfall_metrics: bool) -> Self {
        Self {
            current: 0,
            flip_batchs: [
                Some(AsyncWriteBatch::new(
                    kv_engine.write_batch(),
                    raft_engine.log_batch(256 * 1024),
                    waterfall_metrics,
                )),
                Some(AsyncWriteBatch::new(
                    kv_engine.write_batch(),
                    raft_engine.log_batch(256 * 1024),
                    waterfall_metrics,
                )),
            ],
            stopped: false,
        }
    }

    /// Get the current writebatch
    pub fn get(&mut self) -> &mut AsyncWriteBatch<EK, ER> {
        self.flip_batchs[self.current].as_mut().unwrap()
    }

    fn flip_and_take(&mut self) -> AsyncWriteBatch<EK, ER> {
        self.current ^= 1;
        self.flip_batchs[self.current ^ 1].take().unwrap()
    }

    fn put_back(&mut self, batch: AsyncWriteBatch<EK, ER>) {
        assert!(self.flip_batchs[self.current ^ 1].is_none());
        self.flip_batchs[self.current ^ 1] = Some(batch);
    }
}

#[allow(dead_code)]
struct AsyncWriteWorker<EK, ER, T, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport,
    N: Notifier,
{
    store_id: u64,
    tag: String,
    kv_engine: EK,
    raft_engine: ER,
    notifier: N,
    trans: T,
    flip_wb: Arc<(Mutex<AsyncFlipWriteBatch<EK, ER>>, Condvar)>,
    sender: Sender<AsyncWriteMsg<EK, ER>>,
    raft_write_size_limit: usize,
    message_metrics: RaftSendMessageMetrics,
    perf_context: EK::PerfContext,
}

impl<EK, ER, T, N> AsyncWriteWorker<EK, ER, T, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: Transport,
    N: Notifier,
{
    fn new(
        store_id: u64,
        tag: String,
        kv_engine: EK,
        raft_engine: ER,
        notifier: N,
        trans: T,
        flip_wb: Arc<(Mutex<AsyncFlipWriteBatch<EK, ER>>, Condvar)>,
        sender: Sender<AsyncWriteMsg<EK, ER>>,
        config: &Config,
    ) -> Self {
        let perf_context =
            kv_engine.get_perf_context(config.perf_level, PerfContextKind::RaftstoreStore);
        Self {
            store_id,
            tag,
            kv_engine,
            raft_engine,
            notifier,
            trans,
            flip_wb,
            sender,
            raft_write_size_limit: config.raft_write_size_limit.0 as usize,
            message_metrics: Default::default(),
            perf_context,
        }
    }

    fn run(&mut self) {
        loop {
            let mut flip_wb = self.flip_wb.0.lock().unwrap();
            while flip_wb.get().is_empty() && !flip_wb.stopped {
                flip_wb = self.flip_wb.1.wait(flip_wb).unwrap();
            }
            if flip_wb.stopped {
                break;
            }
            let mut wb = flip_wb.flip_and_take();
            drop(flip_wb);

            STORE_WRITE_TRIGGER_SIZE_HISTOGRAM.observe(wb.get_raft_size() as f64);

            self.sync_write(&mut wb);

            assert!(wb.last_readies.is_empty());
            wb.last_readies = std::mem::take(&mut wb.readies);
            let tasks = std::mem::take(&mut wb.tasks);
            wb.clear();

            {
                let mut flip_wb = self.flip_wb.0.lock().unwrap();
                flip_wb.put_back(wb);
            }

            // io thread is destroyed after batch thread during shutdown
            self.sender.send(AsyncWriteMsg::NotifyPersisted).unwrap();

            self.send_msg(tasks);
        }
    }

    /*fn run(&mut self) {
        let mut stopped = false;
        while !stopped {
            let loop_begin = Instant::now();
            let mut handle_begin = loop_begin;

            let mut first_time = true;
            while self.wb.get_raft_size() < self.raft_write_size_limit {
                let msg = if first_time {
                    match self.receiver.recv() {
                        Ok(msg) => {
                            first_time = false;
                            STORE_WRITE_TASK_GEN_DURATION_HISTOGRAM
                                .observe(duration_to_sec(loop_begin.elapsed()));
                            handle_begin = Instant::now();
                            msg
                        }
                        Err(_) => return,
                    }
                } else {
                    match self.receiver.try_recv() {
                        Ok(msg) => msg,
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            stopped = true;
                            break;
                        }
                    }
                };
                match msg {
                    AsyncWriteMsg::Shutdown => {
                        stopped = true;
                        break;
                    }
                    AsyncWriteMsg::WriteTask(task) => {
                        STORE_WRITE_TASK_WAIT_DURATION_HISTOGRAM
                            .observe(duration_to_sec(task.send_time.elapsed()));
                        self.wb.add_write_task(task);
                    }
                }
            }

            STORE_WRITE_HANDLE_MSG_DURATION_HISTOGRAM
                .observe(duration_to_sec(handle_begin.elapsed()));

            STORE_WRITE_TRIGGER_SIZE_HISTOGRAM.observe(self.wb.get_raft_size() as f64);

            self.sync_write();

            STORE_WRITE_LOOP_DURATION_HISTOGRAM
                .observe(duration_to_sec(handle_begin.elapsed()) as f64);
        }
    }*/

    fn sync_write(&mut self, wb: &mut AsyncWriteBatch<EK, ER>) {
        let mut now = Instant::now();
        wb.before_write_to_db(now);

        fail_point!("raft_before_save");

        if !wb.kv_wb.is_empty() {
            let now = Instant::now();
            let mut write_opts = WriteOptions::new();
            write_opts.set_sync(true);
            wb.kv_wb.write_opt(&write_opts).unwrap_or_else(|e| {
                panic!("{} failed to write to kv engine: {:?}", self.tag, e);
            });
            if wb.kv_wb.data_size() > KV_WB_SHRINK_SIZE {
                wb.kv_wb = self.kv_engine.write_batch_with_cap(4 * 1024);
            }

            STORE_WRITE_KVDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        }

        now = Instant::now();
        wb.after_write_to_kv_db(now);

        fail_point!("raft_between_save");

        if !wb.raft_wb.is_empty() {
            fail_point!("raft_before_save_on_store_1", self.store_id == 1, |_| {});

            self.perf_context.start_observe();
            let now = Instant::now();
            self.raft_engine
                .consume_and_shrink(&mut wb.raft_wb, true, RAFT_WB_SHRINK_SIZE, 16 * 1024)
                .unwrap_or_else(|e| {
                    panic!("{} failed to write to raft engine: {:?}", self.tag, e);
                });
            self.perf_context.report_metrics();

            STORE_WRITE_RAFTDB_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()) as f64);
        }

        now = Instant::now();
        wb.after_write_to_db(now);

        wb.flush_metrics();

        fail_point!("raft_after_save");
    }

    fn send_msg(&mut self, tasks: Vec<AsyncWriteTask<EK, ER>>) {
        fail_point!("raft_before_follower_send");

        let now = Instant::now();
        let mut unreachable_peers = HashSet::default();
        for mut task in tasks {
            for msg in task.messages.drain(..) {
                let msg_type = msg.get_message().get_msg_type();
                let to_peer_id = msg.get_to_peer().get_id();
                let to_store_id = msg.get_to_peer().get_store_id();
                if let Err(e) = self.trans.send(msg) {
                    // We use metrics to observe failure on production.
                    debug!(
                        "failed to send msg to other peer in async-writer";
                        "region_id" => task.region_id,
                        "peer_id" => task.peer_id,
                        "target_peer_id" => to_peer_id,
                        "target_store_id" => to_store_id,
                        "err" => ?e,
                        "error_code" => %e.error_code(),
                    );
                    self.message_metrics.add(msg_type, false);
                    // Send unreachable to this peer.
                    // If this msg is snapshot, it is unnecessary to send snapshot
                    // status to this peer because it has already become follower.
                    // (otherwise the snapshot msg should be sent in store thread other than here)
                    unreachable_peers.insert((task.region_id, to_peer_id));
                } else {
                    self.message_metrics.add(msg_type, true);
                }
            }
        }
        self.trans.flush();
        self.message_metrics.flush();
        for (region_id, to_peer_id) in unreachable_peers {
            self.notifier.notify_unreachable(region_id, to_peer_id);
        }
        STORE_WRITE_SEND_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()));
    }
}

pub enum AsyncWriteMsg<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    WriteTask(AsyncWriteTask<EK, ER>),
    NotifyPersisted,
    Shutdown,
}

struct AsyncBatchWorker<EK, ER, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    N: Notifier,
{
    flip_wb: Arc<(Mutex<AsyncFlipWriteBatch<EK, ER>>, Condvar)>,
    receiver: Receiver<AsyncWriteMsg<EK, ER>>,
    notifier: N,
}

impl<EK, ER, N> AsyncBatchWorker<EK, ER, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    N: Notifier,
{
    fn new(
        flip_wb: Arc<(Mutex<AsyncFlipWriteBatch<EK, ER>>, Condvar)>,
        receiver: Receiver<AsyncWriteMsg<EK, ER>>,
        notifier: N,
    ) -> Self {
        Self {
            flip_wb,
            receiver,
            notifier,
        }
    }

    fn run(&mut self) {
        loop {
            let msg = match self.receiver.recv() {
                Ok(m) => m,
                Err(_) => return,
            };
            match msg {
                AsyncWriteMsg::Shutdown => return,
                AsyncWriteMsg::WriteTask(task) => {
                    STORE_WRITE_TASK_WAIT_DURATION_HISTOGRAM
                        .observe(duration_to_sec(task.send_time.elapsed()));
                    let mut flip_wb = self.flip_wb.0.lock().unwrap();
                    let wb = flip_wb.get();
                    let is_empty = wb.is_empty();
                    wb.add_write_task(task);
                    let last_readies = if !wb.last_readies.is_empty() {
                        Some(std::mem::take(&mut wb.last_readies))
                    } else {
                        None
                    };
                    drop(flip_wb);
                    if is_empty {
                        self.flip_wb.1.notify_one();
                    }
                    if let Some(readies) = last_readies {
                        self.notify_persisted(readies);
                    }
                }
                AsyncWriteMsg::NotifyPersisted => {
                    let mut flip_wb = self.flip_wb.0.lock().unwrap();
                    let wb = flip_wb.get();
                    let last_readies = if !wb.last_readies.is_empty() {
                        Some(std::mem::take(&mut wb.last_readies))
                    } else {
                        None
                    };
                    drop(flip_wb);
                    if let Some(readies) = last_readies {
                        self.notify_persisted(readies);
                    }
                }
            }
        }
    }

    fn notify_persisted(&mut self, readies: HashMap<u64, (u64, u64)>) {
        let now = Instant::now();
        for (region_id, (peer_id, ready_number)) in readies {
            self.notifier
                .notify_persisted(region_id, peer_id, ready_number, now);
        }
        STORE_WRITE_CALLBACK_DURATION_HISTOGRAM.observe(duration_to_sec(now.elapsed()));
    }
}

pub struct AsyncWriter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    flip_wb: Arc<(Mutex<AsyncFlipWriteBatch<EK, ER>>, Condvar)>,
    io_handler: Option<JoinHandle<()>>,
    batch_sender: Sender<AsyncWriteMsg<EK, ER>>,
    batch_handler: Option<JoinHandle<()>>,
}

impl<EK, ER> AsyncWriter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new<N: Notifier, T: Transport + 'static>(
        store_id: u64,
        kv_engine: &EK,
        raft_engine: &ER,
        notifier: &N,
        trans: &T,
        config: &Config,
    ) -> Result<AsyncWriter<EK, ER>> {
        let (tx, rx) = channel();
        let tag = format!("store-writer");
        let flip_wb = Arc::new((
            Mutex::new(AsyncFlipWriteBatch::new(
                kv_engine,
                raft_engine,
                config.store_waterfall_metrics,
            )),
            Condvar::new(),
        ));
        let mut batch_worker = AsyncBatchWorker::new(flip_wb.clone(), rx, notifier.clone());
        let batch_handler = thread::Builder::new()
            .name(thd_name!("store-batcher"))
            .spawn(move || {
                batch_worker.run();
            })?;
        let mut io_worker = AsyncWriteWorker::new(
            store_id,
            tag.clone(),
            kv_engine.clone(),
            raft_engine.clone(),
            notifier.clone(),
            trans.clone(),
            flip_wb.clone(),
            tx.clone(),
            config,
        );
        let io_handler = thread::Builder::new()
            .name(thd_name!("store-writer"))
            .spawn(move || {
                io_worker.run();
            })?;
        Ok(AsyncWriter {
            flip_wb,
            io_handler: Some(io_handler),
            batch_sender: tx,
            batch_handler: Some(batch_handler),
        })
    }

    pub fn batch_sender(&self) -> &Sender<AsyncWriteMsg<EK, ER>> {
        &self.batch_sender
    }

    pub fn shutdown(&mut self) {
        let mut flip_wb = self.flip_wb.0.lock().unwrap();
        flip_wb.stopped = true;
        self.flip_wb.1.notify_one();
        self.io_handler.take().unwrap().join().unwrap();

        self.batch_sender.send(AsyncWriteMsg::Shutdown).unwrap();
        self.batch_handler.take().unwrap().join().unwrap();
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
