// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;
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
    // Write raft state once for a region everytime writing to disk
    pub raft_states: HashMap<u64, RaftLocalState>,
    pub state_size: usize,
    pub tasks: Vec<AsyncWriteTask<EK, ER>>,
    metrics: AsyncWriteMetrics,
    waterfall_metrics: bool,
    has_last_data: bool,
    last_tasks: Vec<AsyncWriteTask<EK, ER>>,
    last_readies: HashMap<u64, (u64, u64)>,
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
            raft_states: HashMap::default(),
            tasks: vec![],
            state_size: 0,
            metrics: Default::default(),
            waterfall_metrics,
            has_last_data: false,
            last_tasks: vec![],
            last_readies: HashMap::default(),
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

        let entries = mem::take(&mut task.entries);
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
                self.state_size += mem::size_of::<RaftLocalState>();
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
    fn save_last_data(&mut self) {
        // The last data must be taken out before the writebatch being taken to persist
        assert!(self.last_tasks.is_empty());
        self.last_tasks = mem::take(&mut self.tasks);
        assert!(self.last_readies.is_empty());
        self.last_readies = mem::take(&mut self.readies);
        self.has_last_data = true;
    }

    #[inline]
    fn take_last_data(
        &mut self,
    ) -> Option<(Vec<AsyncWriteTask<EK, ER>>, HashMap<u64, (u64, u64)>)> {
        if !self.has_last_data {
            return None;
        }
        self.has_last_data = false;
        Some((
            mem::take(&mut self.last_tasks),
            mem::take(&mut self.last_readies),
        ))
    }

    #[inline]
    fn get_raft_size(&self) -> usize {
        self.state_size + self.raft_wb.persist_size()
    }

    fn before_write_to_db(&mut self) {
        // Put raft state to raft writebatch
        let raft_states = mem::take(&mut self.raft_states);
        for (region_id, state) in raft_states {
            self.raft_wb.put_raft_state(region_id, &state).unwrap();
        }
        self.state_size = 0;
        if self.waterfall_metrics {
            let now = Instant::now();
            for task in &self.tasks {
                for ts in &task.proposal_times {
                    self.metrics.to_write.observe(duration_to_sec(now - *ts));
                }
            }
        }
    }

    fn after_write_to_kv_db(&mut self) {
        if self.waterfall_metrics {
            let now = Instant::now();
            for task in &self.tasks {
                for ts in &task.proposal_times {
                    self.metrics.kvdb_end.observe(duration_to_sec(now - *ts));
                }
            }
        }
    }

    fn after_write_to_db(&mut self) {
        if self.waterfall_metrics {
            let now = Instant::now();
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
    stopped: bool,
    current: usize,
    flip_batchs: [Option<AsyncWriteBatch<EK, ER>>; 2],
}

impl<EK, ER> AsyncFlipWriteBatch<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn new(kv_engine: &EK, raft_engine: &ER, waterfall_metrics: bool) -> Self {
        Self {
            stopped: false,
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
        }
    }

    fn get(&mut self) -> &mut AsyncWriteBatch<EK, ER> {
        self.flip_batchs[self.current].as_mut().unwrap()
    }

    fn get_another(&mut self) -> Option<&mut AsyncWriteBatch<EK, ER>> {
        self.flip_batchs[self.current ^ 1].as_mut()
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
struct AsyncWriteWorker<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    store_id: u64,
    tag: String,
    kv_engine: EK,
    raft_engine: ER,
    flip_wb: Arc<(Mutex<AsyncFlipWriteBatch<EK, ER>>, Condvar)>,
    sender: Sender<AsyncWriteMsg<EK, ER>>,
    perf_context: EK::PerfContext,
}

impl<EK, ER> AsyncWriteWorker<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn new(
        store_id: u64,
        tag: String,
        kv_engine: EK,
        raft_engine: ER,
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
            flip_wb,
            sender,
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

            wb.save_last_data();
            wb.clear();

            {
                let mut flip_wb = self.flip_wb.0.lock().unwrap();
                flip_wb.put_back(wb);
            }

            // io thread is destroyed after batch thread during shutdown
            self.sender.send(AsyncWriteMsg::Persisted).unwrap();
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
        wb.before_write_to_db();

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

        wb.after_write_to_kv_db();

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

        wb.after_write_to_db();

        wb.flush_metrics();

        fail_point!("raft_after_save");
    }
}

pub enum AsyncWriteMsg<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    WriteTask(AsyncWriteTask<EK, ER>),
    Persisted,
    Shutdown,
}

struct AsyncBatchWorker<EK, ER, N, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    N: Notifier,
    T: Transport,
{
    flip_wb: Arc<(Mutex<AsyncFlipWriteBatch<EK, ER>>, Condvar)>,
    receiver: Receiver<AsyncWriteMsg<EK, ER>>,
    notifier: N,
    trans: T,
    message_metrics: RaftSendMessageMetrics,
}

impl<EK, ER, N, T> AsyncBatchWorker<EK, ER, N, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    N: Notifier,
    T: Transport,
{
    fn new(
        flip_wb: Arc<(Mutex<AsyncFlipWriteBatch<EK, ER>>, Condvar)>,
        receiver: Receiver<AsyncWriteMsg<EK, ER>>,
        notifier: N,
        trans: T,
    ) -> Self {
        Self {
            flip_wb,
            receiver,
            notifier,
            trans,
            message_metrics: Default::default(),
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
                    let last_data_1 = wb.take_last_data();
                    let last_data_2 = flip_wb.get_another().map_or(None, |wb| wb.take_last_data());
                    drop(flip_wb);
                    if is_empty {
                        self.flip_wb.1.notify_one();
                    }
                    self.handle_last_data(last_data_1);
                    self.handle_last_data(last_data_2);
                }
                AsyncWriteMsg::Persisted => {
                    let mut flip_wb = self.flip_wb.0.lock().unwrap();
                    let last_data_1 = flip_wb.get().take_last_data();
                    let last_data_2 = flip_wb.get_another().map_or(None, |wb| wb.take_last_data());
                    drop(flip_wb);
                    self.handle_last_data(last_data_1);
                    self.handle_last_data(last_data_2);
                }
            }
        }
    }

    #[inline]
    fn handle_last_data(
        &mut self,
        last_data: Option<(Vec<AsyncWriteTask<EK, ER>>, HashMap<u64, (u64, u64)>)>,
    ) {
        if let Some((tasks, readies)) = last_data {
            self.send_msg(tasks);
            self.notify_persisted(readies);
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

pub struct AsyncWriter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    flip_wbs: Vec<Arc<(Mutex<AsyncFlipWriteBatch<EK, ER>>, Condvar)>>,
    io_handlers: Vec<JoinHandle<()>>,
    batch_senders: Vec<Sender<AsyncWriteMsg<EK, ER>>>,
    batch_handlers: Vec<JoinHandle<()>>,
}

impl<EK, ER> AsyncWriter<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new() -> Self {
        Self {
            flip_wbs: vec![],
            io_handlers: vec![],
            batch_senders: vec![],
            batch_handlers: vec![],
        }
    }

    pub fn spawn<N: Notifier, T: Transport + 'static>(
        &mut self,
        store_id: u64,
        kv_engine: &EK,
        raft_engine: &ER,
        notifier: &N,
        trans: &T,
        config: &Config,
    ) -> Result<()> {
        for i in 0..config.store_io_pool_size {
            let io_tag = format!("store-writer-{}", i);
            let batch_tag = format!("store-batcher-{}", i);
            let (tx, rx) = channel();
            let flip_wb = Arc::new((
                Mutex::new(AsyncFlipWriteBatch::new(
                    kv_engine,
                    raft_engine,
                    config.store_waterfall_metrics,
                )),
                Condvar::new(),
            ));
            let mut io_worker = AsyncWriteWorker::new(
                store_id,
                io_tag.clone(),
                kv_engine.clone(),
                raft_engine.clone(),
                flip_wb.clone(),
                tx.clone(),
                config,
            );
            let io_handler = thread::Builder::new()
                .name(thd_name!(io_tag))
                .spawn(move || {
                    io_worker.run();
                })?;
            let mut batch_worker =
                AsyncBatchWorker::new(flip_wb.clone(), rx, notifier.clone(), trans.clone());
            let batch_handler =
                thread::Builder::new()
                    .name(thd_name!(batch_tag))
                    .spawn(move || {
                        batch_worker.run();
                    })?;

            self.flip_wbs.push(flip_wb);
            self.io_handlers.push(io_handler);
            self.batch_senders.push(tx);
            self.batch_handlers.push(batch_handler);
        }
        Ok(())
    }

    pub fn batch_senders(&self) -> &Vec<Sender<AsyncWriteMsg<EK, ER>>> {
        &self.batch_senders
    }

    pub fn shutdown(&mut self) {
        assert_eq!(self.flip_wbs.len(), self.io_handlers.len());
        assert_eq!(self.io_handlers.len(), self.batch_senders.len());
        assert_eq!(self.batch_senders.len(), self.batch_handlers.len());
        for flip_wb in &self.flip_wbs {
            let mut wb = flip_wb.0.lock().unwrap();
            wb.stopped = true;
            flip_wb.1.notify_one();
        }
        for handler in self.io_handlers.drain(..) {
            handler.join().unwrap();
        }
        for sender in &self.batch_senders {
            sender.send(AsyncWriteMsg::Shutdown).unwrap();
        }
        for handler in self.batch_handlers.drain(..) {
            handler.join().unwrap();
        }
    }
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
