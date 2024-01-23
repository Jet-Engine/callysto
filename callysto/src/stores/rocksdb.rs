use crate::errors::{CallystoError, Result};
use crate::kafka::ctopic::CTP;
use crate::prelude::CTable;
use crate::stores::store::Store;
use crate::types::collection::Collection;
use crate::types::context::Context;
use crate::types::service::{Service, ServiceState};
use async_trait::*;
use futures::future::{BoxFuture, FutureExt, TryFutureExt};
use futures_timer::Delay;
use lever::prelude::{LOTable, TTas};
use lever::sync::atomics::AtomicBox;
use lever::sync::ifaces::LockIface;
use rdkafka::message::OwnedMessage;
use rdkafka::Message;
use rocksdb::{
    BlockBasedOptions, Cache, DBPath, DBWithThreadMode, Error, Options as DBOptions,
    SingleThreaded, WriteBatch, DB,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::convert::{identity, TryFrom, TryInto};
use std::ffi::CString;
use std::hint::unreachable_unchecked;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};
use url::Url;

const DEFAULT_WRITE_BUFFER_SIZE: usize = (1_usize << 6) * 1024 * 1024; // 64 MB
const DEFAULT_MAX_WRITE_BUFFER_NUMBER: i32 = 3;
const DEFAULT_TARGET_FILE_SIZE_BASE: u64 = (1_u64 << 6) * 1024 * 1024; // 64 MB
const DEFAULT_BLOCK_CACHE_SIZE: usize = 2 * (1_usize << 10).pow(3);
const DEFAULT_BLOCK_CACHE_COMPRESSED_SIZE: usize = 500 * (1_usize << 10).pow(2);
const DEFAULT_BLOOM_FILTER_SIZE: u64 = 3;
const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: u64 = 1 << 8;

const CALLYSTO_OFFSET_KEY: &[u8; 19] = b"__callysto\0offset__";

#[inline]
fn get_rlimit_max() -> u64 {
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    let ret = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) };
    if ret != 0 {
        panic!("RLIMIT_NOFILE Err: {}", std::io::Error::last_os_error());
    }
    let limit = unsafe { limit.assume_init() };
    u64::try_from(limit.rlim_max).unwrap()
}

///
/// Give rounded down hard limit db max open file count
/// `Total max proc fd * 0.9`
pub(super) fn get_db_level_rlimit_max() -> u64 {
    let rlimit = get_rlimit_max();
    (rlimit as f64 * 0.9).ceil() as _
}

#[inline]
fn get_statvfs_bavail_frsize<T>(p: T) -> u64
where
    T: AsRef<Path>,
{
    let path: CString = p
        .as_ref()
        .to_str()
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))
        .and_then(|s| {
            CString::new(s).map_err(|_| std::io::Error::from(std::io::ErrorKind::InvalidInput))
        })
        .unwrap();

    let mut vfs = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let ret = unsafe { libc::statvfs(path.as_ptr(), vfs.as_mut_ptr()) };
    if ret != 0 {
        panic!("FS STAT Err: {}", std::io::Error::last_os_error());
    }

    let vfs = unsafe { vfs.assume_init() };
    u64::try_from(vfs.f_frsize * vfs.f_bavail).unwrap()
}

///
/// Give rounded down estimated state size based on the free disk block
/// `Total free disk size * 0.3`
pub(super) fn get_db_level_free_limit<T>(p: T) -> u64
where
    T: AsRef<Path>,
{
    let free = get_statvfs_bavail_frsize(p);
    (free as f64 * 0.3).ceil() as _
}

#[derive(Clone)]
pub struct RocksDbStore {
    app_name: String,
    storage_url: Url,
    db_lock: Arc<TTas<()>>,
    rebalance_ack: Arc<AtomicBool>,
    table_name: String,
    service_state: Arc<AtomicBox<ServiceState>>,
    dbs: LOTable<usize, Arc<DB>>,
    db_options: DBOptions,
}

impl RocksDbStore {
    pub fn new(app_name: String, storage_url: Url, table_name: String) -> Self {
        let mut rds = Self {
            app_name,
            storage_url,
            db_lock: Arc::new(TTas::new(())),
            rebalance_ack: Arc::new(AtomicBool::default()),
            table_name,
            service_state: Arc::new(AtomicBox::new(ServiceState::PreStart)),
            dbs: LOTable::default(),
            db_options: DBOptions::default(),
        };
        rds.set_db_options();
        rds
    }

    pub fn set_db_options(&mut self) {
        let mut opts = DBOptions::default();
        opts.set_max_open_files(get_db_level_rlimit_max() as _);
        opts.set_write_buffer_size(DEFAULT_WRITE_BUFFER_SIZE);
        opts.set_max_write_buffer_number(DEFAULT_MAX_WRITE_BUFFER_NUMBER);
        opts.set_target_file_size_base(DEFAULT_TARGET_FILE_SIZE_BASE);
        opts.set_max_bytes_for_level_base(DEFAULT_MAX_BYTES_FOR_LEVEL_BASE);
        opts.create_if_missing(true);

        let mut block_opts = BlockBasedOptions::default();
        // Adapt ribbons.
        block_opts.set_format_version(5);
        block_opts.set_index_block_restart_interval(16);
        // XXX: Check if cache usage for idx are giving a speedup.
        // block_opts.set_cache_index_and_filter_blocks(true);
        // block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);

        let block_cache = Cache::new_lru_cache(DEFAULT_BLOCK_CACHE_SIZE).unwrap();
        block_opts.set_block_cache(&block_cache);

        let block_cache_compressed =
            Cache::new_lru_cache(DEFAULT_BLOCK_CACHE_COMPRESSED_SIZE).unwrap();
        block_opts.set_block_cache_compressed(&block_cache_compressed);
        opts.set_block_based_table_factory(&block_opts);

        self.db_options = opts;
    }

    fn open_for_partition(&self, partition: usize) -> Result<DB> {
        // TODO: Hint for SST files
        // let dbpath = DBPath::new(
        //     self.storage_url.path(),
        //     get_db_level_free_limit(self.storage_url.path()),
        // ).unwrap();
        let partition_path = self.partition_path(partition);
        DB::open(&self.db_options, partition_path).map_err(CallystoError::RocksDBError)
    }

    fn partition_path(&self, partition: usize) -> String {
        PathBuf::from(self.storage_url.path())
            .join(format!(
                "{}-{}-{}.db",
                self.app_name, self.table_name, partition
            ))
            .into_os_string()
            .into_string()
            .unwrap()
    }

    fn db_for_partition(&self, partition: usize) -> Result<Arc<DB>> {
        match self.dbs.get(&partition) {
            Some(x) => Ok(x),
            _ => {
                let db = Arc::new(self.open_for_partition(partition)?);
                let _ = self.dbs.insert(partition, db.clone());
                Ok(db)
            }
        }
    }

    fn revoke_partitions<State>(&self, table: impl Collection<State>, tps: HashSet<CTP>)
    where
        State: Clone + Send + Sync + 'static,
    {
        tps.iter().for_each(|tp| {
            if table.changelog_topic().topic_name() == tp.topic {
                if let Some(_) = self.dbs.get(&tp.partition) {
                    info!("closing db {} partition {}", tp.topic, tp.partition);
                    let _ = self.dbs.remove(&tp.partition);
                }
            }
        });
    }

    async fn assign_partitions<State>(
        &self,
        table: impl Collection<State>,
        tps: HashSet<CTP>,
        generation_id: usize,
    ) where
        State: Clone + Send + Sync + 'static,
    {
        self.rebalance_ack.store(true, Ordering::SeqCst);
        for tp in tps.iter() {
            if tp.topic == table.changelog_topic().topic_name()
                && self.rebalance_ack.load(Ordering::SeqCst)
            {
                self.clone()
                    .try_open_db_for_partition::<State>(tp.partition, 30, 1., generation_id)
                    .await;
                Delay::new(Duration::from_millis(5)).await;
                todo!()
            }
        }
        todo!("Implement assigned partitions on rebalance. Also assignors.\n https://github.com/fede1024/rust-rdkafka/blob/5d23e82a675d9df1bf343aedcaa35be864787dab/examples/simple_consumer.rs#L17-L36")
    }

    async fn try_open_db_for_partition<State>(
        self,
        partition: usize,
        max_retries: usize,
        retry_delay: f64,
        generation_id: usize,
    ) -> Result<Arc<DB>>
    where
        State: Clone + Send + Sync + 'static,
    {
        for i in (0..max_retries).into_iter() {
            info!(
                "opening partition {} for gen id {} app id {}",
                partition, generation_id, 0
            );
            match self.db_for_partition(partition) {
                Ok(x) => return Ok(x),
                Err(CallystoError::RocksDBError(ref e)) => {
                    if i == max_retries - 1 || !e.clone().into_string().contains("lock") {
                        // Release all the locks and crash
                        warn!("DB for partition {} retries timed out", partition);
                        <Self as Service<State>>::stop(&self).await;
                        return Err(CallystoError::RocksDBError(e.clone()));
                    }

                    let rdelay: u64 = retry_delay.round() as _;
                    info!(
                        "DB for partition {} is locked! Retry in {}s...",
                        partition, rdelay
                    );
                    // TODO: Generation check that rebalance occurred again.

                    Delay::new(Duration::from_secs(retry_delay.round() as _)).await;
                    e
                }
                _ => unreachable!("Shouldn't be here."),
            };
        }

        Err(CallystoError::GeneralError(format!(
            "DB Open failure for partition {}",
            partition
        )))
    }
}

#[async_trait]
impl<State> Service<State> for RocksDbStore
where
    State: Clone + Send + Sync + 'static,
{
    async fn call(&self, st: Context<State>) -> Result<State> {
        todo!()
    }

    async fn start(&self) -> Result<BoxFuture<'_, ()>> {
        let closure = async move {
            info!("Rocksdb backend is started.");
            self.service_state.replace_with(|_| ServiceState::Running);
        };

        Ok(closure.boxed())
    }

    async fn restart(&self) -> Result<()> {
        <Self as Service<State>>::stop(&self)
            .and_then(|_| <Self as Service<State>>::start(self))
            .await;

        Ok(())
    }

    async fn crash(&self) {
        <Self as Service<State>>::service_state(self)
            .await
            .replace_with(|e| ServiceState::Crashed);
    }

    async fn stop(&self) -> Result<()> {
        todo!()
    }

    async fn wait_until_stopped(&self) {
        todo!()
    }

    async fn started(&self) -> bool {
        *<Self as Service<State>>::service_state(self).await.get() == ServiceState::Running
    }

    async fn stopped(&self) -> bool {
        *<Self as Service<State>>::service_state(self).await.get() == ServiceState::Stopped
    }

    async fn crashed(&self) -> bool {
        *<Self as Service<State>>::service_state(self).await.get() == ServiceState::Crashed
    }

    async fn state(&self) -> String {
        todo!()
    }

    async fn label(&self) -> String {
        format!(
            "{}@{}",
            self.app_name,
            <Self as Service<State>>::shortlabel(self).await
        )
    }

    async fn shortlabel(&self) -> String {
        format!("rocksdb:{}", self.table_name)
    }

    async fn service_state(&self) -> Arc<AtomicBox<ServiceState>> {
        self.service_state.clone()
    }
}

#[async_trait]
impl<State> Store<State> for RocksDbStore
where
    State: Clone + Send + Sync + 'static,
{
    fn get(&self, serialized_key: Vec<u8>, msg: OwnedMessage) -> Result<Option<Vec<u8>>> {
        let partition: usize = msg.partition() as _;
        let db = self.db_for_partition(partition)?;
        match db.get(serialized_key.as_slice())? {
            Some(x) => Ok(Some(bincode::deserialize(x.as_slice())?)),
            _ => Ok(None),
        }
    }

    fn set(
        &self,
        serialized_key: Vec<u8>,
        serialized_val: Vec<u8>,
        msg: OwnedMessage,
    ) -> Result<()> {
        let partition: usize = msg.partition() as _;
        // self.send_changelog(self.partition_for_key(serialized_key))
        let db = self.db_for_partition(partition)?;
        Ok(db.put(serialized_key.as_slice(), serialized_val.as_slice())?)
    }

    fn del(&self, serialized_key: Vec<u8>, msg: OwnedMessage) -> Result<()> {
        let partition: usize = msg.partition() as _;
        let db = self.db_for_partition(partition)?;
        Ok(db.delete(serialized_key.as_slice())?)
    }

    fn table(&self) -> CTable<State> {
        unimplemented!("Table needs to be implemented on top of Storage.")
    }

    fn persisted_offset(&self, tp: CTP) -> Result<Option<usize>> {
        let offset = self
            .db_for_partition(tp.partition)?
            .get(CALLYSTO_OFFSET_KEY)?
            .map_or(None, |e| {
                Option::from(usize::from_ne_bytes(e.as_slice().try_into().unwrap()))
            });
        Ok(offset)
    }

    fn set_persisted_offset(&self, tp: CTP, offset: usize) -> Result<()> {
        Ok(self
            .db_for_partition(tp.partition)?
            .put(CALLYSTO_OFFSET_KEY, offset.to_ne_bytes())?)
    }

    fn apply_changelog_batch(&self, events: Vec<OwnedMessage>) -> Result<()> {
        let mut write_batches: HashMap<usize, WriteBatch> = HashMap::with_capacity(10);
        let mut tp_offsets: HashMap<CTP, usize> = HashMap::with_capacity(events.len());
        events.iter().for_each(|e| {
            let tp = CTP::new(e.topic().into(), e.partition() as _);
            let offset: usize = e.offset() as _;
            tp_offsets
                .entry(tp)
                .and_modify(|o| *o = offset.max(*o))
                .or_insert(offset);

            match e.payload() {
                Some(p) => {
                    write_batches
                        .entry(e.partition() as _)
                        .and_modify(|wb| {
                            wb.put(e.key().unwrap(), p);
                        })
                        .or_insert_with(|| {
                            let mut wb = WriteBatch::default();
                            wb.put(e.key().unwrap(), p);
                            wb
                        });
                }
                _ => {
                    write_batches
                        .entry(e.partition() as _)
                        .and_modify(|wb| {
                            wb.delete(e.key().unwrap());
                        })
                        .or_insert_with(|| {
                            let mut wb = WriteBatch::default();
                            wb.delete(e.key().unwrap());
                            wb
                        });
                }
            }
        });

        write_batches
            .into_iter()
            .try_for_each(|(partition, batch)| {
                self.db_for_partition(partition)?
                    .write(batch)
                    .map_err(CallystoError::RocksDBError)
            })?;

        tp_offsets.into_iter().try_for_each(|(tp, offset)| {
            <Self as Store<State>>::set_persisted_offset(self, tp, offset)
        })?;

        Ok(())
    }

    fn reset_state(&self) -> Result<()> {
        self.dbs.clear();
        let p = std::fs::canonicalize(
            self.storage_url
                .to_file_path()
                .map_err(|_| CallystoError::GeneralError("To file path failed.".into()))?,
        )?;
        std::fs::remove_dir_all(p);
        Ok(())
    }

    async fn on_rebalance(
        &self,
        assigned: Vec<CTP>,
        revoked: Vec<CTP>,
        newly_assigned: Vec<CTP>,
        generation_id: usize,
    ) -> Result<()> {
        // TODO: On Rebalance
        self.rebalance_ack.store(false, Ordering::SeqCst);
        if let Some(_lock) = self.db_lock.try_lock() {
            //             self.revoke_partitions(self.table, revoked)
            //             await self.assign_partitions(self.table, newly_assigned, generation_id)
        }
        // Dropped but unlock anyway.
        let _ = self.db_lock.unlock();
        todo!()
    }

    async fn on_recovery_completed(
        &self,
        active_tps: Vec<CTP>,
        standby_tps: Vec<CTP>,
    ) -> Result<()> {
        todo!()
    }

    fn into_service(&self) -> &dyn Service<State> {
        self
    }
}
