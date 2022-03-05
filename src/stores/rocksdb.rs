use std::cmp::max;
use std::collections::HashMap;
use crate::definitions::Context;
use crate::errors::Result;
use crate::kafka::ctopic::CTP;
use crate::service::{Service, ServiceState};
use crate::stores::stores::Store;
use async_trait::*;
use futures::future::TryFutureExt;
use lever::prelude::LOTable;
use lever::sync::atomics::AtomicBox;
use rdkafka::message::OwnedMessage;
use rocksdb::{BlockBasedOptions, Cache, DBPath, Options as DBOptions, DB, WriteBatch};
use std::convert::{identity, TryFrom, TryInto};
use std::ffi::CString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use rdkafka::Message;
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
    storage_url: Url,
    table_name: String,
    service_state: Arc<AtomicBox<ServiceState>>,
    dbs: LOTable<usize, Arc<DB>>,
    db_options: DBOptions,
}

impl RocksDbStore {
    pub fn new(storage_url: Url, table_name: String) -> Self {
        let mut rds = Self {
            storage_url,
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

    fn open_for_partition(&self, partition: usize) -> DB {
        // TODO: Hint for SST files
        // let dbpath = DBPath::new(
        //     self.storage_url.path(),
        //     get_db_level_free_limit(self.storage_url.path()),
        // ).unwrap();
        let partition_path = self.partition_path(partition);
        DB::open(&self.db_options, partition_path).unwrap()
    }

    fn partition_path(&self, partition: usize) -> String {
        PathBuf::from(self.storage_url.path())
            .join(format!("{}-{}.db", self.table_name, partition))
            .into_os_string()
            .into_string()
            .unwrap()
    }

    fn db_for_partition(&self, partition: usize) -> Arc<DB> {
        self.dbs.get(&partition).unwrap_or_else(|| {
            let db = Arc::new(self.open_for_partition(partition));
            let _ = self.dbs.insert(partition, db.clone());
            db
        })
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

    async fn start(&self) -> Result<()> {
        todo!()
    }

    async fn restart(&self) -> Result<()> {
        <Self as Service<State>>::stop(self)
            .and_then(|_| <Self as Service<State>>::start(self))
            .await
    }

    async fn crash(&self) {
        todo!()
    }

    async fn stop(&self) -> Result<()> {
        todo!()
    }

    async fn wait_until_stopped(&self) {
        todo!()
    }

    async fn started(&self) -> bool {
        todo!()
    }

    async fn crashed(&self) -> bool {
        todo!()
    }

    async fn state(&self) -> String {
        todo!()
    }

    async fn label(&self) -> String {
        todo!()
    }

    async fn shortlabel(&self) -> String {
        todo!()
    }

    async fn service_state(&self) -> Arc<ServiceState> {
        self.service_state.get()
    }
}

#[async_trait]
impl<State> Store<State> for RocksDbStore
where
    State: Clone + Send + Sync + 'static,
{
    fn persisted_offset(&self, tp: CTP) -> Result<Option<usize>> {
        let offset =
            self.db_for_partition(tp.partition).get(CALLYSTO_OFFSET_KEY)?
                .map_or(None, |e| Option::from(usize::from_ne_bytes(e.as_slice().try_into().unwrap())));
        Ok(offset)
    }

    fn set_persisted_offset(&self, tp: CTP, offset: usize) -> Result<()> {
        Ok(self.db_for_partition(tp.partition).put(CALLYSTO_OFFSET_KEY, offset.to_string())?)
    }

    fn apply_changelog_batch(&self, events: Vec<OwnedMessage>) -> Result<()> {
        let mut write_batches: HashMap<usize, WriteBatch> = HashMap::with_capacity(10);
        let mut tp_offsets: HashMap<CTP, usize> = HashMap::with_capacity(events.len());
        events.iter().for_each(|e| {
            let tp = CTP::new(e.topic().into(), e.partition() as _);
            let offset: usize = e.offset() as _;
            tp_offsets.entry(tp)
                .and_modify(|o| *o = offset.max(*o))
                .or_insert(offset);

            match e.payload() {
                Some(p) => {
                    write_batches.entry(e.partition() as _)
                        .and_modify(|wb| {
                            wb.put(e.key().unwrap(), p);
                        })
                        .or_insert_with(|| {
                            let mut wb = WriteBatch::default();
                            wb.put(e.key().unwrap(), p);
                            wb
                        });
                },
                _ => {
                    write_batches.entry(e.partition() as _)
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

        write_batches.into_iter().try_for_each(|(partition, batch)| {
            self.db_for_partition(partition).write(batch)
        })?;

        tp_offsets.into_iter().try_for_each(|(tp, offset)| {
            <Self as Store<State>>::set_persisted_offset(self, tp, offset)
        })?;

        Ok(())
    }

    fn reset_state(&self) -> Result<()> {
        todo!()
    }

    async fn on_rebalance(
        &self,
        assigned: Vec<CTP>,
        revoked: Vec<CTP>,
        newly_assigned: Vec<CTP>,
        generation_id: usize,
    ) -> Result<()> {
        todo!()
    }

    async fn on_recovery_completed(
        &self,
        active_tps: Vec<CTP>,
        standby_tps: Vec<CTP>,
    ) -> Result<()> {
        todo!()
    }
}
