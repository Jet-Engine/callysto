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
use rocksdb::{BlockBasedOptions, Cache, DBPath, Options as DBOptions, DB};
use std::convert::TryFrom;
use std::ffi::CString;
use std::path::Path;
use std::sync::Arc;
use url::Url;

const DEFAULT_WRITE_BUFFER_SIZE: usize = (1_usize << 6) * 1024 * 1024; // 64 MB
const DEFAULT_MAX_WRITE_BUFFER_NUMBER: i32 = 3;
const DEFAULT_TARGET_FILE_SIZE_BASE: u64 = (1_u64 << 6) * 1024 * 1024; // 64 MB
const DEFAULT_BLOCK_CACHE_SIZE: usize = 2 * (1_usize << 10).pow(3);
const DEFAULT_BLOCK_CACHE_COMPRESSED_SIZE: usize = 500 * (1_usize << 10).pow(2);
const DEFAULT_BLOOM_FILTER_SIZE: u64 = 3;
const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: u64 = 1 << 8;

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
    let freel = get_statvfs_bavail_frsize(p);
    (freel as f64 * 0.3).ceil() as _
}

#[derive(Clone)]
pub struct RocksDbStore {
    storage_url: Url,
    service_state: Arc<AtomicBox<ServiceState>>,
    dbs: LOTable<usize, Arc<DB>>,
    db_options: DBOptions,
}

impl RocksDbStore {
    pub fn new(storage_url: Url) -> Self {
        let mut rds = Self {
            storage_url,
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

    fn open(&self, partition_path: String) -> DB {
        // TODO: Hint for SST files
        // let dbpath = DBPath::new(
        //     self.storage_url.path(),
        //     get_db_level_free_limit(self.storage_url.path()),
        // ).unwrap();
        DB::open(&self.db_options, self.storage_url.path()).unwrap()
    }

    fn partition_path(partition: usize) -> String {
        todo!()
    }

    fn open_for_partition(&self, partition: usize) -> DB {
        todo!()
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
    fn persisted_offset(&self, tp: CTP) -> Option<usize> {
        todo!()
    }

    fn set_persisted_offset(&mut self, tp: CTP, offset: usize) -> crate::errors::Result<()> {
        todo!()
    }

    fn apply_changelog_batch(&self, events: Vec<OwnedMessage>) -> crate::errors::Result<()> {
        todo!()
    }

    fn reset_state(&self) -> crate::errors::Result<()> {
        todo!()
    }

    async fn on_rebalance(
        &self,
        assigned: Vec<CTP>,
        revoked: Vec<CTP>,
        newly_assigned: Vec<CTP>,
        generation_id: usize,
    ) -> crate::errors::Result<()> {
        todo!()
    }

    async fn on_recovery_completed(
        &self,
        active_tps: Vec<CTP>,
        standby_tps: Vec<CTP>,
    ) -> crate::errors::Result<()> {
        todo!()
    }
}
