// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::HashMap,
    fs,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
    time::{Duration, UNIX_EPOCH},
};

use api_version::{api_v2::KEYSPACE_PREFIX_LEN, ApiV2};
use bytes::Bytes;
use cloud_encryption::MasterKey;
use dashmap::DashMap;
use hyper::{Body, Request};
use keys::next_key;
use kvengine::{
    context::{new_meta_file_cache, IaCtx, MetaFileCacheWeighter, PrepareType, SnapCtx},
    dfs::{self, Dfs},
    ia::{ia_file::parse_table_meta_filename, manager::IaManager, util::IaConfig},
    table::{
        columnar::{
            filter::TableScanCtx, ColumnarFileCache, ColumnarFileCacheConfig, ColumnarMetaCache,
        },
        file::{FdCache, File},
        fts::{FtsCache, FtsCacheConfig, FtsDeltaCache, FtsDeltaCacheConfig},
        schema_file::SchemaFile,
        sstable::{BlockCache, BlockCacheType},
        vector_index::{VectorIndexCache, VectorIndexConfig},
    },
    table_id::encode_table_prefix_key,
    txn_chunk_manager::TxnChunkManager,
    CloudColumnarReaders, SnapAccess, TableCtx,
};
use kvproto::{
    coprocessor::DelegateResponse,
    metapb::{Peer, Store},
};
use pd_client::{BucketStat, PdClient};
use protobuf::Message;
use quick_cache::{
    sync::{Cache, DefaultLifecycle},
    DefaultHashBuilder,
};
use security::{HttpClientError, SecurityManager};
use thiserror::Error;
use tikv_util::{
    config::AbsoluteOrPercentSize,
    memory::{MemoryLimiter, MemoryQuota},
    sys::SysQuota,
    time::Instant,
};
use tipb::ColumnInfo;

use crate::metrics::{COLUMNAR_FETCH_SNAPSHOT_HISTOGRAM, COLUMNAR_FETCH_SNAPSHOT_RETRY_COUNTER};

const BACKOFF_INITIAL: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(10);
const BACKOFF_RETRY_COUNT: usize = 5;

const SNAPSHOT_CACHE_SIZE: usize = 10240; // 10k shards
const SNAPSHOT_CACHE_CAP: u64 = 1024 * 1024 * 1024; // 1GB snapshot size with memtable
const SNAPSHOT_CACHE_CAPABILITY_HEADER: &str = "x-cse-snapshot-cache-version";
pub(crate) const DEFAULT_SCHEMA_FILE_CACHE_MAX_KEYSPACES: usize = 1024;

#[derive(Clone)]
struct RefreshingHttpClient {
    inner: Arc<RefreshingHttpClientInner>,
}

struct RefreshingHttpClientInner {
    security_mgr: Arc<SecurityManager>,
    client: Mutex<security::HttpClient>,
    #[cfg(test)]
    refresh_count: std::sync::atomic::AtomicUsize,
}

impl RefreshingHttpClient {
    fn new(security_mgr: Arc<SecurityManager>) -> Self {
        let client = security_mgr.http_client(hyper::Client::builder()).unwrap();
        Self {
            inner: Arc::new(RefreshingHttpClientInner {
                security_mgr,
                client: Mutex::new(client),
                #[cfg(test)]
                refresh_count: std::sync::atomic::AtomicUsize::new(0),
            }),
        }
    }

    async fn request(&self, req: Request<Body>) -> Result<hyper::Response<Body>, HttpClientError> {
        let uri = req.uri().clone();
        let client = self.inner.client.lock().unwrap().clone();
        let resp = client.request(req).await;
        if let Err(err) = resp.as_ref() {
            self.maybe_refresh_on_tls_error(&uri, &err.to_string());
        }
        resp
    }

    fn maybe_refresh_on_tls_error(&self, uri: &hyper::Uri, err: &str) {
        if is_tls_certificate_error(err) {
            warn!(
                "snapshot request hit tls certificate error, refresh http client";
                "uri" => uri.to_string(),
                "err" => err,
            );
            self.refresh();
        }
    }

    fn refresh(&self) {
        match self
            .inner
            .security_mgr
            .http_client(hyper::Client::builder())
        {
            Ok(client) => {
                info!("refreshed snapshot http client"; "reason" => "tls_error");
                let mut current = self.inner.client.lock().unwrap();
                *current = client;
                #[cfg(test)]
                self.inner.refresh_count.fetch_add(1, Ordering::Relaxed);
            }
            Err(err) => {
                warn!(
                    "failed to refresh snapshot http client";
                    "reason" => "tls_error",
                    "err" => err.to_string(),
                );
            }
        }
    }

    #[cfg(test)]
    fn refresh_count(&self) -> usize {
        self.inner.refresh_count.load(Ordering::Relaxed)
    }
}

fn is_tls_certificate_error(err: &str) -> bool {
    err.contains("CertificateExpired")
        || err.contains("InvalidCertificate")
        || err.to_ascii_lowercase().contains("certificate")
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("pd client error {0}")]
    PdClientError(#[from] pd_client::Error),
    #[error("region error {0:?}")]
    RegionError(kvproto::errorpb::Error),
    #[error("key is locked {0:?}")]
    KeyIsLocked(kvproto::kvrpcpb::LockInfo),
    #[error("{0}")]
    Other(String),
}

impl From<String> for Error {
    fn from(error: String) -> Self {
        Error::Other(error)
    }
}

impl From<&str> for Error {
    fn from(error: &str) -> Self {
        Error::Other(error.to_string())
    }
}

impl From<hyper::Error> for Error {
    fn from(error: hyper::Error) -> Self {
        Error::Other(error.to_string())
    }
}

impl From<HttpClientError> for Error {
    fn from(error: HttpClientError) -> Self {
        Error::Other(error.to_string())
    }
}

impl From<kvengine::Error> for Error {
    fn from(error: kvengine::Error) -> Self {
        Error::Other(error.to_string())
    }
}

pub struct CloudEngineBackends {
    pub dfs: Arc<dyn Dfs>,
    pub pd_client: Arc<dyn PdClient>,
    pub master_key: MasterKey,
    pub txn_chunk_mgr: TxnChunkManager,
}

#[derive(Clone, Copy, Debug)]
struct SchemaFileCacheEntry {
    latest_version: i64,
    last_access_seq: u64,
}

#[derive(Debug, Default)]
struct SchemaFileCacheState {
    access_seq: u64,
    keyspaces: HashMap<u32, SchemaFileCacheEntry>,
}

#[derive(Clone, Copy, Debug)]
struct LatestSchemaFile {
    file_id: u64,
    version: i64,
}

#[derive(Clone)]
struct RegionBucketCacheEntry {
    region_ver: u64,
    keys: Vec<Vec<u8>>,
}

impl From<&BucketStat> for RegionBucketCacheEntry {
    fn from(bucket_stat: &BucketStat) -> Self {
        Self {
            region_ver: bucket_stat.meta.region_epoch.get_version(),
            keys: bucket_stat.meta.keys.clone(),
        }
    }
}

#[derive(Clone)]
pub struct PdClientWithCache {
    pd_client: Arc<dyn PdClient>,
    store_cache: Arc<DashMap<u64, Store>>, // store_id -> Store
    region_cache: Arc<DashMap<u64, Peer>>, // region_id -> Peer
    region_bucket_cache: Arc<DashMap<u64, RegionBucketCacheEntry>>, // region_id -> bucket keys
}

impl PdClientWithCache {
    pub fn new(pd_client: Arc<dyn PdClient>) -> PdClientWithCache {
        PdClientWithCache {
            pd_client,
            store_cache: Arc::new(DashMap::new()),
            region_cache: Arc::new(DashMap::new()),
            region_bucket_cache: Arc::new(DashMap::new()),
        }
    }

    // Get the leader store by region id.
    pub async fn get_leader_store_by_region_id(&self, region_id: u64) -> Result<Store, Error> {
        // First check if we have the peer in cache
        if let Some(peer) = self.region_cache.get(&region_id) {
            let peer = peer.clone();
            let store_id = peer.get_store_id();

            // Check if we have the store in cache
            if let Some(store) = self.store_cache.get(&store_id) {
                return Ok(store.clone());
            }

            // Get store and update cache
            let store = self.pd_client.get_store(store_id)?;
            self.store_cache.insert(store_id, store.clone());
            return Ok(store);
        }

        // If not in cache, fetch from PD
        let (_, peer) = self
            .pd_client
            .get_region_leader_by_id(region_id)
            .await?
            .ok_or_else(|| {
                let mut error = kvproto::errorpb::Error::default();
                error.mut_region_not_found().set_region_id(region_id);
                Error::RegionError(error)
            })?;

        // Update cache after async operation
        self.region_cache.insert(region_id, peer.clone());

        let store_id = peer.get_store_id();
        let store = self.pd_client.get_store(store_id)?;
        self.store_cache.insert(store_id, store.clone());

        Ok(store)
    }

    pub fn evict_region_cache(&self, region_id: u64) {
        self.region_cache.remove(&region_id);
        self.region_bucket_cache.remove(&region_id);
    }

    pub fn get_security_mgr(&self) -> Arc<SecurityManager> {
        self.pd_client.get_security_mgr()
    }

    pub fn get_region_bucket_keys(&self, region_id: u64, region_ver: u64) -> Vec<Vec<u8>> {
        if let Some(bucket_entry) = self.region_bucket_cache.get(&region_id) {
            match bucket_entry.region_ver.cmp(&region_ver) {
                std::cmp::Ordering::Equal => return bucket_entry.keys.clone(),
                std::cmp::Ordering::Greater => return Vec::new(),
                std::cmp::Ordering::Less => {}
            }
        }

        let Ok(Some(bucket_stat)) =
            futures::executor::block_on(self.pd_client.get_buckets_async(region_id))
        else {
            self.region_bucket_cache.remove(&region_id);
            return Vec::new();
        };
        let bucket_entry = RegionBucketCacheEntry::from(&bucket_stat);
        let bucket_keys = if bucket_entry.region_ver == region_ver {
            bucket_entry.keys.clone()
        } else {
            Vec::new()
        };
        self.region_bucket_cache.insert(region_id, bucket_entry);
        bucket_keys
    }
}

#[derive(Clone)]
pub struct CloudHelper {
    dfs: Arc<dyn Dfs>,
    master_key: MasterKey,
    txn_chunk_mgr: TxnChunkManager,
    pd_client: Arc<PdClientWithCache>,
    vector_index_cache: VectorIndexCache,
    columnar_file_cache: ColumnarFileCache,
    fts_cache: FtsCache,
    fts_delta_cache: FtsDeltaCache,
    block_cache: BlockCache,
    snapshot_cache: SnapCache,
    snapshot_cache_capable_stores: Arc<DashMap<u64, ()>>,
    shared_snap_access_cache: SharedSnapAccessCache,
    meta_file_cache: Arc<Cache<u64, Arc<dyn File>, MetaFileCacheWeighter>>,
    schema_files: Arc<DashMap<u64, SchemaFile>>,
    schema_file_cache_state: Arc<Mutex<SchemaFileCacheState>>,
    schema_file_cache_max_keyspaces: usize,
    runtime: Arc<tokio::runtime::Runtime>,
    read_concurrency: usize,
    ia_ctx: IaCtx,
    http_client: RefreshingHttpClient,
}

impl CloudHelper {
    pub fn new(
        backends: CloudEngineBackends,
        data_dir: &PathBuf,
        ia: &IaConfig,
        vector_index_config: VectorIndexConfig,
        columnar_file_cache_config: ColumnarFileCacheConfig,
        fts_cache_config: FtsCacheConfig,
        fts_delta_cache_config: FtsDeltaCacheConfig,
        block_cache_size: AbsoluteOrPercentSize,
        schema_file_cache_max_keyspaces: usize,
    ) -> Self {
        let thread_count = std::cmp::max(4, (SysQuota::cpu_cores_quota() * 7.0 / 8.0) as usize);
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .thread_name("cloud_helper")
                .worker_threads(thread_count)
                .enable_all()
                .build()
                .unwrap(),
        );
        let dir = vec![data_dir.join("ia")];
        let ia_mgr = build_ia_mgr(backends.dfs.clone(), runtime.clone(), dir.clone(), ia);
        let ia_ctx = IaCtx::Enabled(ia_mgr.clone(), Arc::new(dir));
        let vector_index_cache =
            VectorIndexCache::new(vector_index_config, runtime.handle().clone(), ia_mgr);
        let cache_cap = columnar_file_cache_config.cache_cap.as_memory_size();
        let columnar_file_cache =
            ColumnarFileCache::new(columnar_file_cache_config.cache_size, cache_cap);
        // Use the same cache capacity for meta file cache.
        let meta_file_cache = new_meta_file_cache(cache_cap);
        let fts_cache = FtsCache::new(fts_cache_config, runtime.handle().clone());
        let fts_delta_cache = FtsDeltaCache::new(fts_delta_cache_config.cache_cap.as_memory_size());
        let block_cache = BlockCache::new(
            BlockCacheType::Quick,
            block_cache_size.as_memory_size(),
            64 * 1024,
        );
        let snapshot_cache = SnapCache::new(SNAPSHOT_CACHE_SIZE, SNAPSHOT_CACHE_CAP);
        let snapshot_cache_capable_stores = Arc::new(DashMap::new());
        let shared_snap_access_cache = SharedSnapAccessCache::new();

        // Reuse HTTP connections, but rebuild the client when TLS certs rotate.
        let http_client = RefreshingHttpClient::new(backends.pd_client.get_security_mgr());

        Self {
            dfs: backends.dfs,
            master_key: backends.master_key,
            txn_chunk_mgr: backends.txn_chunk_mgr,
            pd_client: Arc::new(PdClientWithCache::new(backends.pd_client)),
            vector_index_cache,
            columnar_file_cache,
            fts_cache,
            fts_delta_cache,
            block_cache,
            snapshot_cache,
            snapshot_cache_capable_stores,
            shared_snap_access_cache,
            meta_file_cache,
            schema_files: Arc::new(DashMap::new()),
            schema_file_cache_state: Arc::new(Mutex::new(SchemaFileCacheState::default())),
            schema_file_cache_max_keyspaces,
            runtime,
            read_concurrency: thread_count,
            ia_ctx,
            http_client,
        }
    }
}

#[derive(Debug)]
struct IaMetaFileEntry {
    file_id: u64,
    path: PathBuf,
    size: u64,
    modified: Duration,
}

impl CloudHelper {
    pub fn gc_ia_meta_files(&self, meta_cap: u64) {
        let IaCtx::Enabled(ia_mgr, meta_paths) = &self.ia_ctx else {
            return;
        };

        if meta_cap == 0 {
            return;
        }

        let mut entries = match collect_ia_meta_files(meta_paths) {
            Ok(entries) => entries,
            Err(err) => {
                error!("ia meta gc scan failed"; "err" => ?err);
                return;
            }
        };
        let total_size: u64 = entries.iter().map(|entry| entry.size).sum();
        if total_size <= meta_cap {
            return;
        }

        entries.sort_by(|a, b| {
            a.modified
                .cmp(&b.modified)
                .then_with(|| a.file_id.cmp(&b.file_id))
                .then_with(|| a.path.cmp(&b.path))
        });
        let mut current_size = total_size;
        let mut removed_files = 0;
        let mut removed_bytes = 0;
        for entry in entries {
            if current_size <= meta_cap {
                break;
            }

            match remove_ia_meta_file(ia_mgr, &entry.path, entry.file_id) {
                Ok(true) => {
                    current_size = current_size.saturating_sub(entry.size);
                    removed_files += 1;
                    removed_bytes += entry.size;
                }
                Ok(false) => {}
                Err(err) => {
                    error!(
                        "ia meta gc remove failed";
                        "file_id" => entry.file_id,
                        "path" => ?entry.path,
                        "err" => ?err,
                    );
                }
            }
        }

        if removed_files > 0 {
            info!(
                "ia meta gc reclaimed files to satisfy cap";
                "meta_cap" => meta_cap,
                "removed_files" => removed_files,
                "removed_bytes" => removed_bytes,
                "size_before" => total_size,
                "size_after" => current_size,
            );
        }
    }

    pub fn gc_schema_file_cache(&self) {
        gc_schema_file_cache_global(
            &self.schema_files,
            &self.schema_file_cache_state,
            self.schema_file_cache_max_keyspaces,
        );
    }

    pub fn make_columnar_reader(
        &self,
        shard_id: u64,
        shard_ver: u64,
        start_ts: u64,
        tables: Vec<TableCtx>,
        columns: &[ColumnInfo],
        table_scan: tipb::Executor,
        filter_conditions: Vec<tipb::Expr>,
        ann_query_info: tipb::AnnQueryInfo,
        fts_query_info: tipb::FtsQueryInfo,
    ) -> Result<CloudColumnarReaders, Error> {
        let dfs = self.dfs.clone();
        let pd_client = self.pd_client.clone();
        let schema_files = self.schema_files.clone();
        let schema_file_cache_state = self.schema_file_cache_state.clone();
        let txn_mgr = self.txn_chunk_mgr.clone();
        let master_key = self.master_key.clone();
        let ia_ctx = self.ia_ctx.clone();
        let http_client = self.http_client.clone();
        info!(
            "make_columnar_reader, start_ts: {:?}, filter_conditions: {:?}, num_tables: {:?}",
            start_ts,
            filter_conditions,
            tables.len()
        );
        let scan_ctx = TableScanCtx::new(table_scan, filter_conditions);
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let start = Instant::now_coarse();
        let vector_index_cache = self.vector_index_cache.clone();
        let snap_cache = self.snapshot_cache.clone();
        let snap_cache_capable_stores = self.snapshot_cache_capable_stores.clone();
        let shared_snap_access_cache = self.shared_snap_access_cache.clone();
        let meta_file_cache = self.meta_file_cache.clone();
        let columnar_file_cache = self.columnar_file_cache.clone();
        let fts_cache = self.fts_cache.clone();
        let fts_delta_cache = self.fts_delta_cache.clone();
        let block_cache = self.block_cache.clone();
        let tables_clone = tables.clone();
        let fts_query_info_clone = fts_query_info.clone();
        self.runtime.spawn(async move {
            let snap = get_or_request_shared_snapshot(
                shared_snap_access_cache,
                pd_client,
                http_client,
                dfs,
                ia_ctx,
                vector_index_cache,
                columnar_file_cache,
                snap_cache,
                snap_cache_capable_stores,
                meta_file_cache,
                schema_files,
                schema_file_cache_state,
                txn_mgr,
                block_cache,
                fts_cache,
                fts_delta_cache,
                shard_id,
                shard_ver,
                start_ts,
                tables_clone,
                &master_key,
                fts_query_info_clone,
            )
            .await;
            tx.send(snap).unwrap();
        });
        match rx.recv() {
            Ok(Ok(snap)) => {
                info!(
                    "begin to make columnar reader, elapsed: {:?}",
                    start.saturating_elapsed_secs()
                );
                let ann_query_info_opt = (ann_query_info.get_query_type()
                    != tipb::AnnQueryType::InvalidQueryType)
                    .then_some(ann_query_info);
                let fts_query_info_opt = (fts_query_info.get_query_type()
                    != tipb::FtsQueryType::FtsQueryTypeInvalid)
                    .then_some(fts_query_info);
                let reader = CloudColumnarReaders::new(
                    self.runtime.handle().clone(),
                    snap,
                    tables,
                    columns.to_vec(),
                    scan_ctx,
                    ann_query_info_opt,
                    fts_query_info_opt,
                    self.ia_ctx.clone(),
                    start_ts,
                    self.read_concurrency,
                )?;
                let elapsed = start.saturating_elapsed_secs();
                info!("make columnar reader"; "shard_id" => shard_id, "shard_ver" => shard_ver, "cost" => elapsed);
                COLUMNAR_FETCH_SNAPSHOT_HISTOGRAM.observe(elapsed);
                Ok(reader)
            }
            Ok(Err(e)) => {
                error!("failed to get snapshot"; "err" => ?e);
                Err(e)
            }
            Err(e) => {
                error!("failed to recv"; "err" => e.to_string());
                Err(Error::Other(e.to_string()))
            }
        }
    }

    pub fn get_region_bucket_keys(&self, region_id: u64, region_ver: u64) -> Vec<Vec<u8>> {
        self.pd_client.get_region_bucket_keys(region_id, region_ver)
    }

    pub fn clear_shared_snap_access_by_start_ts(&self, start_ts: u64) {
        if start_ts == 0 {
            return;
        }

        let (cleared_cache_entries, in_flight_loader_entries) =
            self.shared_snap_access_cache.remove_by_start_ts(start_ts);

        info!(
            "clear shared snapaccess by start_ts, start_ts: {}, cleared_cache_entries: {}, in_flight_loader_entries: {}",
            start_ts, cleared_cache_entries, in_flight_loader_entries
        );
    }
}

fn collect_ia_meta_files(meta_paths: &[PathBuf]) -> std::io::Result<Vec<IaMetaFileEntry>> {
    let mut entries = Vec::new();
    for meta_path in meta_paths {
        if !meta_path.is_dir() {
            continue;
        }
        for entry in fs::read_dir(meta_path)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let filename = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            let Some((file_id, _)) = parse_table_meta_filename(filename) else {
                continue;
            };
            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(err) => {
                    error!("ia meta gc get metadata failed"; "path" => ?path, "err" => ?err);
                    continue;
                }
            };
            entries.push(IaMetaFileEntry {
                file_id,
                path,
                size: metadata.len(),
                modified: metadata
                    .modified()
                    .ok()
                    .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
                    .unwrap_or(Duration::ZERO),
            });
        }
    }
    Ok(entries)
}

fn remove_ia_meta_file(ia_mgr: &IaManager, path: &Path, file_id: u64) -> std::io::Result<bool> {
    ia_mgr.remove_table_meta(file_id);
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

fn build_ia_mgr(
    dfs: Arc<dyn dfs::Dfs>,
    runtime: Arc<tokio::runtime::Runtime>,
    data_dirs: Vec<PathBuf>,
    ia: &IaConfig,
) -> IaManager {
    // Create the data directory if it doesn't exist.
    for data_dir in data_dirs.iter() {
        if !data_dir.exists() {
            std::fs::create_dir_all(data_dir.as_path()).unwrap();
        }
    }
    let options = ia.to_manager_options(data_dirs).unwrap();
    let handle = runtime.handle().clone();
    let fd_cache = FdCache::new(ia.fd_cache_capacity);
    let mgr = IaManager::new(options, dfs, Some(fd_cache), handle.into()).unwrap();
    mgr
}

async fn request_snapshot_from_leader(
    pd_client: Arc<PdClientWithCache>,
    http_client: RefreshingHttpClient,
    dfs: Arc<dyn dfs::Dfs>,
    ia_ctx: IaCtx,
    vector_index_cache: VectorIndexCache,
    columnar_file_cache: ColumnarFileCache,
    snap_cache: SnapCache,
    snap_cache_capable_stores: Arc<DashMap<u64, ()>>,
    meta_file_cache: Arc<Cache<u64, Arc<dyn File>, MetaFileCacheWeighter>>,
    schema_files: Arc<DashMap<u64, SchemaFile>>,
    schema_file_cache_state: Arc<Mutex<SchemaFileCacheState>>,
    txn_chunk_manager: TxnChunkManager,
    block_cache: BlockCache,
    fts_cache: FtsCache,
    fts_delta_cache: FtsDeltaCache,
    shard_id: u64,
    shard_ver: u64,
    start_ts: u64,
    tables: &[TableCtx],
    master_key: &MasterKey,
    fts_query_info: tipb::FtsQueryInfo,
) -> Result<SnapAccess, Error> {
    let mut last_err = None;
    let mut leader_changed = true;
    let tag = format!("{}:{}", shard_id, shard_ver);
    let mut store = None;
    let mut backoff = tikv_util::backoff::ExponentialBackoff::new(
        BACKOFF_INITIAL,
        BACKOFF_MAX,
        BACKOFF_RETRY_COUNT,
    );
    let start = Instant::now_coarse();
    let start_table_id = tables[0].table_id;
    let end_table_id = tables[tables.len() - 1].table_id;
    let keyspace_prefix = tables[0].ranges[0].get_low()[..KEYSPACE_PREFIX_LEN].to_vec();
    let start_table_id_prefix = vec![
        keyspace_prefix.clone(),
        encode_table_prefix_key(start_table_id).to_vec(),
    ]
    .concat();
    let end_table_id_prefix = vec![
        keyspace_prefix,
        encode_table_prefix_key(end_table_id).to_vec(),
    ]
    .concat();
    let start_key = hex::encode_upper(start_table_id_prefix.to_vec());
    let end_key = hex::encode_upper(next_key(&end_table_id_prefix.to_vec()));
    while let Ok(next_delay) = backoff.next_delay() {
        if leader_changed {
            let store_res = pd_client.get_leader_store_by_region_id(shard_id).await;
            match store_res {
                Ok(s) => store = Some(s),
                Err(Error::RegionError(err)) if err.has_region_not_found() => {
                    error!("{} get leader store failed, region not found", tag);
                    pd_client.evict_region_cache(shard_id);
                    return Err(Error::RegionError(err));
                }
                Err(e) => {
                    error!(
                        "{} get leader store failed, other error {:?}, will retry",
                        tag, e
                    );
                    last_err = Some(e);
                    pd_client.evict_region_cache(shard_id);
                    COLUMNAR_FETCH_SNAPSHOT_RETRY_COUNTER.inc();
                    tokio::time::sleep(next_delay).await;
                    continue;
                }
            }
        }

        // Try to get snapshot from local cache first.
        let snap_cache_key = SnapCacheKey::new(shard_id, shard_ver, start_table_id, end_table_id);
        let store_id = store.as_ref().unwrap().get_id();
        let snap_cache_supported = snap_cache_capable_stores.contains_key(&store_id);
        let snap_entry = if snap_cache_supported {
            snap_cache.get(&snap_cache_key)
        } else {
            None
        };
        let mut uri_str = format!(
            "{}/kvengine/snapshot/{}?start_ts={}&shard_ver={}&start_key={}&end_key={}",
            store.as_ref().unwrap().status_address,
            shard_id,
            start_ts,
            shard_ver,
            start_key,
            end_key
        );
        let cache_resp = if let Some(SnapCacheEntry {
            snap,
            meta_seq,
            write_seq,
        }) = snap_entry
        {
            uri_str = format!("{}&meta_seq={}&write_seq={}", uri_str, meta_seq, write_seq);
            snap
        } else {
            Bytes::new()
        };

        let security_mgr = pd_client.get_security_mgr();
        let uri = security_mgr.build_uri(&uri_str).unwrap();
        let req = Request::get(uri.clone()).body(Body::empty()).unwrap();
        match send_req_to_store(&http_client, req).await {
            Ok((resp, cache_valid, server_supports_snap_cache)) => {
                if server_supports_snap_cache {
                    snap_cache_capable_stores.insert(store_id, ());
                } else if snap_cache_supported {
                    snap_cache_capable_stores.remove(&store_id);
                    if cache_valid {
                        warn!(
                            "{} store does not support snapshot cache but returned cache valid, ignore cache and retry, uri: {}",
                            tag, uri_str
                        );
                        tokio::time::sleep(next_delay).await;
                        continue;
                    }
                }
                let snap_data = if cache_valid { cache_resp } else { resp };
                let mut delegate_resp = DelegateResponse::default();
                delegate_resp.merge_from_bytes(&snap_data).unwrap();
                if delegate_resp.get_region_error().has_not_leader() {
                    error!("{} request_snapshot_from_leader failed, not leader", tag);
                    pd_client.evict_region_cache(shard_id);
                    leader_changed = true;
                    last_err = Some(Error::RegionError(delegate_resp.take_region_error()));
                    tokio::time::sleep(next_delay).await;
                    continue;
                }
                if delegate_resp.get_region_error().has_epoch_not_match() {
                    // Return epoch not match error to caller to retry new plan.
                    error!(
                        "{} request_snapshot_from_leader failed, epoch not match, {:?}",
                        tag,
                        delegate_resp.get_region_error()
                    );
                    return Err(Error::RegionError(delegate_resp.take_region_error()));
                }
                if delegate_resp.has_locked() {
                    error!("{} request_snapshot_from_leader failed, has locked", tag);
                    let lock_info = delegate_resp.take_locked();
                    return Err(Error::KeyIsLocked(lock_info));
                }
                let elapsed = start.saturating_elapsed_secs();
                let snap_bytes = delegate_resp.get_snapshot();
                let mut cs = kvenginepb::ChangeSet::default();
                cs.merge_from_bytes(&snap_bytes).unwrap();
                info!(
                    "{} request_snapshot_from_leader, uri: {}, cache_hit: {}, resp size: {}, memtable size: {}, snapshot size: {}, cost: {:?}, snapshot: {:?}",
                    tag,
                    uri_str,
                    cache_valid,
                    snap_data.len(),
                    delegate_resp.get_mem_table_data().len(),
                    snap_bytes.len(),
                    elapsed,
                    cs.get_snapshot(),
                );

                let prepare_type =
                    if fts_query_info.get_query_type() == tipb::FtsQueryType::FtsQueryTypeInvalid {
                        PrepareType::ColumnarOnly
                    } else {
                        PrepareType::All
                    };

                let snap_ctx = SnapCtx {
                    dfs: dfs.clone(),
                    master_key: master_key.clone(),
                    block_cache,
                    vector_index_cache: Some(vector_index_cache),
                    columnar_file_cache: Some(columnar_file_cache),
                    fts_cache,
                    fts_delta_cache,
                    meta_file_cache,
                    schema_files: Some(schema_files.clone()),
                    txn_chunk_manager,
                    ia_ctx,
                    prepare_type,
                    read_columnar: true,
                    columnar_meta_cache: ColumnarMetaCache::default(),
                    encryption_key_manager: Arc::new(cloud_encryption::EncryptionKeyManager::new()),
                    strict_file_memory_quota_wait_timeout: Duration::from_secs(30),
                    strict_file_memory_quota: Arc::new(MemoryQuota::new(usize::MAX)),
                };
                let memory_limiter = MemoryLimiter::new(u64::MAX, None);
                let (snap, _) = SnapAccess::construct_snapshot(
                    &tag,
                    &snap_ctx,
                    delegate_resp.get_mem_table_data(),
                    &snap_bytes,
                    memory_limiter,
                )
                .await?;
                if let Some(keyspace_id) = get_snapshot_keyspace_id(tables) {
                    touch_schema_file_cache_keyspace(&schema_file_cache_state, keyspace_id);
                }
                // Fill cache if not hit.
                if !cache_valid && server_supports_snap_cache {
                    snap_cache.insert(
                        SnapCacheKey::new(shard_id, shard_ver, start_table_id, end_table_id),
                        SnapCacheEntry {
                            snap: snap_data,
                            meta_seq: cs.get_sequence(),
                            write_seq: delegate_resp.get_mem_table_sequence(),
                        },
                    );
                }
                return Ok(snap);
            }
            Err(err) => {
                error!(
                    "{} request_snapshot_from_leader failed, other error: {:?}, will retry",
                    tag, err
                );
                last_err = Some(err);
                // region maybe merged and destroyed, set leader_changed and get region from pd.
                leader_changed = true;
                pd_client.evict_region_cache(shard_id);
                COLUMNAR_FETCH_SNAPSHOT_RETRY_COUNTER.inc();
                tokio::time::sleep(next_delay).await;
            }
        }
    }
    error!("get snapshot failed"; "shard_id" => shard_id, "shard_ver" => shard_ver, "tag" => &tag, "err" => ?last_err);
    Err(last_err.unwrap())
}

async fn send_req_to_store(
    http_client: &RefreshingHttpClient,
    req: Request<Body>,
) -> Result<
    (
        Bytes,
        bool, // cache valid
        bool, // cache supported
    ),
    Error,
> {
    let uri = req.uri().clone();
    let resp = http_client.request(req).await?;
    let cache_supported = resp
        .headers()
        .get(SNAPSHOT_CACHE_CAPABILITY_HEADER)
        .and_then(|value| value.to_str().ok())
        == Some("1");
    if resp.status() == hyper::StatusCode::NOT_MODIFIED {
        return Ok((Bytes::new(), true, cache_supported));
    }
    if !resp.status().is_success() {
        let status = resp.status();
        let body = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        return Err(Error::Other(format!("{:?} {:?}: {:?}", uri, status, body)));
    }
    match hyper::body::to_bytes(resp.into_body()).await {
        Ok(body) => Ok((body, false, cache_supported)),
        Err(e) => Err(Error::Other(format!("{:?}", e))),
    }
}

#[derive(Clone)]
pub struct SnapCache {
    core: Arc<SnapCacheCore>,
}

impl Deref for SnapCache {
    type Target = SnapCacheCore;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl SnapCache {
    pub fn new(size: usize, cap: u64) -> Self {
        Self {
            core: Arc::new(SnapCacheCore::new(size, cap)),
        }
    }
}

#[derive(Clone)]
pub struct SnapCacheCore {
    cache: Arc<Cache<SnapCacheKey, SnapCacheEntry, SnapWeighter, DefaultHashBuilder>>,
}

impl SnapCacheCore {
    pub fn new(size: usize, cap: u64) -> Self {
        let opts = quick_cache::OptionsBuilder::new()
            .weight_capacity(cap)
            .estimated_items_capacity(size)
            .build()
            .unwrap();

        let cache = Arc::new(Cache::with_options(
            opts,
            SnapWeighter,
            DefaultHashBuilder::default(),
            DefaultLifecycle::default(),
        ));
        Self { cache }
    }

    pub fn get(&self, key: &SnapCacheKey) -> Option<SnapCacheEntry> {
        self.cache.get(key)
    }

    pub fn insert(&self, key: SnapCacheKey, entry: SnapCacheEntry) {
        self.cache.insert(key, entry);
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct SnapCacheKey {
    pub shard_id: u64,
    pub shard_ver: u64,
    pub start_table_id: i64,
    pub end_table_id: i64,
}

impl SnapCacheKey {
    pub fn new(shard_id: u64, shard_ver: u64, start_table_id: i64, end_table_id: i64) -> Self {
        Self {
            shard_id,
            shard_ver,
            start_table_id,
            end_table_id,
        }
    }
}

#[derive(Clone)]
pub struct SnapCacheEntry {
    pub snap: Bytes,
    pub meta_seq: u64,
    pub write_seq: u64,
}

impl SnapCacheEntry {
    pub fn new(meta_seq: u64, write_seq: u64, snap: Bytes) -> Self {
        Self {
            snap,
            meta_seq,
            write_seq,
        }
    }
}

#[derive(Clone)]
pub struct SnapWeighter;

impl quick_cache::Weighter<SnapCacheKey, SnapCacheEntry> for SnapWeighter {
    fn weight(&self, _key: &SnapCacheKey, val: &SnapCacheEntry) -> u64 {
        val.snap.len() as u64
    }
}

#[derive(Clone)]
pub struct SharedSnapAccessCache {
    core: Arc<SharedSnapAccessCacheCore>,
}

impl Deref for SharedSnapAccessCache {
    type Target = SharedSnapAccessCacheCore;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl SharedSnapAccessCache {
    pub fn new() -> Self {
        Self {
            core: Arc::new(SharedSnapAccessCacheCore::new()),
        }
    }
}

#[derive(Clone)]
pub struct SharedSnapAccessCacheCore {
    groups: Arc<DashMap<u64, Arc<SharedSnapAccessGroup>>>,
}

impl SharedSnapAccessCacheCore {
    pub fn new() -> Self {
        Self {
            groups: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, key: &SharedSnapAccessKey) -> Option<Weak<kvengine::SnapAccessCore>> {
        let group = self.groups.get(&key.start_ts).map(|entry| entry.clone())?;
        let _state_guard = group.state_lock.lock().unwrap();
        if group.is_terminal() {
            return None;
        }
        group.entries.get(key).map(|entry| entry.clone())
    }

    pub fn insert(&self, key: SharedSnapAccessKey, entry: Weak<kvengine::SnapAccessCore>) {
        let Some(group) = self.groups.get(&key.start_ts).map(|entry| entry.clone()) else {
            return;
        };
        let _state_guard = group.state_lock.lock().unwrap();
        if group.is_terminal() {
            return;
        }
        group.entries.insert(key, entry);
    }

    pub fn get_loader(&self, key: &SharedSnapAccessKey) -> Option<Arc<tokio::sync::Mutex<()>>> {
        let group = match self.groups.entry(key.start_ts) {
            dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(Arc::new(SharedSnapAccessGroup::new())).clone()
            }
        };
        let _state_guard = group.state_lock.lock().unwrap();
        if group.is_terminal() {
            return None;
        }
        let loader = group
            .loaders
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        Some(loader)
    }

    pub fn remove_loader(&self, key: &SharedSnapAccessKey) -> bool {
        let Some(group) = self.groups.get(&key.start_ts).map(|entry| entry.clone()) else {
            return false;
        };
        let _state_guard = group.state_lock.lock().unwrap();
        let removed = group.loaders.remove(key).is_some();
        drop(_state_guard);
        self.try_remove_empty_group(key.start_ts, &group);
        removed
    }

    pub fn remove_entry(&self, key: &SharedSnapAccessKey) -> bool {
        let Some(group) = self.groups.get(&key.start_ts).map(|entry| entry.clone()) else {
            return false;
        };
        let _state_guard = group.state_lock.lock().unwrap();
        let removed = group.entries.remove(key).is_some();
        drop(_state_guard);
        self.try_remove_empty_group(key.start_ts, &group);
        removed
    }

    pub fn remove_by_start_ts(&self, start_ts: u64) -> (usize, usize) {
        let Some(group) = self.groups.get(&start_ts).map(|entry| entry.clone()) else {
            return (0, 0);
        };
        let _state_guard = group.state_lock.lock().unwrap();
        group.mark_terminal();
        let removed_entries = group.entries.len();
        let in_flight_loaders = group.loaders.len();
        group.entries.clear();
        drop(_state_guard);
        self.try_remove_empty_group(start_ts, &group);
        (removed_entries, in_flight_loaders)
    }

    fn try_remove_empty_group(&self, start_ts: u64, group: &Arc<SharedSnapAccessGroup>) {
        let _state_guard = group.state_lock.lock().unwrap();
        if group.entries.is_empty() && group.loaders.is_empty() {
            if let Some(entry) = self.groups.get(&start_ts) {
                if Arc::ptr_eq(entry.value(), group) {
                    drop(entry);
                    let _ = self.groups.remove(&start_ts);
                }
            }
        }
    }
}

pub struct SharedSnapAccessGroup {
    entries: DashMap<SharedSnapAccessKey, Weak<kvengine::SnapAccessCore>>,
    loaders: DashMap<SharedSnapAccessKey, Arc<tokio::sync::Mutex<()>>>,
    terminal: AtomicBool,
    state_lock: Mutex<()>,
}

impl SharedSnapAccessGroup {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            loaders: DashMap::new(),
            terminal: AtomicBool::new(false),
            state_lock: Mutex::new(()),
        }
    }

    fn is_terminal(&self) -> bool {
        self.terminal.load(Ordering::Acquire)
    }

    fn mark_terminal(&self) {
        self.terminal.store(true, Ordering::Release);
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct SharedSnapAccessKey {
    pub shard_id: u64,
    pub shard_ver: u64,
    pub start_ts: u64,
    pub start_table_id: i64,
    pub end_table_id: i64,
    pub prepare_all: bool,
}

impl SharedSnapAccessKey {
    pub fn new(
        shard_id: u64,
        shard_ver: u64,
        start_ts: u64,
        start_table_id: i64,
        end_table_id: i64,
        prepare_all: bool,
    ) -> Self {
        Self {
            shard_id,
            shard_ver,
            start_ts,
            start_table_id,
            end_table_id,
            prepare_all,
        }
    }
}

fn upgrade_shared_snap_access(
    cache: &SharedSnapAccessCache,
    key: &SharedSnapAccessKey,
) -> Option<SnapAccess> {
    let core = match cache.get(key) {
        Some(core) => match core.upgrade() {
            Some(core) => core,
            None => {
                cache.remove_entry(key);
                return None;
            }
        },
        None => return None,
    };
    Some(SnapAccess { core })
}

async fn get_or_request_shared_snapshot(
    shared_snap_access_cache: SharedSnapAccessCache,
    pd_client: Arc<PdClientWithCache>,
    http_client: RefreshingHttpClient,
    dfs: Arc<dyn dfs::Dfs>,
    ia_ctx: IaCtx,
    vector_index_cache: VectorIndexCache,
    columnar_file_cache: ColumnarFileCache,
    snap_cache: SnapCache,
    snap_cache_capable_stores: Arc<DashMap<u64, ()>>,
    meta_file_cache: Arc<Cache<u64, Arc<dyn File>, MetaFileCacheWeighter>>,
    schema_files: Arc<DashMap<u64, SchemaFile>>,
    schema_file_cache_state: Arc<Mutex<SchemaFileCacheState>>,
    txn_chunk_manager: TxnChunkManager,
    block_cache: BlockCache,
    fts_cache: FtsCache,
    fts_delta_cache: FtsDeltaCache,
    shard_id: u64,
    shard_ver: u64,
    start_ts: u64,
    tables: Vec<TableCtx>,
    master_key: &MasterKey,
    fts_query_info: tipb::FtsQueryInfo,
) -> Result<SnapAccess, Error> {
    let start_table_id = tables[0].table_id;
    let end_table_id = tables[tables.len() - 1].table_id;
    let prepare_all = fts_query_info.get_query_type() != tipb::FtsQueryType::FtsQueryTypeInvalid;
    let key = SharedSnapAccessKey::new(
        shard_id,
        shard_ver,
        start_ts,
        start_table_id,
        end_table_id,
        prepare_all,
    );

    if let Some(snap) = upgrade_shared_snap_access(&shared_snap_access_cache, &key) {
        info!(
            "reuse shared snapaccess directly, shard_id: {}, shard_ver: {}, start_ts: {}, start_table_id: {}, end_table_id: {}",
            shard_id, shard_ver, start_ts, start_table_id, end_table_id
        );
        return Ok(snap);
    }

    let Some(loader) = shared_snap_access_cache.get_loader(&key) else {
        return Err(format!("shared snapaccess evicted, start_ts: {}", start_ts).into());
    };
    let _guard = loader.lock().await;

    if let Some(snap) = upgrade_shared_snap_access(&shared_snap_access_cache, &key) {
        info!(
            "reuse shared snapaccess after wait, shard_id: {}, shard_ver: {}, start_ts: {}, start_table_id: {}, end_table_id: {}",
            shard_id, shard_ver, start_ts, start_table_id, end_table_id
        );
        return Ok(snap);
    }

    info!(
        "load shared snapaccess, shard_id: {}, shard_ver: {}, start_ts: {}, start_table_id: {}, end_table_id: {}",
        shard_id, shard_ver, start_ts, start_table_id, end_table_id
    );
    let snap = request_snapshot_from_leader(
        pd_client,
        http_client,
        dfs,
        ia_ctx,
        vector_index_cache,
        columnar_file_cache,
        snap_cache,
        snap_cache_capable_stores,
        meta_file_cache,
        schema_files,
        schema_file_cache_state,
        txn_chunk_manager,
        block_cache,
        fts_cache,
        fts_delta_cache,
        shard_id,
        shard_ver,
        start_ts,
        &tables,
        master_key,
        fts_query_info,
    )
    .await;

    if let Ok(ref snap_access) = snap {
        shared_snap_access_cache.insert(key.clone(), Arc::downgrade(&snap_access.core));
    }
    shared_snap_access_cache.remove_loader(&key);
    snap
}

fn get_snapshot_keyspace_id(tables: &[TableCtx]) -> Option<u32> {
    tables
        .first()
        .and_then(|table| table.ranges.first())
        .and_then(|range| ApiV2::get_u32_keyspace_id_by_key(range.get_low()))
}

fn touch_schema_file_cache_keyspace(
    schema_file_cache_state: &Mutex<SchemaFileCacheState>,
    active_keyspace_id: u32,
) {
    // The read path only updates recency. Stale schema files are pruned by the
    // background GC task so a keyspace may temporarily keep multiple versions.
    let mut cache_state = schema_file_cache_state.lock().unwrap();
    let access_seq = cache_state.access_seq.wrapping_add(1);
    cache_state.access_seq = access_seq;
    cache_state
        .keyspaces
        .entry(active_keyspace_id)
        .and_modify(|entry| entry.last_access_seq = access_seq)
        .or_insert(SchemaFileCacheEntry {
            latest_version: 0,
            last_access_seq: access_seq,
        });
}

fn gc_schema_file_cache_global(
    schema_files: &DashMap<u64, SchemaFile>,
    schema_file_cache_state: &Mutex<SchemaFileCacheState>,
    max_keyspaces: usize,
) {
    let latest_by_keyspace = prune_stale_schema_file_versions(schema_files);
    let mut cache_state = schema_file_cache_state.lock().unwrap();
    cache_state
        .keyspaces
        .retain(|keyspace_id, _| latest_by_keyspace.contains_key(keyspace_id));

    for (&keyspace_id, latest_schema_file) in &latest_by_keyspace {
        cache_state
            .keyspaces
            .entry(keyspace_id)
            .and_modify(|entry| entry.latest_version = latest_schema_file.version)
            .or_insert(SchemaFileCacheEntry {
                latest_version: latest_schema_file.version,
                last_access_seq: 0,
            });
    }

    if max_keyspaces == 0 {
        return;
    }

    while cache_state.keyspaces.len() > max_keyspaces {
        let Some((victim_keyspace_id, victim_entry)) = cache_state
            .keyspaces
            .iter()
            .map(|(keyspace_id, entry)| (*keyspace_id, *entry))
            .min_by_key(|(_, entry)| entry.last_access_seq)
        else {
            break;
        };
        cache_state.keyspaces.remove(&victim_keyspace_id);
        evict_schema_file_cache_keyspace(
            schema_files,
            victim_keyspace_id,
            victim_entry.latest_version,
            max_keyspaces,
        );
    }
}

fn prune_stale_schema_file_versions(
    schema_files: &DashMap<u64, SchemaFile>,
) -> HashMap<u32, LatestSchemaFile> {
    let mut latest_by_keyspace = HashMap::new();
    let mut stale_files = Vec::new();

    for entry in schema_files.iter() {
        let schema_file = entry.value();
        let keyspace_id = schema_file.get_keyspace_id();
        let candidate = LatestSchemaFile {
            file_id: *entry.key(),
            version: schema_file.get_version(),
        };

        match latest_by_keyspace.get_mut(&keyspace_id) {
            None => {
                latest_by_keyspace.insert(keyspace_id, candidate);
            }
            Some(current) if is_newer_schema_file(candidate, *current) => {
                stale_files.push((keyspace_id, current.file_id, current.version));
                *current = candidate;
            }
            Some(_) => stale_files.push((keyspace_id, candidate.file_id, candidate.version)),
        }
    }

    for (_, file_id, _) in &stale_files {
        schema_files.remove(file_id);
    }

    if !stale_files.is_empty() {
        info!(
            "removed stale schema file versions during full cache gc";
            "removed_file_count" => stale_files.len(),
            "removed_files" => ?stale_files,
        );
    }

    latest_by_keyspace
}

fn evict_schema_file_cache_keyspace(
    schema_files: &DashMap<u64, SchemaFile>,
    keyspace_id: u32,
    latest_version: i64,
    max_keyspaces: usize,
) {
    let removed_files = remove_schema_files_for_keyspace(schema_files, keyspace_id);
    if !removed_files.is_empty() {
        info!(
            "evicted schema file cache for inactive keyspace";
            "victim_keyspace_id" => keyspace_id,
            "victim_latest_version" => latest_version,
            "removed_file_count" => removed_files.len(),
            "removed_files" => ?removed_files,
            "max_keyspaces" => max_keyspaces,
        );
    }
}

fn remove_schema_files_for_keyspace(
    schema_files: &DashMap<u64, SchemaFile>,
    keyspace_id: u32,
) -> Vec<(u64, i64)> {
    let removed_files: Vec<_> = schema_files
        .iter()
        .filter_map(|entry| {
            let schema_file = entry.value();
            (schema_file.get_keyspace_id() == keyspace_id)
                .then_some((*entry.key(), schema_file.get_version()))
        })
        .collect();

    for (file_id, _) in &removed_files {
        schema_files.remove(file_id);
    }
    removed_files
}

fn is_newer_schema_file(lhs: LatestSchemaFile, rhs: LatestSchemaFile) -> bool {
    (lhs.version, lhs.file_id) > (rhs.version, rhs.file_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use kvengine::table::{file::InMemFile, schema_file::build_schema_file};
    use security::SecurityConfig;

    fn new_test_schema_file(file_id: u64, keyspace_id: u32, version: i64) -> SchemaFile {
        let schema_file_data = build_schema_file(keyspace_id, version, Vec::new(), 0);
        let file = Arc::new(InMemFile::new(file_id, Bytes::from(schema_file_data)));
        SchemaFile::open(file).unwrap()
    }

    #[test]
    fn shared_snap_access_eviction_is_sticky_for_in_flight_loader() {
        let cache = SharedSnapAccessCache::new();
        let key = SharedSnapAccessKey::new(1, 2, 3, 4, 5, false);

        let loader = cache
            .get_loader(&key)
            .expect("active group should create loader");
        cache.insert(key.clone(), Weak::new());
        assert!(cache
            .groups
            .get(&key.start_ts)
            .is_some_and(|group| group.entries.contains_key(&key)));

        let (removed_entries, in_flight_loaders) = cache.remove_by_start_ts(key.start_ts);
        assert_eq!(removed_entries, 1);
        assert_eq!(in_flight_loaders, 1);
        assert!(cache.get(&key).is_none());
        assert!(cache.get_loader(&key).is_none());

        cache.insert(key.clone(), Weak::new());
        assert!(cache
            .groups
            .get(&key.start_ts)
            .is_some_and(|group| group.entries.is_empty()));

        drop(loader);
        assert!(cache.remove_loader(&key));
        assert!(cache.groups.get(&key.start_ts).is_none());
    }

    #[test]
    fn refreshing_http_client_refreshes_on_certificate_expired_error() {
        let security_mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
        let http_client = RefreshingHttpClient::new(security_mgr);
        let uri = hyper::Uri::from_static("http://127.0.0.1/test");

        http_client.maybe_refresh_on_tls_error(
            &uri,
            "connection error: received fatal alert: CertificateExpired",
        );

        assert_eq!(http_client.refresh_count(), 1);
    }

    #[test]
    fn schema_file_cache_keeps_latest_version_per_keyspace() {
        let schema_files = DashMap::new();
        schema_files.insert(11, new_test_schema_file(11, 1, 1));
        schema_files.insert(13, new_test_schema_file(13, 1, 3));
        schema_files.insert(12, new_test_schema_file(12, 1, 2));
        schema_files.insert(21, new_test_schema_file(21, 2, 8));

        let latest = prune_stale_schema_file_versions(&schema_files);

        assert!(!schema_files.contains_key(&11));
        assert!(!schema_files.contains_key(&12));
        assert!(schema_files.contains_key(&13));
        assert!(schema_files.contains_key(&21));
        assert_eq!(latest.len(), 2);
        assert_eq!(latest.get(&1).unwrap().version, 3);
        assert_eq!(latest.get(&1).unwrap().file_id, 13);
    }

    #[test]
    fn schema_file_cache_touch_updates_lru_state() {
        let cache_state = Mutex::new(SchemaFileCacheState::default());

        touch_schema_file_cache_keyspace(&cache_state, 1);
        touch_schema_file_cache_keyspace(&cache_state, 2);
        touch_schema_file_cache_keyspace(&cache_state, 1);

        let cache_state = cache_state.lock().unwrap();
        assert_eq!(cache_state.keyspaces.len(), 2);
        assert!(cache_state.keyspaces.contains_key(&1));
        assert!(cache_state.keyspaces.contains_key(&2));
        assert_eq!(cache_state.keyspaces.get(&1).unwrap().latest_version, 0);
        assert!(
            cache_state.keyspaces.get(&1).unwrap().last_access_seq
                > cache_state.keyspaces.get(&2).unwrap().last_access_seq
        );
    }

    #[test]
    fn schema_file_cache_global_gc_removes_stale_versions_and_enforces_limit() {
        let schema_files = DashMap::new();
        let cache_state = Mutex::new(SchemaFileCacheState {
            access_seq: 3,
            keyspaces: HashMap::from([
                (
                    1,
                    SchemaFileCacheEntry {
                        latest_version: 1,
                        last_access_seq: 3,
                    },
                ),
                (
                    2,
                    SchemaFileCacheEntry {
                        latest_version: 2,
                        last_access_seq: 1,
                    },
                ),
            ]),
        });

        schema_files.insert(11, new_test_schema_file(11, 1, 1));
        schema_files.insert(12, new_test_schema_file(12, 1, 2));
        schema_files.insert(21, new_test_schema_file(21, 2, 5));
        schema_files.insert(31, new_test_schema_file(31, 3, 7));

        gc_schema_file_cache_global(&schema_files, &cache_state, 2);

        assert!(!schema_files.contains_key(&11));
        assert!(!schema_files.contains_key(&21));
        assert!(schema_files.contains_key(&12));
        assert!(schema_files.contains_key(&31));

        let cache_state = cache_state.lock().unwrap();
        assert_eq!(cache_state.keyspaces.get(&1).unwrap().latest_version, 2);
        assert!(!cache_state.keyspaces.contains_key(&2));
        assert!(cache_state.keyspaces.contains_key(&3));
    }

    #[test]
    fn schema_file_cache_global_gc_keeps_all_keyspaces_when_limit_is_zero() {
        let schema_files = DashMap::new();
        let cache_state = Mutex::new(SchemaFileCacheState::default());

        schema_files.insert(11, new_test_schema_file(11, 1, 1));
        schema_files.insert(21, new_test_schema_file(21, 2, 2));
        schema_files.insert(31, new_test_schema_file(31, 3, 3));

        gc_schema_file_cache_global(&schema_files, &cache_state, 0);

        assert!(schema_files.contains_key(&11));
        assert!(schema_files.contains_key(&21));
        assert!(schema_files.contains_key(&31));

        let cache_state = cache_state.lock().unwrap();
        assert!(cache_state.keyspaces.contains_key(&1));
        assert!(cache_state.keyspaces.contains_key(&2));
        assert!(cache_state.keyspaces.contains_key(&3));
    }
}
