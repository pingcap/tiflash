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
    collections::BTreeMap,
    ffi::{CStr, OsStr},
    fs, io,
    os::raw::{c_char, c_int},
    path::{Path, PathBuf},
    process,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Once,
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use builtin_dfs::BuiltinDfs;
use clap::{App, Arg};
use cloud_encryption::MasterKey;
use grpcio::EnvBuilder;
use kvengine::{
    dfs::{DFSConfig, Dfs, InMemFs, S3Fs},
    ia::util::IaConfig,
    table::{
        columnar::ColumnarFileCacheConfig,
        fts::{FtsCacheConfig, FtsDeltaCacheConfig},
        sstable::BlockCache,
        vector_index::VectorIndexConfig,
    },
    txn_chunk_manager::{with_pool_size, TxnChunkManager, TxnChunkManagerConfig},
};
use kvproto::{metapb, pdpb};
use pd_client::{Config as PdConfig, Error as PdError, PdClient, RpcClient};
use security::{RestfulClient, SecurityConfig, SecurityManager};
use serde::{Deserialize, Serialize};
use tikv_util::{
    config::{AbsoluteOrPercentSize, LogFormat, ReadableSize},
    logger,
    sys::disk::get_disks_stats,
};

use crate::{
    cloud_helper::{CloudEngineBackends, CloudHelper},
    columnar_impls::{
        ffi_clear_shared_snap_access_by_start_ts, ffi_columnar_scan_stats,
        ffi_get_region_bucket_keys, ffi_make_columnar_reader, ffi_physical_table_id,
        ffi_read_block, ffi_read_column, ffi_read_handle, ffi_read_version,
    },
    domain_impls::ffi_gc_rust_ptr,
    engine_store_helper::{
        get_engine_store_server_helper, init_engine_store_server_helper, EngineStoreServerHelperExt,
    },
    hub::ColumnarHub,
    interfaces_ffi::{
        CloudStorageEngineInterfaces, ConfigJsonType, EncryptionMethod, EngineStoreServerStatus,
        FileEncryptionInfoRaw, FileEncryptionRes, RaftProxyStatus, RaftStoreProxyFFIHelper,
        SSTReaderInterfaces,
    },
    server_info::ffi_server_info,
    status_server::HubStatusServer,
};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct StorageConfig {
    data_dir: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct ServerConfig {
    engine_addr: String,
    engine_version: String,
    engine_git_hash: String,
    addr: String,
    advertise_addr: String,
    status_addr: String,
    advertise_status_addr: String,
    labels: BTreeMap<String, String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            engine_addr: String::new(),
            engine_version: String::new(),
            engine_git_hash: String::new(),
            addr: "127.0.0.1:20170".to_owned(),
            advertise_addr: String::new(),
            status_addr: "127.0.0.1:20292".to_owned(),
            advertise_status_addr: String::new(),
            labels: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct HubLogFileConfig {
    filename: String,
    max_size: u64,
    max_days: u64,
    max_backups: usize,
}

impl Default for HubLogFileConfig {
    fn default() -> Self {
        Self {
            filename: String::new(),
            max_size: 300,
            max_days: 0,
            max_backups: 0,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct HubLogConfig {
    level: String,
    txn_info_logging: bool,
    format: LogFormat,
    enable_timestamp: bool,
    file: HubLogFileConfig,
}

impl Default for HubLogConfig {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
            txn_info_logging: true,
            format: LogFormat::Text,
            enable_timestamp: true,
            file: HubLogFileConfig::default(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, rename_all = "kebab-case")]
struct ConfigFile {
    pd: PdConfig,
    security: SecurityConfig,
    storage: StorageConfig,
    server: ServerConfig,
    dfs: DFSConfig,
    ia: IaConfig,
    vector_index: VectorIndexConfig,
    columnar_file_cache: ColumnarFileCacheConfig,
    fts_cache: FtsCacheConfig,
    fts_delta_cache: FtsDeltaCacheConfig,
    block_cache_size: AbsoluteOrPercentSize,
    log: HubLogConfig,
    log_level: String,
    log_file: String,
    log_format: Option<LogFormat>,
    log_rotation_size: ReadableSize,
}

#[cfg(target_os = "macos")]
fn update_default_ia_config(ia: &mut IaConfig) {
    // In MacOS, a relative directory size cannot be correctly calculated.
    // So we use a fixed value by default.
    ia.mem_cap = AbsoluteOrPercentSize::Abs(ReadableSize::mb(100));
    ia.disk_cap = AbsoluteOrPercentSize::Abs(ReadableSize::mb(100));
}

#[cfg(target_os = "linux")]
fn update_default_ia_config(ia: &mut IaConfig) {
    ia.mem_cap = AbsoluteOrPercentSize::Percent(20.0);
    ia.disk_cap = AbsoluteOrPercentSize::Percent(70.0);
}

impl Default for ConfigFile {
    fn default() -> Self {
        let mut ia = IaConfig::default();
        update_default_ia_config(&mut ia);
        Self {
            pd: PdConfig::default(),
            security: SecurityConfig::default(),
            storage: StorageConfig::default(),
            server: ServerConfig::default(),
            dfs: DFSConfig::default(),
            ia,
            vector_index: VectorIndexConfig::default(),
            columnar_file_cache: ColumnarFileCacheConfig::default(),
            fts_cache: FtsCacheConfig::default(),
            fts_delta_cache: FtsDeltaCacheConfig::default(),
            block_cache_size: AbsoluteOrPercentSize::default(),
            log: HubLogConfig::default(),
            log_level: String::default(),
            log_file: String::default(),
            log_format: Option::default(),
            log_rotation_size: ReadableSize::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ResolvedHubLogConfig {
    level: slog::Level,
    filename: String,
    format: LogFormat,
    enable_timestamp: bool,
    txn_info_logging: bool,
    max_size: u64,
    max_days: u64,
    max_backups: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct HubServerSummary<'a> {
    engine_addr: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct HubConfigSummary<'a> {
    server: HubServerSummary<'a>,
    pd_endpoints: &'a [String],
    data_dir: &'a str,
    init_only: bool,
}

const STORE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const HEARTBEAT_SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(200);
const STORE_TOMBSTONE_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const STORE_TOMBSTONE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const METRICS_PREFIX: &str = "tiflash_proxy";

fn init_metrics() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        tikv_util::metrics::monitor_process().unwrap_or_else(|err| {
            panic!("failed to start process monitor: {}", err);
        });
        tikv_util::metrics::warn_if_kernel_metrics_disabled();
        tikv_util::metrics::monitor_threads(METRICS_PREFIX).unwrap_or_else(|err| {
            panic!("failed to start thread monitor: {}", err);
        });
        tikv_util::metrics::monitor_system_psi(METRICS_PREFIX).unwrap_or_else(|err| {
            panic!("failed to start PSI monitor: {}", err);
        });
        tikv_util::metrics::monitor_allocator_stats(METRICS_PREFIX).unwrap_or_else(|err| {
            panic!("failed to monitor allocator stats: {}", err);
        });
    });
}

fn load_config(path: Option<&OsStr>) -> ConfigFile {
    path.map_or_else(ConfigFile::default, |path| {
        let path = Path::new(path);
        let data = std::fs::read_to_string(path).unwrap_or_else(|err| {
            panic!("invalid configuration file {}: {}", path.display(), err);
        });
        toml::from_str(&data).unwrap_or_else(|err| {
            panic!("invalid configuration file {}: {}", path.display(), err);
        })
    })
}

fn build_dfs(config: &ConfigFile, pd_client: Arc<dyn PdClient>) -> Arc<dyn Dfs> {
    let dfs_conf = &config.dfs;
    let dfs_s3_bucket = std::env::var("DFS_S3_BUCKET").unwrap_or_default();

    if dfs_conf.s3_endpoint == "memory" {
        info!("TiFlash Columnar Hub uses in-memory DFS backend");
        Arc::new(InMemFs::new())
    } else if dfs_conf.s3_endpoint == "local"
        || (dfs_conf.s3_bucket.is_empty()
            && dfs_conf.s3_endpoint.is_empty()
            && dfs_s3_bucket.is_empty())
    {
        info!("TiFlash Columnar Hub uses builtin DFS backend");
        Arc::new(BuiltinDfs::new(pd_client))
    } else if dfs_conf.s3_bucket.is_empty()
        && dfs_conf.s3_endpoint.is_empty()
        && !dfs_s3_bucket.is_empty()
    {
        let dfs_prefix = std::env::var("DFS_PREFIX").unwrap_or_default();
        let dfs_s3_endpoint = std::env::var("DFS_S3_ENDPOINT").unwrap_or_default();
        let dfs_s3_key_id = std::env::var("DFS_S3_KEY_ID").unwrap_or_default();
        let dfs_s3_secret_key = std::env::var("DFS_S3_SECRET_KEY").unwrap_or_default();
        let dfs_s3_region = std::env::var("DFS_S3_REGION").unwrap_or_default();
        info!(
            "TiFlash Columnar Hub uses env-overridden S3 DFS backend";
            "endpoint" => &dfs_s3_endpoint,
            "bucket" => &dfs_s3_bucket,
            "prefix" => &dfs_prefix,
        );
        Arc::new(S3Fs::new(
            dfs_prefix,
            dfs_s3_endpoint,
            dfs_s3_key_id,
            dfs_s3_secret_key,
            dfs_s3_region,
            dfs_s3_bucket,
            config.dfs.conn_options.clone(),
            true,
        ))
    } else {
        info!(
            "TiFlash Columnar Hub uses configured S3 DFS backend";
            "endpoint" => &dfs_conf.s3_endpoint,
            "bucket" => &dfs_conf.s3_bucket,
            "prefix" => &dfs_conf.prefix,
        );
        Arc::new(S3Fs::new(
            dfs_conf.prefix.clone(),
            dfs_conf.s3_endpoint.clone(),
            dfs_conf.s3_key_id.clone(),
            dfs_conf.s3_secret_key.clone(),
            dfs_conf.s3_region.clone(),
            dfs_conf.s3_bucket.clone(),
            dfs_conf.conn_options.clone(),
            true,
        ))
    }
}

fn overwrite_config_with_cmd_args(config: &mut ConfigFile, matches: &clap::ArgMatches<'_>) -> bool {
    if let Some(level) = matches.value_of("log-level") {
        config.log.level = level.to_owned();
        config.log_level.clear();
    }
    if let Some(file) = matches.value_of("log-file") {
        config.log.file.filename = file.to_owned();
        config.log_file.clear();
    }
    if let Some(data_dir) = matches.value_of("data-dir") {
        config.storage.data_dir = data_dir.to_owned();
    }
    if let Some(endpoints) = matches.values_of("pd-endpoints") {
        config.pd.endpoints = endpoints.map(ToOwned::to_owned).collect();
    }
    if let Some(ca_path) = matches.value_of("ca-path") {
        config.security.ca_path = ca_path.to_owned();
    }
    if let Some(cert_path) = matches.value_of("cert-path") {
        config.security.cert_path = cert_path.to_owned();
    }
    if let Some(key_path) = matches.value_of("key-path") {
        config.security.key_path = key_path.to_owned();
    }
    if let Some(addr) = matches.value_of("addr") {
        config.server.addr = addr.to_owned();
    }
    if let Some(advertise_addr) = matches.value_of("advertise-addr") {
        config.server.advertise_addr = advertise_addr.to_owned();
    }
    if let Some(status_addr) = matches.value_of("status-addr") {
        config.server.status_addr = status_addr.to_owned();
    }
    if let Some(advertise_status_addr) = matches.value_of("advertise-status-addr") {
        config.server.advertise_status_addr = advertise_status_addr.to_owned();
    }
    if let Some(engine_version) = matches.value_of("engine-version") {
        config.server.engine_version = engine_version.to_owned();
    }
    if let Some(engine_git_hash) = matches.value_of("engine-git-hash") {
        config.server.engine_git_hash = engine_git_hash.to_owned();
    }
    if let Some(engine_addr) = matches.value_of("engine-addr") {
        config.server.engine_addr = engine_addr.to_owned();
    }
    if let Some(engine_addr) = matches.value_of("advertise-engine-addr") {
        config.server.engine_addr = engine_addr.to_owned();
    }
    if let Some(labels) = matches.values_of("labels") {
        for label in labels {
            let (key, value) = label
                .split_once('=')
                .unwrap_or_else(|| panic!("invalid label: {}", label));
            if value.contains('=') {
                panic!("invalid label: {}", label);
            }
            config
                .server
                .labels
                .entry(key.to_owned())
                .or_insert_with(|| value.to_owned());
        }
    }
    if let Some(engine_label) = matches.value_of("engine-label") {
        config
            .server
            .labels
            .insert("engine".to_owned(), engine_label.to_owned());
    }
    matches.is_present("init-only")
}

fn resolve_hub_log_config(config: &ConfigFile) -> ResolvedHubLogConfig {
    let default_log = HubLogConfig::default();
    let default_log_file = HubLogFileConfig::default();

    let level = if config.log.level == default_log.level && !config.log_level.is_empty() {
        logger::get_level_by_string(&config.log_level).unwrap_or(slog::Level::Info)
    } else {
        logger::get_level_by_string(&config.log.level).unwrap_or(slog::Level::Info)
    };

    let filename = if config.log.file.filename.is_empty() && !config.log_file.is_empty() {
        config.log_file.clone()
    } else {
        config.log.file.filename.clone()
    };

    let format = if matches!(config.log.format, LogFormat::Text) {
        config.log_format.unwrap_or(LogFormat::Text)
    } else {
        config.log.format
    };

    let max_size = if config.log.file.max_size == default_log_file.max_size
        && config.log_rotation_size.0 > 0
    {
        config.log_rotation_size.as_mb()
    } else {
        config.log.file.max_size
    };

    ResolvedHubLogConfig {
        level,
        filename,
        format,
        enable_timestamp: config.log.enable_timestamp,
        txn_info_logging: config.log.txn_info_logging,
        max_size,
        max_days: config.log.file.max_days,
        max_backups: config.log.file.max_backups,
    }
}

fn build_status_config_json(config: &ConfigFile) -> Arc<String> {
    let mut status_config = config.clone();
    status_config.dfs.s3_key_id.clear();
    status_config.dfs.s3_secret_key.clear();
    status_config.dfs.azure.connection_string.clear();
    status_config.dfs.azure.account_key.clear();
    Arc::new(
        serde_json::to_string_pretty(&status_config).unwrap_or_else(|err| {
            panic!(
                "failed to serialize TiFlash Columnar Hub status config: {}",
                err
            )
        }),
    )
}

fn rename_log_by_timestamp(path: &Path) -> io::Result<PathBuf> {
    let mut rotated = path.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
    let mut file_name = path.file_stem().unwrap_or_default().to_os_string();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    file_name.push(format!("-{}", ts));
    if let Some(ext) = path.extension() {
        file_name.push(".");
        file_name.push(ext);
    }
    rotated.push(file_name);
    Ok(rotated)
}

fn init_hub_logger(config: &ConfigFile) {
    let log_config = resolve_hub_log_config(config);
    logger::set_txn_info_logging(log_config.txn_info_logging);

    let init_result = if log_config.filename.is_empty() {
        match log_config.format {
            LogFormat::Text => logger::init_log(
                logger::text_format(logger::term_writer(), log_config.enable_timestamp),
                log_config.level,
                true,
                true,
                vec![],
                0,
            ),
            LogFormat::Json => logger::init_log(
                logger::json_format(logger::term_writer(), log_config.enable_timestamp),
                log_config.level,
                true,
                true,
                vec![],
                0,
            ),
        }
    } else {
        let writer = logger::file_writer(
            &log_config.filename,
            log_config.max_size,
            log_config.max_backups,
            log_config.max_days,
            rename_log_by_timestamp,
        )
        .unwrap_or_else(|err| {
            panic!(
                "failed to initialize TiFlash Columnar Hub log file {}: {}",
                log_config.filename, err
            )
        });
        match log_config.format {
            LogFormat::Text => logger::init_log(
                logger::text_format(writer, log_config.enable_timestamp),
                log_config.level,
                true,
                true,
                vec![],
                0,
            ),
            LogFormat::Json => logger::init_log(
                logger::json_format(writer, log_config.enable_timestamp),
                log_config.level,
                true,
                true,
                vec![],
                0,
            ),
        }
    };

    if let Err(err) = init_result {
        panic!("failed to initialize TiFlash Columnar Hub logger: {}", err);
    }
}

fn log_columnar_hub_info() {
    info!("Welcome to TiFlash Columnar");
    for line in crate::proxy_version_info().lines() {
        info!("{}", line);
    }
}

fn build_cli_app<'a, 'b>() -> App<'a, 'b> {
    App::new("TiFlash Columnar Hub")
        .arg(
            Arg::with_name("config")
                .short("C")
                .long("config")
                .takes_value(true),
        )
        .arg(Arg::with_name("config-check").long("config-check"))
        .arg(
            Arg::with_name("log-level")
                .short("L")
                .long("log-level")
                .alias("log")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log-file")
                .short("f")
                .long("log-file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("data-dir")
                .short("s")
                .long("data-dir")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("pd-endpoints")
                .long("pd-endpoints")
                .takes_value(true)
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(","),
        )
        .arg(Arg::with_name("ca-path").long("ca-path").takes_value(true))
        .arg(
            Arg::with_name("cert-path")
                .long("cert-path")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("key-path")
                .long("key-path")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("addr")
                .short("A")
                .long("addr")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("advertise-addr")
                .long("advertise-addr")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("status-addr")
                .long("status-addr")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("advertise-status-addr")
                .long("advertise-status-addr")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("engine-version")
                .long("engine-version")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("engine-git-hash")
                .long("engine-git-hash")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("engine-addr")
                .long("engine-addr")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("advertise-engine-addr")
                .long("advertise-engine-addr")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("labels")
                .long("labels")
                .takes_value(true)
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(","),
        )
        .arg(
            Arg::with_name("engine-label")
                .long("engine-label")
                .takes_value(true),
        )
        .arg(Arg::with_name("init-only").long("init-only"))
}

fn is_known_value_arg(arg: &str) -> bool {
    matches!(
        arg,
        "-C" | "--config"
            | "-L"
            | "--log-level"
            | "--log"
            | "-f"
            | "--log-file"
            | "-s"
            | "--data-dir"
            | "--pd-endpoints"
            | "--ca-path"
            | "--cert-path"
            | "--key-path"
            | "-A"
            | "--addr"
            | "--advertise-addr"
            | "--status-addr"
            | "--advertise-status-addr"
            | "--engine-version"
            | "--engine-git-hash"
            | "--engine-addr"
            | "--advertise-engine-addr"
            | "--labels"
            | "--engine-label"
    )
}

fn is_known_flag_arg(arg: &str) -> bool {
    matches!(arg, "--config-check" | "--init-only")
}

fn has_known_value_assignment(arg: &str) -> bool {
    [
        "--config=",
        "--log-level=",
        "--log=",
        "--log-file=",
        "--data-dir=",
        "--pd-endpoints=",
        "--ca-path=",
        "--cert-path=",
        "--key-path=",
        "--addr=",
        "--advertise-addr=",
        "--status-addr=",
        "--advertise-status-addr=",
        "--engine-version=",
        "--engine-git-hash=",
        "--engine-addr=",
        "--advertise-engine-addr=",
        "--labels=",
        "--engine-label=",
    ]
    .iter()
    .any(|prefix| arg.starts_with(prefix))
}

fn resolve_engine_addr(config: &ConfigFile) -> &str {
    if !config.server.engine_addr.is_empty() {
        &config.server.engine_addr
    } else if !config.server.advertise_addr.is_empty() {
        &config.server.advertise_addr
    } else {
        &config.server.addr
    }
}

fn resolve_peer_addr(config: &ConfigFile) -> &str {
    if !config.server.advertise_addr.is_empty() {
        &config.server.advertise_addr
    } else if !config.server.addr.is_empty() {
        &config.server.addr
    } else {
        resolve_engine_addr(config)
    }
}

fn resolve_status_addr(config: &ConfigFile) -> &str {
    if !config.server.advertise_status_addr.is_empty() {
        &config.server.advertise_status_addr
    } else {
        &config.server.status_addr
    }
}

fn build_store_labels(config: &ConfigFile) -> protobuf::RepeatedField<metapb::StoreLabel> {
    config
        .server
        .labels
        .iter()
        .map(|(key, value)| {
            let mut label = metapb::StoreLabel::default();
            label.set_key(key.clone());
            label.set_value(value.clone());
            label
        })
        .collect::<Vec<_>>()
        .into()
}

fn build_store(config: &ConfigFile, store_id: u64) -> metapb::Store {
    let mut store = metapb::Store::default();
    store.set_id(store_id);

    let address = resolve_engine_addr(config).to_owned();
    if !address.is_empty() {
        store.set_address(address.clone());
        store.set_peer_address(resolve_peer_addr(config).to_owned());
    }

    let status_address = resolve_status_addr(config).to_owned();
    if !status_address.is_empty() {
        store.set_status_address(status_address);
    }

    if !config.server.engine_version.is_empty() {
        store.set_version(config.server.engine_version.clone());
    } else {
        store.set_version(env!("CARGO_PKG_VERSION").to_owned());
    }

    if !config.server.engine_git_hash.is_empty() {
        store.set_git_hash(config.server.engine_git_hash.clone());
    } else {
        store.set_git_hash("Unknown".to_owned());
    }

    if let Ok(path) = std::env::current_exe() {
        if let Some(path) = path.parent() {
            store.set_deploy_path(path.to_string_lossy().to_string());
        }
    }

    let start_timestamp = current_unix_timestamp_secs() as i64;
    store.set_start_timestamp(start_timestamp);
    store.set_labels(build_store_labels(config));
    store
}

fn is_tiflash_compute_store(store: &metapb::Store) -> bool {
    store
        .get_labels()
        .iter()
        .any(|label| label.get_key() == "engine" && label.get_value() == "tiflash_compute")
}

fn extract_duplicated_store_id(err_msg: &str) -> Option<u64> {
    lazy_static::lazy_static! {
        static ref DUPLICATED_STORE_RE: regex::Regex =
            regex::Regex::new(r"already registered by id:(\d+)").unwrap();
    }
    DUPLICATED_STORE_RE
        .captures(err_msg)
        .and_then(|caps| caps.get(1))
        .and_then(|matched| matched.as_str().parse::<u64>().ok())
}

fn wait_for_store_tombstone(
    pd_client: &dyn PdClient,
    conflict_store_id: u64,
    timeout: Duration,
) -> Result<(), String> {
    let start = SystemTime::now();
    loop {
        match pd_client.get_store(conflict_store_id) {
            Err(PdError::StoreTombstone(_)) => return Ok(()),
            Ok(_) => {}
            Err(err) => {
                return Err(format!(
                    "failed to confirm store {} tombstone state: {}",
                    conflict_store_id, err
                ))
            }
        }

        let elapsed = start.elapsed().unwrap_or_default();
        if elapsed >= timeout {
            return Err(format!(
                "timed out waiting {}s for store {} to become tombstone",
                timeout.as_secs(),
                conflict_store_id
            ));
        }
        thread::sleep(STORE_TOMBSTONE_POLL_INTERVAL);
    }
}

fn try_remove_duplicated_compute_store(
    pd_client: &dyn PdClient,
    pd_config: &PdConfig,
    store: &metapb::Store,
    err_msg: &str,
) -> Result<bool, String> {
    let Some(conflict_store_id) = extract_duplicated_store_id(err_msg) else {
        return Ok(false);
    };
    if !is_tiflash_compute_store(store) {
        return Ok(false);
    }

    info!(
        "detected duplicated TiFlash compute store registration, removing conflicting store from PD";
        "current_store_id" => store.get_id(),
        "conflict_store_id" => conflict_store_id,
        "status_address" => store.get_status_address().to_owned()
    );

    let pd_control = RestfulClient::new(
        "tiflash-columnar-hub-store-cleanup",
        pd_config.endpoints.clone(),
        pd_client.get_security_mgr(),
    )
    .map_err(|err| format!("failed to initialize PD control client: {}", err))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("failed to build PD control runtime: {}", err))?;
    runtime
        .block_on(pd_control.delete(format!("pd/api/v1/store/{conflict_store_id}")))
        .map_err(|err| {
            format!(
                "failed to delete duplicated store {} from PD: {}",
                conflict_store_id, err
            )
        })?;
    wait_for_store_tombstone(pd_client, conflict_store_id, STORE_TOMBSTONE_WAIT_TIMEOUT)?;
    Ok(true)
}

fn register_store_to_pd(
    pd_client: &dyn PdClient,
    pd_config: &PdConfig,
    store: metapb::Store,
) -> Result<(), String> {
    let store_id = store.get_id();
    match pd_client.put_store(store.clone()) {
        Ok(_) => Ok(()),
        Err(err) => {
            let err_msg = err.to_string();
            match try_remove_duplicated_compute_store(pd_client, pd_config, &store, &err_msg) {
                Ok(true) => pd_client.put_store(store).map(|_| ()).map_err(|retry_err| {
                    format!(
                        "failed to register TiFlash Columnar Hub store {} to PD after removing duplicated store: {}",
                        store_id, retry_err
                    )
                }),
                Ok(false) => Err(format!(
                    "failed to register TiFlash Columnar Hub store {} to PD: {}",
                    store_id, err_msg
                )),
                Err(remove_err) => Err(format!(
                    "failed to register TiFlash Columnar Hub store {} to PD: {}; duplicated-store cleanup failed: {}",
                    store_id, err_msg, remove_err
                )),
            }
        }
    }
}

fn current_unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|err| panic!("system clock before Unix epoch: {}", err))
        .as_secs()
}

fn collect_store_space_stats(data_dir: &Path) -> Option<(u64, u64)> {
    let dirs = [data_dir.to_path_buf()];
    match get_disks_stats(&dirs) {
        Ok((capacity, available)) => Some((capacity, available)),
        Err(err) => {
            warn!(
                "failed to collect disk stats for store heartbeat";
                "path" => data_dir.display().to_string(),
                "err" => ?err
            );
            None
        }
    }
}

fn collect_store_space_stats_from_engine_store() -> Option<(u64, u64)> {
    let stats = get_engine_store_server_helper().handle_compute_store_stats();
    if stats.fs_stats.ok == 0 {
        return None;
    }
    Some((stats.fs_stats.capacity_size, stats.fs_stats.avail_size))
}

fn build_store_heartbeat_stats_from_space(
    store_id: u64,
    start_time: u32,
    last_report_ts: u64,
    capacity: u64,
    available: u64,
) -> Option<pdpb::StoreStats> {
    if capacity == 0 {
        return None;
    }

    let mut stats = pdpb::StoreStats::default();
    stats.set_store_id(store_id);
    stats.set_start_time(start_time);
    stats.set_capacity(capacity);
    stats.set_available(available);
    stats.set_used_size(capacity.saturating_sub(available));

    let mut interval = pdpb::TimeInterval::default();
    interval.set_start_timestamp(last_report_ts);
    stats.set_interval(interval);
    Some(stats)
}

fn build_store_heartbeat_stats(
    store_id: u64,
    start_time: u32,
    last_report_ts: u64,
    data_dir: &Path,
) -> Option<pdpb::StoreStats> {
    let (capacity, available) = collect_store_space_stats_from_engine_store()
        .or_else(|| collect_store_space_stats(data_dir))?;
    if capacity == 0 {
        warn!(
            "skip store heartbeat because disk capacity is unavailable";
            "store_id" => store_id,
            "path" => data_dir.display().to_string(),
            "available" => available
        );
        return None;
    }
    build_store_heartbeat_stats_from_space(
        store_id,
        start_time,
        last_report_ts,
        capacity,
        available,
    )
}

fn spawn_store_heartbeat_loop(
    pd_client: Arc<dyn PdClient>,
    store_id: u64,
    start_time: u32,
    data_dir: PathBuf,
    shutdown: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name(format!("pd-store-heartbeat-{store_id}"))
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap_or_else(|err| panic!("failed to build store heartbeat runtime: {}", err));
            let mut last_report_ts = current_unix_timestamp_secs();

            info!(
                "starting TiFlash Columnar Hub store heartbeat loop";
                "store_id" => store_id,
                "interval_secs" => STORE_HEARTBEAT_INTERVAL.as_secs(),
                "path" => data_dir.display().to_string()
            );

            while !shutdown.load(Ordering::SeqCst) {
                if let Some(stats) =
                    build_store_heartbeat_stats(store_id, start_time, last_report_ts, &data_dir)
                {
                    match runtime.block_on(pd_client.store_heartbeat(stats, None, None)) {
                        Ok(_) => {
                            last_report_ts = current_unix_timestamp_secs();
                        }
                        Err(err) => {
                            warn!(
                                "failed to send TiFlash Columnar Hub store heartbeat to PD";
                                "store_id" => store_id,
                                "err" => ?err
                            );
                        }
                    }
                }

                let mut slept = Duration::from_secs(0);
                while slept < STORE_HEARTBEAT_INTERVAL && !shutdown.load(Ordering::SeqCst) {
                    let step = std::cmp::min(
                        HEARTBEAT_SHUTDOWN_POLL_INTERVAL,
                        STORE_HEARTBEAT_INTERVAL - slept,
                    );
                    thread::sleep(step);
                    slept += step;
                }
            }

            info!(
                "stopped TiFlash Columnar Hub store heartbeat loop";
                "store_id" => store_id
            );
        })
        .unwrap_or_else(|err| panic!("failed to spawn store heartbeat thread: {}", err))
}

fn store_id_path(data_dir: &Path) -> PathBuf {
    data_dir.join("tiflash-columnar-hub").join("store_id")
}

fn load_store_id(store_id_path: &Path) -> Option<u64> {
    if !store_id_path.exists() {
        return None;
    }

    let store_id_text = fs::read_to_string(store_id_path).unwrap_or_else(|err| {
        panic!(
            "failed to read persisted store id {}: {}",
            store_id_path.display(),
            err
        )
    });
    let trimmed = store_id_text.trim();
    if trimmed.is_empty() {
        panic!("persisted store id {} is empty", store_id_path.display());
    }
    Some(trimmed.parse::<u64>().unwrap_or_else(|err| {
        panic!(
            "invalid persisted store id {} in {}: {}",
            trimmed,
            store_id_path.display(),
            err
        )
    }))
}

fn persist_store_id(store_id_path: &Path, store_id: u64) {
    if let Some(parent) = store_id_path.parent() {
        fs::create_dir_all(parent).unwrap_or_else(|err| {
            panic!(
                "failed to create store id directory {}: {}",
                parent.display(),
                err
            )
        });
    }

    let temp_path = store_id_path.with_extension("tmp");
    fs::write(&temp_path, format!("{}\n", store_id)).unwrap_or_else(|err| {
        panic!(
            "failed to persist store id {} to {}: {}",
            store_id,
            temp_path.display(),
            err
        )
    });
    fs::rename(&temp_path, store_id_path).unwrap_or_else(|err| {
        panic!(
            "failed to move persisted store id from {} to {}: {}",
            temp_path.display(),
            store_id_path.display(),
            err
        )
    });
}

fn ensure_store_id(pd_client: &dyn PdClient, data_dir: &Path) -> u64 {
    let store_id_path = store_id_path(data_dir);
    if let Some(store_id) = load_store_id(&store_id_path) {
        return store_id;
    }

    let store_id = pd_client.alloc_id().unwrap_or_else(|err| {
        panic!("failed to allocate store id from PD: {}", err);
    });
    persist_store_id(&store_id_path, store_id);
    store_id
}

fn sanitize_proxy_args(raw_args: &[&str]) -> Vec<String> {
    let mut sanitized = Vec::with_capacity(raw_args.len().max(1));
    sanitized.push(
        raw_args
            .first()
            .copied()
            .unwrap_or("TiFlash Columnar Hub")
            .to_owned(),
    );

    let mut i = 1;
    while i < raw_args.len() {
        let arg = raw_args[i];
        if is_known_flag_arg(arg) || is_known_value_arg(arg) || has_known_value_assignment(arg) {
            sanitized.push(arg.to_owned());
            if is_known_value_arg(arg) && i + 1 < raw_args.len() {
                sanitized.push(raw_args[i + 1].to_owned());
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if let Some(value) = arg.strip_prefix("-C") {
            if !value.is_empty() {
                sanitized.push("-C".to_owned());
                sanitized.push(value.to_owned());
                i += 1;
                continue;
            }
        }
        if let Some(value) = arg.strip_prefix("-s") {
            if !value.is_empty() {
                sanitized.push("-s".to_owned());
                sanitized.push(value.to_owned());
                i += 1;
                continue;
            }
        }
        if let Some(value) = arg.strip_prefix("-L") {
            if !value.is_empty() {
                sanitized.push("-L".to_owned());
                sanitized.push(value.to_owned());
                i += 1;
                continue;
            }
        }
        if let Some(value) = arg.strip_prefix("-f") {
            if !value.is_empty() {
                sanitized.push("-f".to_owned());
                sanitized.push(value.to_owned());
                i += 1;
                continue;
            }
        }

        i += 1;
        if i < raw_args.len() && !raw_args[i].starts_with('-') {
            i += 1;
        }
    }
    sanitized
}

fn make_disabled_file_encryption_info() -> FileEncryptionInfoRaw {
    FileEncryptionInfoRaw {
        res: FileEncryptionRes::Disabled,
        method: EncryptionMethod::Plaintext,
        key: std::ptr::null_mut(),
        iv: std::ptr::null_mut(),
        error_msg: std::ptr::null_mut(),
    }
}

extern "C" fn ffi_handle_get_proxy_status(
    proxy_ptr: crate::interfaces_ffi::RaftStoreProxyPtr,
) -> RaftProxyStatus {
    unsafe { proxy_ptr.as_ref().status() }
}

extern "C" fn ffi_is_encryption_enabled(
    _proxy_ptr: crate::interfaces_ffi::RaftStoreProxyPtr,
) -> u8 {
    0
}

extern "C" fn ffi_encryption_method(
    _proxy_ptr: crate::interfaces_ffi::RaftStoreProxyPtr,
) -> EncryptionMethod {
    EncryptionMethod::Plaintext
}

extern "C" fn ffi_handle_get_file(
    _proxy_ptr: crate::interfaces_ffi::RaftStoreProxyPtr,
    _name: crate::interfaces_ffi::BaseBuffView,
) -> FileEncryptionInfoRaw {
    make_disabled_file_encryption_info()
}

extern "C" fn ffi_handle_new_file(
    _proxy_ptr: crate::interfaces_ffi::RaftStoreProxyPtr,
    _name: crate::interfaces_ffi::BaseBuffView,
) -> FileEncryptionInfoRaw {
    make_disabled_file_encryption_info()
}

extern "C" fn ffi_handle_delete_file(
    _proxy_ptr: crate::interfaces_ffi::RaftStoreProxyPtr,
    _name: crate::interfaces_ffi::BaseBuffView,
) -> FileEncryptionInfoRaw {
    make_disabled_file_encryption_info()
}

extern "C" fn ffi_handle_link_file(
    _proxy_ptr: crate::interfaces_ffi::RaftStoreProxyPtr,
    _src: crate::interfaces_ffi::BaseBuffView,
    _dst: crate::interfaces_ffi::BaseBuffView,
) -> FileEncryptionInfoRaw {
    make_disabled_file_encryption_info()
}

extern "C" fn ffi_get_keyspace_encryption(
    _proxy_ptr: crate::interfaces_ffi::RaftStoreProxyPtr,
    _keyspace_id: u32,
) -> bool {
    false
}

extern "C" fn ffi_get_master_key(
    _proxy_ptr: crate::interfaces_ffi::RaftStoreProxyPtr,
) -> crate::interfaces_ffi::RawCppStringPtr {
    let helper = get_engine_store_server_helper();
    helper.gen_cpp_string(b"")
}

extern "C" fn ffi_get_config_json(
    proxy_ptr: crate::interfaces_ffi::RaftStoreProxyPtr,
    kind: ConfigJsonType,
) -> crate::interfaces_ffi::RustStrWithView {
    match kind {
        ConfigJsonType::ProxyConfigAddressed => unsafe {
            crate::build_from_string(proxy_ptr.as_ref().get_config_str().as_bytes().to_vec())
        },
    }
}

fn build_hub_ffi_helper(hub: &ColumnarHub) -> RaftStoreProxyFFIHelper {
    RaftStoreProxyFFIHelper {
        proxy_ptr: hub.into(),
        fn_handle_get_proxy_status: Some(ffi_handle_get_proxy_status),
        fn_is_encryption_enabled: Some(ffi_is_encryption_enabled),
        fn_encryption_method: Some(ffi_encryption_method),
        fn_handle_get_file: Some(ffi_handle_get_file),
        fn_handle_new_file: Some(ffi_handle_new_file),
        fn_handle_delete_file: Some(ffi_handle_delete_file),
        fn_handle_link_file: Some(ffi_handle_link_file),
        fn_handle_batch_read_index: None,
        sst_reader_interfaces: SSTReaderInterfaces {
            fn_get_sst_reader: None,
            fn_remained: None,
            fn_key: None,
            fn_value: None,
            fn_next: None,
            fn_gc: None,
            fn_kind: None,
            fn_seek: None,
            fn_approx_size: None,
            fn_get_split_keys: None,
        },
        cloud_storage_engine_interfaces: CloudStorageEngineInterfaces {
            fn_get_keyspace_encryption: Some(ffi_get_keyspace_encryption),
            fn_get_master_key: Some(ffi_get_master_key),
            fn_get_region_bucket_keys: Some(ffi_get_region_bucket_keys),
            fn_clear_shared_snap_access_by_start_ts: Some(ffi_clear_shared_snap_access_by_start_ts),
            fn_get_columnar_reader: Some(ffi_make_columnar_reader),
            fn_read_block: Some(ffi_read_block),
            fn_read_handle: Some(ffi_read_handle),
            fn_read_version: Some(ffi_read_version),
            fn_read_column: Some(ffi_read_column),
            fn_physical_table_id: Some(ffi_physical_table_id),
            fn_columnar_scan_stats: Some(ffi_columnar_scan_stats),
        },
        fn_server_info: Some(ffi_server_info),
        fn_make_read_index_task: None,
        fn_make_async_waker: None,
        fn_poll_read_index_task: None,
        fn_gc_rust_ptr: Some(ffi_gc_rust_ptr),
        fn_make_timer_task: None,
        fn_poll_timer_task: None,
        fn_get_region_local_state: None,
        fn_get_cluster_raftstore_version: None,
        fn_notify_compact_log: None,
        fn_get_config_json: Some(ffi_get_config_json),
    }
}

pub unsafe fn run_proxy(argc: c_int, argv: *const *const c_char, helper_ptr: *const u8) {
    init_engine_store_server_helper(helper_ptr);
    let helper = get_engine_store_server_helper();
    helper.check();

    let mut args = Vec::with_capacity(argc as usize);
    for i in 0..argc {
        let raw = CStr::from_ptr(*argv.offset(i as isize));
        args.push(raw.to_str().unwrap());
    }
    let args = sanitize_proxy_args(&args);

    let matches = build_cli_app().get_matches_from(args);

    let mut config = load_config(matches.value_of_os("config"));
    let init_only = overwrite_config_with_cmd_args(&mut config, &matches);
    init_hub_logger(&config);
    log_columnar_hub_info();
    config.security.override_from_env();
    config.dfs.override_from_env();
    config.pd.validate().unwrap();

    if matches.is_present("config-check") {
        println!("config check successful");
        process::exit(0);
    }

    init_metrics();

    let security_mgr = Arc::new(SecurityManager::new(&config.security).unwrap());
    let env = Arc::new(EnvBuilder::new().cq_count(1).name_prefix("pd").build());
    let pd_client: Arc<dyn PdClient> =
        Arc::new(RpcClient::new(&config.pd, Some(env), security_mgr.clone()).unwrap());
    let dfs = build_dfs(&config, pd_client.clone());
    let master_key = tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(config.security.new_master_key())
        .unwrap_or_else(|_| MasterKey::new(&[]));
    let txn_chunk_mgr = TxnChunkManager::new(
        vec![],
        dfs.clone(),
        BlockCache::None,
        None,
        with_pool_size(8),
        TxnChunkManagerConfig::default(),
    );
    let data_dir = PathBuf::from(if config.storage.data_dir.is_empty() {
        "."
    } else {
        &config.storage.data_dir
    });
    fs::create_dir_all(&data_dir).unwrap_or_else(|err| {
        panic!(
            "failed to create TiFlash Columnar Hub data directory {}: {}",
            data_dir.display(),
            err
        )
    });
    let data_dir_str = data_dir.to_string_lossy().to_string();
    let hub_config_str = serde_json::to_string(&HubConfigSummary {
        server: HubServerSummary {
            engine_addr: resolve_engine_addr(&config),
        },
        pd_endpoints: &config.pd.endpoints,
        data_dir: &data_dir_str,
        init_only,
    })
    .unwrap_or_default();
    let status_config_json = build_status_config_json(&config);
    let mut store_registration: Option<(metapb::Store, u32)> = None;
    let mut heartbeat_context: Option<(u64, u32)> = None;
    let heartbeat_shutdown = Arc::new(AtomicBool::new(false));
    let mut heartbeat_handle: Option<thread::JoinHandle<()>> = None;

    if !init_only {
        let store_id = ensure_store_id(pd_client.as_ref(), &data_dir);
        let store = build_store(&config, store_id);
        let start_time = store.get_start_timestamp() as u32;
        store_registration = Some((store, start_time));
    }

    let cloud_helper = CloudHelper::new(
        CloudEngineBackends {
            dfs,
            pd_client: pd_client.clone(),
            master_key,
            txn_chunk_mgr,
        },
        &data_dir,
        &config.ia,
        config.vector_index.clone(),
        config.columnar_file_cache.clone(),
        config.fts_cache.clone(),
        config.fts_delta_cache.clone(),
        config.block_cache_size,
    );
    let hub_status = Arc::new(AtomicU8::new(RaftProxyStatus::Idle as u8));
    let hub = ColumnarHub::new(hub_status.clone(), cloud_helper, hub_config_str);
    let mut ffi_helper = build_hub_ffi_helper(&hub);
    helper.set_proxy(&mut ffi_helper);
    let mut status_server = if config.server.status_addr.is_empty() {
        None
    } else {
        let mut server =
            HubStatusServer::new(security_mgr.clone(), hub_status.clone(), status_config_json)
                .unwrap_or_else(|err| {
                    panic!(
                        "failed to initialize TiFlash Columnar Hub status server: {}",
                        err
                    )
                });
        server
            .start(config.server.status_addr.clone())
            .unwrap_or_else(|err| {
                panic!(
                    "failed to start TiFlash Columnar Hub status server: {}",
                    err
                )
            });
        Some(server)
    };

    info!("wait for engine-store server to start");
    let mut engine_store_status = helper.handle_get_engine_store_server_status();
    while matches!(engine_store_status, EngineStoreServerStatus::Idle) {
        thread::sleep(Duration::from_millis(200));
        engine_store_status = helper.handle_get_engine_store_server_status();
    }

    if matches!(engine_store_status, EngineStoreServerStatus::Running) {
        info!("engine-store server is running");

        // Only register the store and start the heartbeat loop after the engine-store server is running. This ensures that TiFlash does not register itself before the PD successfully bootstrapped the cluster with init TiKV servers.
        if let Some((store, start_time)) = store_registration.take() {
            let store_id = store.get_id();
            register_store_to_pd(pd_client.as_ref(), &config.pd, store)
                .unwrap_or_else(|err| panic!("{}", err));
            heartbeat_context = Some((store_id, start_time));
        }

        hub.set_status(RaftProxyStatus::Running);
        if let Some((store_id, start_time)) = heartbeat_context {
            heartbeat_handle = Some(spawn_store_heartbeat_loop(
                pd_client.clone(),
                store_id,
                start_time,
                data_dir.clone(),
                heartbeat_shutdown.clone(),
            ));
        }
    }

    loop {
        match engine_store_status {
            EngineStoreServerStatus::Running => thread::sleep(Duration::from_millis(200)),
            EngineStoreServerStatus::Stopping => {
                hub.set_status(RaftProxyStatus::Stopped);
                heartbeat_shutdown.store(true, Ordering::SeqCst);
                while !matches!(
                    helper.handle_get_engine_store_server_status(),
                    EngineStoreServerStatus::Terminated
                ) {
                    thread::sleep(Duration::from_millis(200));
                }
                break;
            }
            EngineStoreServerStatus::Terminated => {
                heartbeat_shutdown.store(true, Ordering::SeqCst);
                break;
            }
            EngineStoreServerStatus::Idle => thread::sleep(Duration::from_millis(200)),
        }
        engine_store_status = helper.handle_get_engine_store_server_status();
    }

    info!(
        "found engine-store server status is {:?}, start to stop all services in columnar hub",
        engine_store_status
    );

    if let Some(handle) = heartbeat_handle.take() {
        let _ = handle.join();
    }
    if let Some(server) = status_server.take() {
        server.stop();
    }

    info!("TiFlash Columnar Hub has been stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_matches<'a>(args: &'a [&'a str]) -> clap::ArgMatches<'a> {
        build_cli_app().get_matches_from_safe(args).unwrap()
    }

    #[test]
    fn test_sanitize_proxy_args_keeps_store_registration_args() {
        let args = [
            "hub",
            "--log-file=/tmp/hub.log",
            "-L",
            "debug",
            "--engine-addr",
            "1.1.1.1:3930",
            "--advertise-status-addr=2.2.2.2:20292",
            "--labels=zone=sh,host=a",
            "--engine-label",
            "tiflash_compute",
            "--unknown-flag",
            "ignored",
        ];
        let sanitized = sanitize_proxy_args(&args);
        assert!(sanitized.contains(&"--log-file=/tmp/hub.log".to_owned()));
        assert!(sanitized.contains(&"-L".to_owned()));
        assert!(sanitized.contains(&"debug".to_owned()));
        assert!(sanitized.contains(&"--engine-addr".to_owned()));
        assert!(sanitized.contains(&"1.1.1.1:3930".to_owned()));
        assert!(sanitized.contains(&"--advertise-status-addr=2.2.2.2:20292".to_owned()));
        assert!(sanitized.contains(&"--labels=zone=sh,host=a".to_owned()));
        assert!(sanitized.contains(&"--engine-label".to_owned()));
        assert!(sanitized.contains(&"tiflash_compute".to_owned()));
        assert!(!sanitized.contains(&"--unknown-flag".to_owned()));
    }

    #[test]
    fn test_overwrite_config_with_cmd_args_for_store_registration() {
        let matches = parse_matches(&[
            "hub",
            "--log-file",
            "/tmp/hub.log",
            "--log-level",
            "warn",
            "--engine-addr",
            "1.1.1.1:3930",
            "--advertise-status-addr",
            "2.2.2.2:20292",
            "--engine-version",
            "v9",
            "--engine-git-hash",
            "abcd",
            "--labels",
            "zone=sh,host=a",
            "--engine-label",
            "tiflash_compute",
        ]);
        let mut config = ConfigFile::default();
        let init_only = overwrite_config_with_cmd_args(&mut config, &matches);

        assert!(!init_only);
        assert_eq!(config.log.file.filename, "/tmp/hub.log");
        assert_eq!(config.log.level, "warn");
        assert_eq!(config.server.engine_addr, "1.1.1.1:3930");
        assert_eq!(config.server.advertise_status_addr, "2.2.2.2:20292");
        assert_eq!(config.server.engine_version, "v9");
        assert_eq!(config.server.engine_git_hash, "abcd");
        assert_eq!(config.server.labels.get("zone").unwrap(), "sh");
        assert_eq!(config.server.labels.get("host").unwrap(), "a");
        assert_eq!(
            config.server.labels.get("engine").unwrap(),
            "tiflash_compute"
        );
    }

    #[test]
    fn test_resolve_hub_log_config_uses_legacy_fields() {
        let config: ConfigFile = toml::from_str(
            r#"
log-file = "/tmp/tiflash-proxy.log"
log-level = "debug"
"#,
        )
        .unwrap();

        let resolved = resolve_hub_log_config(&config);
        assert_eq!(resolved.filename, "/tmp/tiflash-proxy.log");
        assert_eq!(resolved.level, slog::Level::Debug);
        assert_eq!(resolved.format, LogFormat::Text);
    }

    #[test]
    fn test_config_file_default_uses_proxy_compatible_ia_defaults() {
        let config = ConfigFile::default();
        #[cfg(target_os = "linux")]
        {
            assert_eq!(config.ia.mem_cap, AbsoluteOrPercentSize::Percent(20.0));
            assert_eq!(config.ia.disk_cap, AbsoluteOrPercentSize::Percent(70.0));
        }
        #[cfg(target_os = "macos")]
        {
            assert_eq!(
                config.ia.mem_cap,
                AbsoluteOrPercentSize::Abs(ReadableSize::mb(100))
            );
            assert_eq!(
                config.ia.disk_cap,
                AbsoluteOrPercentSize::Abs(ReadableSize::mb(100))
            );
        }
    }

    #[test]
    fn test_missing_ia_config_uses_proxy_compatible_defaults() {
        let config: ConfigFile = toml::from_str(
            r#"
[server]
engine-addr = "1.2.3.4:3930"
"#,
        )
        .unwrap();
        #[cfg(target_os = "linux")]
        {
            assert_eq!(config.ia.mem_cap, AbsoluteOrPercentSize::Percent(20.0));
            assert_eq!(config.ia.disk_cap, AbsoluteOrPercentSize::Percent(70.0));
        }
        #[cfg(target_os = "macos")]
        {
            assert_eq!(
                config.ia.mem_cap,
                AbsoluteOrPercentSize::Abs(ReadableSize::mb(100))
            );
            assert_eq!(
                config.ia.disk_cap,
                AbsoluteOrPercentSize::Abs(ReadableSize::mb(100))
            );
        }
    }

    #[test]
    fn test_resolve_hub_log_config_prefers_nested_log_config() {
        let config: ConfigFile = toml::from_str(
            r#"
log-file = "/tmp/legacy.log"
log-level = "warn"
log-format = "json"
log-rotation-size = "1024MiB"

[log]
level = "error"
format = "text"
enable-timestamp = false

[log.file]
filename = "/tmp/nested.log"
max-size = 512
max-days = 3
max-backups = 7
"#,
        )
        .unwrap();

        let resolved = resolve_hub_log_config(&config);
        assert_eq!(resolved.filename, "/tmp/nested.log");
        assert_eq!(resolved.level, slog::Level::Error);
        assert_eq!(resolved.format, LogFormat::Text);
        assert!(!resolved.enable_timestamp);
        assert_eq!(resolved.max_size, 512);
        assert_eq!(resolved.max_days, 3);
        assert_eq!(resolved.max_backups, 7);
    }

    #[test]
    fn test_resolve_hub_log_config_uses_legacy_rotation_when_nested_default() {
        let config: ConfigFile = toml::from_str(
            r#"
log-file = "/tmp/legacy.log"
log-rotation-size = "1024MiB"
"#,
        )
        .unwrap();

        let resolved = resolve_hub_log_config(&config);
        assert_eq!(resolved.filename, "/tmp/legacy.log");
        assert_eq!(resolved.max_size, 1024);
    }

    #[test]
    fn test_build_store_uses_proxy_compatible_fields() {
        let mut config = ConfigFile::default();
        config.server.engine_addr = "1.1.1.1:3930".to_owned();
        config.server.addr = "10.0.0.1:20170".to_owned();
        config.server.advertise_addr = "10.0.0.2:20170".to_owned();
        config.server.status_addr = "127.0.0.1:20292".to_owned();
        config.server.advertise_status_addr = "2.2.2.2:20292".to_owned();
        config.server.engine_version = "v9".to_owned();
        config.server.engine_git_hash = "abcd".to_owned();
        config
            .server
            .labels
            .insert("zone".to_owned(), "sh".to_owned());
        config
            .server
            .labels
            .insert("engine".to_owned(), "tiflash_compute".to_owned());

        let store = build_store(&config, 42);
        assert_eq!(store.get_id(), 42);
        assert_eq!(store.get_address(), "1.1.1.1:3930");
        assert_eq!(store.get_peer_address(), "10.0.0.2:20170");
        assert_eq!(store.get_status_address(), "2.2.2.2:20292");
        assert_eq!(store.get_version(), "v9");
        assert_eq!(store.get_git_hash(), "abcd");
        assert_eq!(store.get_labels().len(), 2);
    }

    #[test]
    fn test_is_tiflash_compute_store_detects_engine_label() {
        let mut config = ConfigFile::default();
        config
            .server
            .labels
            .insert("engine".to_owned(), "tiflash_compute".to_owned());
        let store = build_store(&config, 42);
        assert!(is_tiflash_compute_store(&store));
    }

    #[test]
    fn test_is_tiflash_compute_store_rejects_other_labels() {
        let mut config = ConfigFile::default();
        config
            .server
            .labels
            .insert("engine".to_owned(), "tiflash".to_owned());
        let store = build_store(&config, 42);
        assert!(!is_tiflash_compute_store(&store));
    }

    #[test]
    fn test_extract_duplicated_store_id() {
        assert_eq!(
            extract_duplicated_store_id("store address is already registered by id:9527"),
            Some(9527)
        );
        assert_eq!(extract_duplicated_store_id("other pd error"), None);
    }

    #[test]
    fn test_hub_config_summary_omits_raftstore_and_keeps_proxy_shape() {
        let value = serde_json::to_value(HubConfigSummary {
            server: HubServerSummary {
                engine_addr: "1.1.1.1:3930",
            },
            pd_endpoints: &["http://127.0.0.1:2379".to_owned()],
            data_dir: "/tmp/hub",
            init_only: true,
        })
        .unwrap();

        assert!(value.get("raftstore").is_none());
        assert_eq!(
            value["server"]["engine-addr"].as_str(),
            Some("1.1.1.1:3930")
        );
        assert_eq!(value["init-only"].as_bool(), Some(true));
    }

    #[test]
    fn test_persist_store_id_roundtrip() {
        let temp_dir = std::env::temp_dir().join(format!(
            "tiflash-columnar-hub-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let file_path = store_id_path(&temp_dir);

        persist_store_id(&file_path, 9527);
        assert_eq!(load_store_id(&file_path), Some(9527));

        let _ = fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn test_build_store_heartbeat_stats_from_space_sets_minimal_fields() {
        let stats = build_store_heartbeat_stats_from_space(9527, 123, 456, 4096, 1024).unwrap();

        assert_eq!(stats.get_store_id(), 9527);
        assert_eq!(stats.get_start_time(), 123);
        assert_eq!(stats.get_capacity(), 4096);
        assert_eq!(stats.get_available(), 1024);
        assert_eq!(stats.get_used_size(), 3072);
        assert_eq!(stats.get_interval().get_start_timestamp(), 456);
    }

    #[test]
    fn test_build_store_heartbeat_stats_skips_zero_capacity() {
        assert!(build_store_heartbeat_stats_from_space(9527, 123, 456, 0, 0).is_none());
    }

    #[test]
    fn test_build_store_heartbeat_stats_collects_disk_usage_from_data_dir() {
        let temp_dir = std::env::temp_dir().join(format!(
            "tiflash-columnar-hub-heartbeat-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&temp_dir).unwrap();

        let (capacity, available) = collect_store_space_stats(&temp_dir).unwrap();
        let stats = build_store_heartbeat_stats_from_space(
            9527,
            123,
            456,
            capacity,
            available,
        )
        .unwrap();
        assert_eq!(stats.get_store_id(), 9527);
        assert_eq!(stats.get_start_time(), 123);
        assert!(stats.get_capacity() > 0);
        assert!(stats.get_capacity() >= stats.get_available());
        assert_eq!(stats.get_interval().get_start_timestamp(), 456);

        let _ = fs::remove_dir_all(temp_dir);
    }
}
