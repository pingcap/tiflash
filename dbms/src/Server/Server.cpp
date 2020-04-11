#include "Server.h"

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Config/ConfigReloader.h>
#include <Common/CurrentMetrics.h>
#include <Common/Macros.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashBuildInfo.h>
#include <Common/config.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Flash/FlashService.h>
#include <Functions/registerFunctions.h>
#include <IO/HTTPCommon.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/loadMetadata.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>
#include <Poco/StringTokenizer.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/StorageEngineType.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/registerStorages.h>
#include <TableFunctions/registerTableFunctions.h>
#include <common/ErrorHandlers.h>
#include <common/getMemoryAmount.h>
#include <common/logger_useful.h>
#include <sys/resource.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <ext/scope_guard.h>
#include <memory>

#include "ClusterManagerService.h"
#include "HTTPHandlerFactory.h"
#include "MetricsPrometheus.h"
#include "MetricsTransmitter.h"
#include "StatusFile.h"
#include "TCPHandlerFactory.h"

#if Poco_NetSSL_FOUND
#include <Poco/Net/Context.h>
#include <Poco/Net/SecureServerSocket.h>
#include <grpc++/grpc++.h>
#endif

namespace CurrentMetrics
{
extern const Metric Revision;
}

namespace DB
{

namespace ErrorCodes
{
extern const int NO_ELEMENTS_IN_CONFIG;
extern const int SUPPORT_IS_DISABLED;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int INVALID_CONFIG_PARAMETER;
} // namespace ErrorCodes


static std::string getCanonicalPath(std::string path)
{
    Poco::trimInPlace(path);
    if (path.empty())
        throw Exception("path configuration parameter is empty");
    if (path.back() != '/')
        path += '/';
    return path;
}

void Server::uninitialize()
{
    logger().information("shutting down");
    BaseDaemon::uninitialize();
}

void Server::initialize(Poco::Util::Application & self)
{
    BaseDaemon::initialize(self);
    logger().information("starting up");
}

std::string Server::getDefaultCorePath() const { return getCanonicalPath(config().getString("path")) + "cores"; }

struct TiFlashProxyConfig
{
    static const std::string config_prefix;
    std::vector<const char *> args;
    std::unordered_map<std::string, std::string> val_map;
    bool inited = false;

    TiFlashProxyConfig(Poco::Util::LayeredConfiguration & config)
    {
        if (!config.has(config_prefix))
            return;

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_prefix, keys);
        for (const auto & key : keys)
        {
            const auto k = config_prefix + "." + key;
            val_map["--" + key] = config.getString(k);
        }

        val_map["--pd-endpoints"] = config.getString("raft.pd_addr");
        val_map["--tiflash-version"] = TiFlashBuildInfo::getReleaseVersion();
        val_map["--tiflash-git-hash"] = TiFlashBuildInfo::getGitHash();

        args.push_back("TiFlash Proxy");
        for (const auto & v : val_map)
        {
            args.push_back(v.first.data());
            args.push_back(v.second.data());
        }
        inited = true;
    }
};

const std::string TiFlashProxyConfig::config_prefix = "flash.proxy";

int Server::main(const std::vector<std::string> & /*args*/)
{
    Logger * log = &logger();

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();

    CurrentMetrics::set(CurrentMetrics::Revision, ClickHouseRevision::get());

    /** Context contains all that query execution is dependent:
      *  settings, available functions, data types, aggregate functions, databases...
      */
    global_context = std::make_unique<Context>(Context::createGlobal());
    global_context->setGlobalContext(*global_context);
    global_context->setApplicationType(Context::ApplicationType::SERVER);

    bool has_zookeeper = config().has("zookeeper");

    std::vector<String> all_fast_path;
    if (config().has("fast_path"))
    {
        String fast_paths = config().getString("fast_path");
        Poco::trimInPlace(fast_paths);
        if (!fast_paths.empty())
        {
            Poco::StringTokenizer string_tokens(fast_paths, ",");
            for (auto it = string_tokens.begin(); it != string_tokens.end(); it++)
            {
                all_fast_path.emplace_back(getCanonicalPath(std::string(*it)));
                LOG_DEBUG(log, "Fast data part candidate path: " << getCanonicalPath(std::string(*it)));
            }
        }
    }
    std::vector<String> all_normal_path;
    {
        String paths = config().getString("path");
        Poco::trimInPlace(paths);
        if (paths.empty())
            throw Exception("path configuration parameter is empty");
        Poco::StringTokenizer string_tokens(paths, ",");
        for (auto it = string_tokens.begin(); it != string_tokens.end(); it++)
        {
            all_normal_path.emplace_back(getCanonicalPath(std::string(*it)));
            LOG_DEBUG(log, "Data part candidate path: " << std::string(*it));
        }
    }

    bool path_realtime_mode = config().getBool("path_realtime_mode", false);
    std::vector<String> extra_paths(all_normal_path.begin(), all_normal_path.end());
    for (auto & p : extra_paths)
        p += "/data";

    if (path_realtime_mode && all_normal_path.size() > 1)
        global_context->setExtraPaths(std::vector<String>(extra_paths.begin() + 1, extra_paths.end()));
    else
        global_context->setExtraPaths(extra_paths);

    std::string path = all_normal_path[0];
    std::string default_database = config().getString("default_database", "default");
    global_context->setPath(path);
    global_context->initializePartPathSelector(std::move(all_normal_path), std::move(all_fast_path));

    /// Create directories for 'path' and for default database, if not exist.
    for (const String & candidate_path : global_context->getPartPathSelector().getAllPath())
    {
        Poco::File(candidate_path + "data/" + default_database).createDirectories();
    }
    Poco::File(path + "metadata/" + default_database).createDirectories();

    StatusFile status{path + "status"};

    SCOPE_EXIT({
        /** Explicitly destroy Context. It is more convenient than in destructor of Server, because logger is still available.
          * At this moment, no one could own shared part of Context.
          */
        global_context.reset();

        LOG_DEBUG(log, "Destroyed global context.");
    });

    /// Try to increase limit on number of open files.
    {
        rlimit rlim;
        if (getrlimit(RLIMIT_NOFILE, &rlim))
            throw Poco::Exception("Cannot getrlimit");

        if (rlim.rlim_cur == rlim.rlim_max)
        {
            LOG_DEBUG(log, "rlimit on number of file descriptors is " << rlim.rlim_cur);
        }
        else
        {
            rlim_t old = rlim.rlim_cur;
            rlim.rlim_cur = config().getUInt("max_open_files", rlim.rlim_max);
            int rc = setrlimit(RLIMIT_NOFILE, &rlim);
            if (rc != 0)
                LOG_WARNING(log,
                    "Cannot set max number of file descriptors to "
                        << rlim.rlim_cur << ". Try to specify max_open_files according to your system limits. error: " << strerror(errno));
            else
                LOG_DEBUG(log, "Set max number of file descriptors to " << rlim.rlim_cur << " (was " << old << ").");
        }
    }

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Initialize DateLUT early, to not interfere with running time of first query.
    LOG_DEBUG(log, "Initializing DateLUT.");
    DateLUT::instance();
    LOG_TRACE(log, "Initialized DateLUT with time zone `" << DateLUT::instance().getTimeZone() << "'.");

    /// Directory with temporary data for processing of heavy queries.
    {
        std::string tmp_path = config().getString("tmp_path", path + "tmp/");
        global_context->setTemporaryPath(tmp_path);
        Poco::File(tmp_path).createDirectories();

        /// Clearing old temporary files.
        Poco::DirectoryIterator dir_end;
        for (Poco::DirectoryIterator it(tmp_path); it != dir_end; ++it)
        {
            if (it->isFile() && startsWith(it.name(), "tmp"))
            {
                LOG_DEBUG(log, "Removing old temporary file " << it->path());
                it->remove();
            }
        }
    }

    /** Directory with 'flags': files indicating temporary settings for the server set by system administrator.
      * Flags may be cleared automatically after being applied by the server.
      * Examples: do repair of local data; clone all replicated tables from replica.
      */
    {
        Poco::File(path + "flags/").createDirectories();
        global_context->setFlagsPath(path + "flags/");
    }

    /** Directory with user provided files that are usable by 'file' table function.
      */
    {

        std::string user_files_path = config().getString("user_files_path", path + "user_files/");
        global_context->setUserFilesPath(user_files_path);
        Poco::File(user_files_path).createDirectories();
    }

    if (config().has("interserver_http_port"))
    {
        String this_host = config().getString("interserver_http_host", "");

        if (this_host.empty())
        {
            this_host = getFQDNOrHostName();
            LOG_DEBUG(log,
                "Configuration parameter 'interserver_http_host' doesn't exist or exists and empty. Will use '" + this_host
                    + "' as replica host.");
        }

        String port_str = config().getString("interserver_http_port");
        int port = parse<int>(port_str);

        if (port < 0 || port > 0xFFFF)
            throw Exception("Out of range 'interserver_http_port': " + toString(port), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        global_context->setInterserverIOAddress(this_host, port);
    }

    if (config().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(config(), "macros"));

    /// Initialize main config reloader.
    auto main_config_reloader = std::make_unique<ConfigReloader>(
        config_path,
        [&](ConfigurationPtr config) {
            buildLoggers(*config);
            global_context->setClustersConfig(config);
            global_context->setMacros(std::make_unique<Macros>(*config, "macros"));
            global_context->getTMTContext().reloadConfig(*config);
        },
        /* already_loaded = */ true);

    /// Initialize users config reloader.
    std::string users_config_path = config().getString("users_config", config_path);
    /// If path to users' config isn't absolute, try guess its root (current) dir.
    /// At first, try to find it in dir of main config, after will use current dir.
    if (users_config_path.empty() || users_config_path[0] != '/')
    {
        std::string config_dir = Poco::Path(config_path).parent().toString();
        if (Poco::File(config_dir + users_config_path).exists())
            users_config_path = config_dir + users_config_path;
    }
    auto users_config_reloader = std::make_unique<ConfigReloader>(
        users_config_path,
        [&](ConfigurationPtr config) { global_context->setUsersConfig(config); },
        /* already_loaded = */ false);

    /// Reload config in SYSTEM RELOAD CONFIG query.
    global_context->setConfigReloadCallback([&]() {
        main_config_reloader->reload();
        users_config_reloader->reload();
    });

    /// Limit on total number of concurrently executed queries.
    global_context->getProcessList().setMaxSize(config().getInt("max_concurrent_queries", 0));

    /// Setup protection to avoid accidental DROP for big tables (that are greater than 50 GB by default)
    if (config().has("max_table_size_to_drop"))
        global_context->setMaxTableSizeToDrop(config().getUInt64("max_table_size_to_drop"));

    /// Size of cache for uncompressed blocks. Zero means disabled.
    size_t uncompressed_cache_size = config().getUInt64("uncompressed_cache_size", 0);
    if (uncompressed_cache_size)
        global_context->setUncompressedCache(uncompressed_cache_size);

    /// Quota size in bytes of persisted mapping cache. 0 means unlimited.
    size_t persisted_cache_size = config().getUInt64("persisted_mapping_cache_size", 0);
    /// Path of persisted cache in fast(er) disk device. Empty means disabled.
    std::string persisted_cache_path = config().getString("persisted_mapping_cache_path", "");
    if (!persisted_cache_path.empty())
        global_context->setPersistedCache(persisted_cache_size, persisted_cache_path);

    bool use_L0_opt = config().getBool("l0_optimize", false);
    global_context->setUseL0Opt(use_L0_opt);

    /// Load global settings from default_profile and system_profile.
    global_context->setDefaultProfiles(config());
    Settings & settings = global_context->getSettingsRef();

    /// Size of cache for marks (index of MergeTree family of tables). It is necessary.
    size_t mark_cache_size = config().getUInt64("mark_cache_size");
    if (mark_cache_size)
        global_context->setMarkCache(mark_cache_size);

    /// Size of cache for minmax index, used by DeltaMerge engine.
    size_t minmax_index_cache_size
        = config().has("minmax_index_cache_size") ? config().getUInt64("minmax_index_cache_size") : mark_cache_size;
    if (minmax_index_cache_size)
        global_context->setMinMaxIndexCache(minmax_index_cache_size);

    /// Init TiFlash metrics.
    global_context->initializeTiFlashMetrics();

    /// Set path for format schema files
    auto format_schema_path = Poco::File(config().getString("format_schema_path", path + "format_schemas/"));
    global_context->setFormatSchemaPath(format_schema_path.path() + "/");
    format_schema_path.createDirectories();

    LOG_INFO(log, "Loading metadata.");
    loadMetadataSystem(*global_context);
    /// After attaching system databases we can initialize system log.
    global_context->initializeSystemLogs();
    /// After the system database is created, attach virtual system tables (in addition to query_log and part_log)
    attachSystemTablesServer(*global_context->getDatabase("system"), has_zookeeper);
    /// Load raft related configs ahead of loading metadata, as TMT storage relies on TMT context, which needs these configs.
    std::vector<std::string> pd_addrs;
    const std::string learner_key = "engine";
    const std::string learner_value = "tiflash";
    std::unordered_set<std::string> ignore_databases{"system"};
    std::string kvstore_path = path + "kvstore/";
    String flash_server_addr = config().getString("flash.service_addr", "0.0.0.0:3930");

    bool disable_bg_flush = false;

    ::TiDB::StorageEngine engine_if_empty = ::TiDB::StorageEngine::DT;
    ::TiDB::StorageEngine engine = engine_if_empty;

    if (config().has("raft"))
    {
        if (config().has("raft.pd_addr"))
        {
            String pd_service_addrs = config().getString("raft.pd_addr");
            Poco::StringTokenizer string_tokens(pd_service_addrs, ",");
            for (auto it = string_tokens.begin(); it != string_tokens.end(); it++)
            {
                pd_addrs.push_back(*it);
            }
            LOG_INFO(log, "Found pd addrs: " << pd_service_addrs);
        }
        else
        {
            LOG_INFO(log, "Not found pd addrs.");
        }

        if (config().has("raft.ignore_databases"))
        {
            String ignore_dbs = config().getString("raft.ignore_databases");
            Poco::StringTokenizer string_tokens(ignore_dbs, ",");
            std::stringstream ss;
            for (auto string_token : string_tokens)
            {
                string_token = Poco::trimInPlace(string_token);
                ignore_databases.emplace(string_token);
                ss << string_token << std::endl;
            }
            LOG_INFO(log, "Found ignore databases:\n" << ss.str());
        }

        if (config().has("raft.kvstore_path"))
        {
            kvstore_path = config().getString("raft.kvstore_path");
        }

        if (config().has("raft.storage_engine"))
        {
            String s_engine = config().getString("raft.storage_engine");
            std::transform(s_engine.begin(), s_engine.end(), s_engine.begin(), [](char ch) { return std::tolower(ch); });
            if (s_engine == "tmt")
                engine = ::TiDB::StorageEngine::TMT;
            else if (s_engine == "dt")
                engine = ::TiDB::StorageEngine::DT;
            else
                engine = engine_if_empty;
        }

        /// "tmt" engine ONLY support disable_bg_flush = false.
        /// "dt" engine ONLY support disable_bg_flush = true.

        String disable_bg_flush_conf = "raft.disable_bg_flush";
        if (engine == ::TiDB::StorageEngine::TMT)
        {
            if (config().has(disable_bg_flush_conf) && config().getBool(disable_bg_flush_conf))
                throw Exception("Illegal arguments: disable background flush while using engine " + MutableSupport::txn_storage_name,
                    ErrorCodes::INVALID_CONFIG_PARAMETER);
            disable_bg_flush = false;
        }
        else if (engine == ::TiDB::StorageEngine::DT)
        {
            /// If background flush is enabled, read will not triggle schema sync.
            /// Which means that we may get the wrong result with outdated schema.
            if (config().has(disable_bg_flush_conf) && !config().getBool(disable_bg_flush_conf))
                throw Exception("Illegal arguments: enable background flush while using engine " + MutableSupport::delta_tree_storage_name,
                    ErrorCodes::INVALID_CONFIG_PARAMETER);
            disable_bg_flush = true;
        }
    }

    {
        LOG_DEBUG(log, "Default storage engine: " << static_cast<Int64>(engine));
        /// create TMTContext
        global_context->createTMTContext(pd_addrs, learner_key, learner_value, ignore_databases, kvstore_path, engine, disable_bg_flush);
        global_context->getTMTContext().reloadConfig(config());
    }

    /// Then, load remaining databases
    loadMetadata(*global_context);
    LOG_DEBUG(log, "Loaded metadata.");

    global_context->setCurrentDatabase(default_database);

    /// Then, sync schemas with TiDB, and initialize schema sync service.
    for (int i = 0; i < 180; i++) // retry for 3 mins
    {
        try
        {
            global_context->getTMTContext().getSchemaSyncer()->syncSchemas(*global_context);
            break;
        }
        catch (Poco::Exception & e)
        {
            LOG_ERROR(
                log, "Bootstrap failed because sync schema error: " << e.displayText() << "\n We will sleep 3 seconds and try again.");
            ::sleep(1);
        }
    }
    LOG_DEBUG(log, "Sync schemas done.");
    global_context->initializeSchemaSyncService();

    SCOPE_EXIT({
        /** Ask to cancel background jobs all table engines,
          *  and also query_log.
          * It is important to do early, not in destructor of Context, because
          *  table engines could use Context on destroy.
          */
        LOG_INFO(log, "Shutting down storages.");
        global_context->shutdown();
        LOG_DEBUG(log, "Shutted down storages.");
    });

    {
        /// initialize TMTContext
        global_context->getTMTContext().restore();
    }

    /// Then, startup grpc server to serve raft and/or flash services.
    std::unique_ptr<FlashService> flash_service = nullptr;
    std::unique_ptr<grpc::Server> flash_grpc_server = nullptr;
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(flash_server_addr, grpc::InsecureServerCredentials());

        /// Init and register flash service.
        flash_service = std::make_unique<FlashService>(*this);
        builder.RegisterService(flash_service.get());
        LOG_INFO(log, "Flash service registered");

        /// Kick off grpc server.
        // Prevent TiKV from throwing "Received message larger than max (4404462 vs. 4194304)" error.
        builder.SetMaxReceiveMessageSize(-1);
        builder.SetMaxSendMessageSize(-1);
        flash_grpc_server = builder.BuildAndStart();
        LOG_INFO(log, "Flash grpc server listening on [" << flash_server_addr << "]");
    }

    SCOPE_EXIT({
        /// Shut down grpc server.
        // wait 5 seconds for pending rpcs to gracefully stop
        gpr_timespec deadline{5, 0, GPR_TIMESPAN};
        LOG_INFO(log, "Begin to shut down flash grpc server");
        flash_grpc_server->Shutdown(deadline);
        flash_grpc_server->Wait();
        flash_grpc_server.reset();
        LOG_INFO(log, "Shut down flash grpc server");

        /// Close flash service.
        LOG_INFO(log, "Begin to shut down flash service");
        flash_service.reset();
        LOG_INFO(log, "Shut down flash service");
    });

    TiFlashProxyConfig proxy_conf(config());
    TiFlashServer tiflash_instance_wrap{.tmt = global_context->getTMTContext()};
    TiFlashServerHelper helper{
        // a special number, also defined in proxy
        .magic_number = 0x13579BDF,
        .version = 4,
        .inner = &tiflash_instance_wrap,
        .fn_gc_buff = GcBuff,
        .fn_handle_write_raft_cmd = HandleWriteRaftCmd,
        .fn_handle_admin_raft_cmd = HandleAdminRaftCmd,
        .fn_handle_apply_snapshot = HandleApplySnapshot,
        .fn_atomic_update_proxy = AtomicUpdateProxy,
        .fn_handle_destroy = HandleDestroy,
        .fn_handle_ingest_sst = HandleIngestSST,
        .fn_handle_check_terminated = HandleCheckTerminated,
    };

    auto proxy_runner = std::thread([&proxy_conf, &log, &helper]() {
        if (!proxy_conf.inited)
            return;

        LOG_INFO(log, "Start tiflash proxy");
        run_tiflash_proxy_ffi((int)proxy_conf.args.size(), proxy_conf.args.data(), &helper);
        LOG_INFO(log, "End tiflash proxy");
    });

    SCOPE_EXIT({
        LOG_INFO(log, "Wait for tiflash proxy to join");
        proxy_runner.join();
        LOG_INFO(log, "TiFlash proxy finish");
    });

    if (has_zookeeper && config().has("distributed_ddl"))
    {
        /// DDL worker should be started after all tables were loaded
        String ddl_zookeeper_path = config().getString("distributed_ddl.path", "/clickhouse/task_queue/ddl/");
        global_context->setDDLWorker(std::make_shared<DDLWorker>(ddl_zookeeper_path, *global_context, &config(), "distributed_ddl"));
    }

    {
        Poco::Timespan keep_alive_timeout(config().getUInt("keep_alive_timeout", 10), 0);

        Poco::ThreadPool server_pool(3, config().getUInt("max_connections", 1024));
        Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
        http_params->setTimeout(settings.receive_timeout);
        http_params->setKeepAliveTimeout(keep_alive_timeout);

        std::vector<std::unique_ptr<Poco::Net::TCPServer>> servers;

        std::vector<std::string> listen_hosts = DB::getMultipleValuesFromConfig(config(), "", "listen_host");

        bool listen_try = config().getBool("listen_try", false);
        if (listen_hosts.empty())
        {
            listen_hosts.emplace_back("::1");
            listen_hosts.emplace_back("127.0.0.1");
            listen_try = true;
        }

        auto make_socket_address = [&](const std::string & host, UInt16 port) {
            Poco::Net::SocketAddress socket_address;
            try
            {
                socket_address = Poco::Net::SocketAddress(host, port);
            }
            catch (const Poco::Net::DNSException & e)
            {
                const auto code = e.code();
                if (code == EAI_FAMILY
#if defined(EAI_ADDRFAMILY)
                    || code == EAI_ADDRFAMILY
#endif
                )
                {
                    LOG_ERROR(log,
                        "Cannot resolve listen_host (" << host << "), error " << e.code() << ": " << e.message()
                                                       << ". "
                                                          "If it is an IPv6 address and your host has disabled IPv6, then consider to "
                                                          "specify IPv4 address to listen in <listen_host> element of configuration "
                                                          "file. Example: <listen_host>0.0.0.0</listen_host>");
                }

                throw;
            }
            return socket_address;
        };

        auto socket_bind_listen = [&](auto & socket, const std::string & host, UInt16 port, bool secure = 0) {
            auto address = make_socket_address(host, port);
#if !POCO_CLICKHOUSE_PATCH || POCO_VERSION <= 0x02000000 // TODO: fill correct version
            if (secure)
                /// Bug in old poco, listen() after bind() with reusePort param will fail because have no implementation in SecureServerSocketImpl
                /// https://github.com/pocoproject/poco/pull/2257
                socket.bind(address, /* reuseAddress = */ true);
            else
#endif
#if POCO_VERSION < 0x01080000
                socket.bind(address, /* reuseAddress = */ true);
#else
            socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ config().getBool("listen_reuse_port", false));
#endif

            socket.listen(/* backlog = */ config().getUInt("listen_backlog", 64));

            return address;
        };

        for (const auto & listen_host : listen_hosts)
        {
            /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
            try
            {
                /// HTTP
                if (config().has("http_port"))
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config().getInt("http_port"));
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(
                        new Poco::Net::HTTPServer(new HTTPHandlerFactory(*this, "HTTPHandler-factory"), server_pool, socket, http_params));

                    LOG_INFO(log, "Listening http://" + address.toString());
                }

                /// HTTPS
                if (config().has("https_port"))
                {
#if Poco_NetSSL_FOUND
                    std::call_once(ssl_init_once, SSLInit);

                    Poco::Net::SecureServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config().getInt("https_port"), /* secure = */ true);
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(
                        new Poco::Net::HTTPServer(new HTTPHandlerFactory(*this, "HTTPSHandler-factory"), server_pool, socket, http_params));

                    LOG_INFO(log, "Listening https://" + address.toString());
#else
                    throw Exception{"HTTPS protocol is disabled because Poco library was built without NetSSL support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                }

                /// TCP
                if (config().has("tcp_port"))
                {
                    std::call_once(ssl_init_once, SSLInit);
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config().getInt("tcp_port"));
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);
                    servers.emplace_back(
                        new Poco::Net::TCPServer(new TCPHandlerFactory(*this), server_pool, socket, new Poco::Net::TCPServerParams));

                    LOG_INFO(log, "Listening tcp: " + address.toString());
                }

                /// TCP with SSL
                if (config().has("tcp_port_secure"))
                {
#if Poco_NetSSL_FOUND
                    Poco::Net::SecureServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config().getInt("tcp_port_secure"), /* secure = */ true);
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);
                    servers.emplace_back(new Poco::Net::TCPServer(
                        new TCPHandlerFactory(*this, /* secure= */ true), server_pool, socket, new Poco::Net::TCPServerParams));
                    LOG_INFO(log, "Listening tcp_secure: " + address.toString());
#else
                    throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                }

                /// At least one of TCP and HTTP servers must be created.
                if (servers.empty())
                    throw Exception("No 'tcp_port' and 'http_port' is specified in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

                /// Interserver IO HTTP
                if (config().has("interserver_http_port"))
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config().getInt("interserver_http_port"));
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(new Poco::Net::HTTPServer(
                        new InterserverIOHTTPHandlerFactory(*this, "InterserverIOHTTPHandler-factory"), server_pool, socket, http_params));

                    LOG_INFO(log, "Listening interserver http: " + address.toString());
                }
            }
            catch (const Poco::Net::NetException & e)
            {
                if (listen_try)
                    LOG_ERROR(log,
                        "Listen [" << listen_host << "]: " << e.code() << ": " << e.what() << ": " << e.message()
                                   << "  If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider to "
                                      "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                                      "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                                      " Example for disabled IPv4: <listen_host>::</listen_host>");
                else
                    throw;
            }
        }

        if (servers.empty())
            throw Exception("No servers started (add valid listen_host and 'tcp_port' or 'http_port' to configuration file.)",
                ErrorCodes::NO_ELEMENTS_IN_CONFIG);

        for (auto & server : servers)
            server->start();

        main_config_reloader->start();
        users_config_reloader->start();

        {
            std::stringstream message;
            message << "Available RAM = " << formatReadableSizeWithBinarySuffix(getMemoryAmount()) << ";"
                    << " physical cores = " << getNumberOfPhysicalCPUCores()
                    << ";"
                    // on ARM processors it can show only enabled at current moment cores
                    << " threads = " << std::thread::hardware_concurrency() << ".";
            LOG_INFO(log, message.str());
        }

        LOG_INFO(log, "Ready for connections.");

        SCOPE_EXIT({
            LOG_DEBUG(log, "Received termination signal.");
            LOG_DEBUG(log, "Waiting for current connections to close.");

            is_cancelled = true;

            int current_connections = 0;
            for (auto & server : servers)
            {
                server->stop();
                current_connections += server->currentConnections();
            }

            LOG_DEBUG(log,
                "Closed all listening sockets."
                    << (current_connections ? " Waiting for " + toString(current_connections) + " outstanding connections." : ""));

            if (current_connections)
            {
                const int sleep_max_ms = 1000 * config().getInt("shutdown_wait_unfinished", 5);
                const int sleep_one_ms = 100;
                int sleep_current_ms = 0;
                while (sleep_current_ms < sleep_max_ms)
                {
                    current_connections = 0;
                    for (auto & server : servers)
                        current_connections += server->currentConnections();
                    if (!current_connections)
                        break;
                    sleep_current_ms += sleep_one_ms;
                    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_one_ms));
                }
            }

            LOG_DEBUG(log,
                "Closed connections." << (current_connections ? " But " + toString(current_connections)
                            + " remains."
                              " Tip: To increase wait time add to config: <shutdown_wait_unfinished>60</shutdown_wait_unfinished>"
                                                              : ""));

            main_config_reloader.reset();
            users_config_reloader.reset();
        });

        /// try to load dictionaries immediately, throw on error and die
        try
        {
            if (!config().getBool("dictionaries_lazy_load", true))
            {
                global_context->tryCreateEmbeddedDictionaries();
                global_context->tryCreateExternalDictionaries();
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "Caught exception while loading dictionaries.");
            throw;
        }

        /// This object will periodically calculate some metrics.
        AsynchronousMetrics async_metrics(*global_context);
        attachSystemTablesAsync(*global_context->getDatabase("system"), async_metrics);

        std::vector<std::unique_ptr<MetricsTransmitter>> metrics_transmitters;
        for (const auto & graphite_key : DB::getMultipleKeysFromConfig(config(), "", "graphite"))
        {
            metrics_transmitters.emplace_back(std::make_unique<MetricsTransmitter>(*global_context, async_metrics, graphite_key));
        }

        auto metrics_prometheus = std::make_unique<MetricsPrometheus>(*global_context, async_metrics);

        SessionCleaner session_cleaner(*global_context);
        ClusterManagerService cluster_manager_service(*global_context, config_path);

        waitForTerminationRequest();

        {
            global_context->getTMTContext().setTerminated();
            LOG_INFO(log, "Set tmt context terminated");
        }
    }

    return Application::EXIT_OK;
}
} // namespace DB

int mainEntryClickHouseServer(int argc, char ** argv)
{
    DB::Server app;
    try
    {
        return app.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
