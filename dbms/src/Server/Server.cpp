// Copyright 2023 PingCAP, Inc.
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

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Common/CPUAffinityManager.h>
#include <Common/ComputeLabelHolder.h>
#include <Common/Config/ConfigReloader.h>
#include <Common/CurrentMetrics.h>
#include <Common/DynamicThreadPool.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Macros.h>
#include <Common/RedactHelpers.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ThreadManager.h>
#include <Common/TiFlashBuildInfo.h>
#include <Common/TiFlashException.h>
#include <Common/TiFlashMetrics.h>
#include <Common/UniThreadPool.h>
#include <Common/assert_cast.h>
#include <Common/config.h>
#include <Common/escapeForFileName.h>
#include <Common/formatReadable.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfCPUCores.h>
#include <Common/setThreadName.h>
#include <Core/TiFlashDisaggregatedMode.h>
#include <Flash/DiagnosticsService.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <Functions/registerFunctions.h>
#include <IO/BaseFile/RateLimiter.h>
#include <IO/Encryption/DataKeyManager.h>
#include <IO/Encryption/KeyspacesKeyManager.h>
#include <IO/Encryption/MockKeyManager.h>
#include <IO/FileProvider/FileProvider.h>
#include <IO/HTTPCommon.h>
#include <IO/IOThreadPools.h>
#include <IO/ReadHelpers.h>
#include <IO/UseSSL.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Interpreters/loadMetadata.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Net/NetException.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Timestamp.h>
#include <Poco/Util/HelpFormatter.h>
#include <Server/BgStorageInit.h>
#include <Server/Bootstrap.h>
#include <Server/CertificateReloader.h>
#include <Server/MetricsPrometheus.h>
#include <Server/MetricsTransmitter.h>
#include <Server/RaftConfigParser.h>
#include <Server/Server.h>
#include <Server/ServerInfo.h>
#include <Server/StatusFile.h>
#include <Server/StorageConfigParser.h>
#include <Server/TCPHandlerFactory.h>
#include <Server/UserConfigParser.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>
#include <Storages/DeltaMerge/ReadThread/ColumnSharingCache.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReadTaskScheduler.h>
#include <Storages/DeltaMerge/ReadThread/SegmentReader.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/FormatVersion.h>
#include <Storages/IManageableStorage.h>
#include <Storages/KVStore/FFI/FileEncryption.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/TiKVHelpers/PDTiKVClient.h>
#include <Storages/Page/V3/Universal/UniversalPageStorage.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Common.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/registerStorages.h>
#include <TableFunctions/registerTableFunctions.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <WindowFunctions/registerWindowFunctions.h>
#include <boost_wrapper/string_split.h>
#include <common/ErrorHandlers.h>
#include <common/config_common.h>
#include <common/logger_useful.h>
#include <sys/resource.h>

#include <boost/algorithm/string/classification.hpp>
#include <ext/scope_guard.h>
#include <magic_enum.hpp>
#include <memory>
#include <thread>

#if Poco_NetSSL_FOUND
#include <Common/grpcpp.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SecureServerSocket.h>
#endif

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

#if USE_MIMALLOC
#include <Poco/JSON/Parser.h>
#include <mimalloc.h>

#include <fstream>
#endif

#ifdef FIU_ENABLE
#include <fiu.h>
#endif


#if USE_MIMALLOC
#define TRY_LOAD_CONF(NAME)                          \
    {                                                \
        try                                          \
        {                                            \
            auto value = obj->getValue<long>(#NAME); \
            mi_option_set(NAME, value);              \
        }                                            \
        catch (...)                                  \
        {}                                           \
    }

void loadMiConfig(Logger * log)
{
    auto config = getenv("MIMALLOC_CONF");
    if (config)
    {
        LOG_INFO(log, "Got environment variable MIMALLOC_CONF: {}", config);
        Poco::JSON::Parser parser;
        std::ifstream data{config};
        Poco::Dynamic::Var result = parser.parse(data);
        auto obj = result.extract<Poco::JSON::Object::Ptr>();
        TRY_LOAD_CONF(mi_option_show_errors);
        TRY_LOAD_CONF(mi_option_show_stats);
        TRY_LOAD_CONF(mi_option_verbose);
        TRY_LOAD_CONF(mi_option_eager_commit);
        TRY_LOAD_CONF(mi_option_eager_region_commit);
        TRY_LOAD_CONF(mi_option_large_os_pages);
        TRY_LOAD_CONF(mi_option_reserve_huge_os_pages);
        TRY_LOAD_CONF(mi_option_segment_cache);
        TRY_LOAD_CONF(mi_option_page_reset);
        TRY_LOAD_CONF(mi_option_segment_reset);
        TRY_LOAD_CONF(mi_option_reset_delay);
        TRY_LOAD_CONF(mi_option_use_numa_nodes);
        TRY_LOAD_CONF(mi_option_reset_decommits);
        TRY_LOAD_CONF(mi_option_eager_commit_delay);
        TRY_LOAD_CONF(mi_option_os_tag);
    }
}
#undef TRY_LOAD_CONF
#endif

namespace
{
[[maybe_unused]] void tryLoadBoolConfigFromEnv(const DB::LoggerPtr & log, bool & target, const char * name)
{
    auto * config = getenv(name);
    if (config)
    {
        LOG_INFO(log, "Got environment variable {} = {}", name, config);
        try
        {
            auto result = std::stoul(config);
            if (result != 0 && result != 1)
            {
                LOG_ERROR(log, "Environment variable{} = {} is not valid", name, result);
                return;
            }
            target = result;
        }
        catch (...)
        {}
    }
}
} // namespace

namespace CurrentMetrics
{
extern const Metric LogicalCPUCores;
extern const Metric MemoryCapacity;
} // namespace CurrentMetrics

namespace DB
{
namespace ErrorCodes
{
extern const int NO_ELEMENTS_IN_CONFIG;
extern const int SUPPORT_IS_DISABLED;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int INVALID_CONFIG_PARAMETER;
} // namespace ErrorCodes

namespace Debug
{
extern void setServiceAddr(const std::string & addr);
}

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

void Server::defineOptions(Poco::Util::OptionSet & options)
{
    options.addOption(
        Poco::Util::Option("help", "h", "show help and exit").required(false).repeatable(false).binding("help"));
    BaseDaemon::defineOptions(options);
}

int Server::run()
{
    if (config().hasOption("help"))
    {
        Poco::Util::HelpFormatter help_formatter(Server::options());
        auto header_str = fmt::format(
            "{} server [OPTION] [-- [POSITIONAL_ARGS]...]\n"
            "POSITIONAL_ARGS can be used to rewrite config properties, for example, --http_port=8010",
            commandName());
        help_formatter.setHeader(header_str);
        help_formatter.format(std::cout);
        return 0;
    }
    return BaseDaemon::run();
}

std::string Server::getDefaultCorePath() const
{
    return getCanonicalPath(config().getString("path")) + "cores";
}

struct TiFlashProxyConfig
{
    std::vector<const char *> args;
    std::unordered_map<std::string, std::string> val_map;
    bool is_proxy_runnable = false;

    // TiFlash Proxy will set the default value of "flash.proxy.addr", so we don't need to set here.

    void addExtraArgs(const std::string & k, const std::string & v)
    {
        std::string key = "--" + k;
        val_map[key] = v;
        auto iter = val_map.find(key);
        args.push_back(iter->first.data());
        args.push_back(iter->second.data());
    }

    explicit TiFlashProxyConfig(Poco::Util::LayeredConfiguration & config, bool has_s3_config)
    {
        auto disaggregated_mode = getDisaggregatedMode(config);

        // tiflash_compute doesn't need proxy.
        if (disaggregated_mode == DisaggregatedMode::Compute && useAutoScaler(config))
        {
            LOG_INFO(
                Logger::get(),
                "TiFlash Proxy will not start because AutoScale Disaggregated Compute Mode is specified.");
            return;
        }

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("flash.proxy", keys);
        if (!config.has("raft.pd_addr"))
        {
            LOG_WARNING(Logger::get(), "TiFlash Proxy will not start because `raft.pd_addr` is not configured.");
            if (!keys.empty())
                LOG_WARNING(Logger::get(), "`flash.proxy.*` is ignored because TiFlash Proxy will not start.");

            return;
        }

        {
            std::unordered_map<std::string, std::string> args_map;
            for (const auto & key : keys)
                args_map[key] = config.getString("flash.proxy." + key);

            args_map["pd-endpoints"] = config.getString("raft.pd_addr");
            args_map["engine-version"] = TiFlashBuildInfo::getReleaseVersion();
            args_map["engine-git-hash"] = TiFlashBuildInfo::getGitHash();
            if (!args_map.contains("engine-addr"))
                args_map["engine-addr"] = config.getString("flash.service_addr", "0.0.0.0:3930");
            else
                args_map["advertise-engine-addr"] = args_map["engine-addr"];
            args_map["engine-label"] = getProxyLabelByDisaggregatedMode(disaggregated_mode);
            if (disaggregated_mode != DisaggregatedMode::Compute && has_s3_config)
                args_map["engine-role-label"] = DISAGGREGATED_MODE_WRITE_ENGINE_ROLE;

            for (auto && [k, v] : args_map)
                val_map.emplace("--" + k, std::move(v));
        }

        args.push_back("TiFlash Proxy");
        for (const auto & v : val_map)
        {
            args.push_back(v.first.data());
            args.push_back(v.second.data());
        }
        is_proxy_runnable = true;
    }
};

pingcap::ClusterConfig getClusterConfig(
    TiFlashSecurityConfigPtr security_config,
    const int api_version,
    const LoggerPtr & log)
{
    pingcap::ClusterConfig config;
    config.tiflash_engine_key = "engine";
    config.tiflash_engine_value = DEF_PROXY_LABEL;
    auto [ca_path, cert_path, key_path] = security_config->getPaths();
    config.ca_path = ca_path;
    config.cert_path = cert_path;
    config.key_path = key_path;
    switch (api_version)
    {
    case 1:
        config.api_version = kvrpcpb::APIVersion::V1;
        break;
    case 2:
        config.api_version = kvrpcpb::APIVersion::V2;
        break;
    default:
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Invalid api version {}", api_version);
    }
    LOG_INFO(
        log,
        "update cluster config, ca_path: {}, cert_path: {}, key_path: {}, api_version: {}",
        ca_path,
        cert_path,
        key_path,
        fmt::underlying(config.api_version));
    return config;
}

LoggerPtr grpc_log;

void printGRPCLog(gpr_log_func_args * args)
{
    String log_msg = fmt::format("{}, line number: {}, log msg : {}", args->file, args->line, args->message);
    if (args->severity == GPR_LOG_SEVERITY_DEBUG)
    {
        LOG_DEBUG(grpc_log, log_msg);
    }
    else if (args->severity == GPR_LOG_SEVERITY_INFO)
    {
        LOG_INFO(grpc_log, log_msg);
    }
    else if (args->severity == GPR_LOG_SEVERITY_ERROR)
    {
        LOG_ERROR(grpc_log, log_msg);
    }
}

struct TCPServer : Poco::Net::TCPServer
{
    TCPServer(
        Poco::Net::TCPServerConnectionFactory::Ptr pFactory,
        Poco::ThreadPool & threadPool,
        const Poco::Net::ServerSocket & socket,
        Poco::Net::TCPServerParams::Ptr pParams)
        : Poco::Net::TCPServer(pFactory, threadPool, socket, pParams)
    {}

protected:
    void run() override
    {
        setThreadName("TCPServer");
        Poco::Net::TCPServer::run();
    }
};

void UpdateMallocConfig([[maybe_unused]] const LoggerPtr & log)
{
#ifdef RUN_FAIL_RETURN
    static_assert(false);
#endif
#define RUN_FAIL_RETURN(f)                                    \
    do                                                        \
    {                                                         \
        if (f)                                                \
        {                                                     \
            LOG_ERROR(log, "Fail to update jemalloc config"); \
            return;                                           \
        }                                                     \
    } while (0)
#if USE_JEMALLOC
    const char * version;
    bool old_b, new_b = true;
    size_t old_max_thd, new_max_thd = 1;
    size_t sz_b = sizeof(bool), sz_st = sizeof(size_t), sz_ver = sizeof(version);

    RUN_FAIL_RETURN(je_mallctl("version", &version, &sz_ver, nullptr, 0));
    LOG_INFO(log, "Got jemalloc version: {}", version);

    auto * malloc_conf = getenv("MALLOC_CONF");
    if (malloc_conf)
    {
        LOG_INFO(log, "Got environment variable MALLOC_CONF: {}", malloc_conf);
    }
    else
    {
        LOG_INFO(log, "Not found environment variable MALLOC_CONF");
    }

    RUN_FAIL_RETURN(je_mallctl("opt.background_thread", (void *)&old_b, &sz_b, nullptr, 0));
    RUN_FAIL_RETURN(je_mallctl("opt.max_background_threads", (void *)&old_max_thd, &sz_st, nullptr, 0));

    LOG_INFO(log, "Got jemalloc config: opt.background_thread {}, opt.max_background_threads {}", old_b, old_max_thd);

    if (!malloc_conf && !old_b)
    {
        LOG_INFO(log, "Try to use background_thread of jemalloc to handle purging asynchronously");

        RUN_FAIL_RETURN(je_mallctl("max_background_threads", nullptr, nullptr, (void *)&new_max_thd, sz_st));
        LOG_INFO(log, "Set jemalloc.max_background_threads {}", new_max_thd);

        RUN_FAIL_RETURN(je_mallctl("background_thread", nullptr, nullptr, (void *)&new_b, sz_b));
        LOG_INFO(log, "Set jemalloc.background_thread {}", new_b);
    }
#endif

#if USE_MIMALLOC
#define MI_OPTION_SHOW(OPTION) LOG_INFO(log, "mimalloc." #OPTION ": {}", mi_option_get(OPTION));

    int version = mi_version();
    LOG_INFO(log, "Got mimalloc version: {}.{}.{}", (version / 100), ((version % 100) / 10), (version % 10));
    loadMiConfig(log);
    MI_OPTION_SHOW(mi_option_show_errors);
    MI_OPTION_SHOW(mi_option_show_stats);
    MI_OPTION_SHOW(mi_option_verbose);
    MI_OPTION_SHOW(mi_option_eager_commit);
    MI_OPTION_SHOW(mi_option_eager_region_commit);
    MI_OPTION_SHOW(mi_option_large_os_pages);
    MI_OPTION_SHOW(mi_option_reserve_huge_os_pages);
    MI_OPTION_SHOW(mi_option_segment_cache);
    MI_OPTION_SHOW(mi_option_page_reset);
    MI_OPTION_SHOW(mi_option_segment_reset);
    MI_OPTION_SHOW(mi_option_reset_delay);
    MI_OPTION_SHOW(mi_option_use_numa_nodes);
    MI_OPTION_SHOW(mi_option_reset_decommits);
    MI_OPTION_SHOW(mi_option_eager_commit_delay);
    MI_OPTION_SHOW(mi_option_os_tag);
#undef MI_OPTION_SHOW
#endif
#undef RUN_FAIL_RETURN
}

extern "C" {
void run_raftstore_proxy_ffi(int argc, const char * const * argv, const EngineStoreServerHelper *);
}

struct RaftStoreProxyRunner : boost::noncopyable
{
    struct RunRaftStoreProxyParms
    {
        const EngineStoreServerHelper * helper;
        const TiFlashProxyConfig & conf;

        /// set big enough stack size to avoid runtime error like stack-overflow.
        size_t stack_size = 1024 * 1024 * 20;
    };

    RaftStoreProxyRunner(RunRaftStoreProxyParms && parms_, const LoggerPtr & log_)
        : parms(std::move(parms_))
        , log(log_)
    {}

    void join() const
    {
        if (!parms.conf.is_proxy_runnable)
            return;
        pthread_join(thread, nullptr);
    }

    void run()
    {
        if (!parms.conf.is_proxy_runnable)
            return;
        pthread_attr_t attribute;
        pthread_attr_init(&attribute);
        pthread_attr_setstacksize(&attribute, parms.stack_size);
        LOG_INFO(log, "start raft store proxy");
        pthread_create(&thread, &attribute, runRaftStoreProxyFFI, &parms);
        pthread_attr_destroy(&attribute);
    }

private:
    static void * runRaftStoreProxyFFI(void * pv)
    {
        setThreadName("RaftStoreProxy");
        const auto & parms = *static_cast<const RunRaftStoreProxyParms *>(pv);
        run_raftstore_proxy_ffi(static_cast<int>(parms.conf.args.size()), parms.conf.args.data(), parms.helper);
        return nullptr;
    }

    RunRaftStoreProxyParms parms;
    pthread_t thread{};
    const LoggerPtr & log;
};

class Server::TcpHttpServersHolder
{
public:
    TcpHttpServersHolder(Server & server_, const Settings & settings, const LoggerPtr & log_)
        : server(server_)
        , log(log_)
        , server_pool(1, server.config().getUInt("max_connections", 1024))
    {
        auto & config = server.config();
        auto security_config = server.global_context->getSecurityConfig();

        Poco::Timespan keep_alive_timeout(config.getUInt("keep_alive_timeout", 10), 0);
        Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams; // NOLINT
        http_params->setTimeout(settings.receive_timeout);
        http_params->setKeepAliveTimeout(keep_alive_timeout);

        std::vector<std::string> listen_hosts = DB::getMultipleValuesFromConfig(config, "", "listen_host");

        bool listen_try = config.getBool("listen_try", false);
        if (listen_hosts.empty())
        {
            listen_hosts.emplace_back("0.0.0.0");
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
                    LOG_ERROR(
                        log,
                        "Cannot resolve listen_host ({}), error {}: {}."
                        "If it is an IPv6 address and your host has disabled IPv6, then consider to "
                        "specify IPv4 address to listen in <listen_host> element of configuration "
                        "file. Example: <listen_host>0.0.0.0</listen_host>",
                        host,
                        e.code(),
                        e.message());
                }

                throw;
            }
            return socket_address;
        };

        auto socket_bind_listen = [&](auto & socket, const std::string & host, UInt16 port, bool secure = false) {
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
            socket.bind(
                address,
                /* reuseAddress = */ true,
                /* reusePort = */ config.getBool("listen_reuse_port", false));
#endif

            socket.listen(/* backlog = */ config.getUInt("listen_backlog", 64));

            return address;
        };

        for (const auto & listen_host : listen_hosts)
        {
            /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
            try
            {
                /// TCP
                if (config.has("tcp_port"))
                {
                    if (security_config->hasTlsConfig())
                    {
                        LOG_ERROR(log, "tls config is set but tcp_port_secure is not set.");
                    }
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config.getInt("tcp_port"));
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);
                    servers.emplace_back(new TCPServer(
                        new TCPHandlerFactory(server),
                        server_pool,
                        socket,
                        new Poco::Net::TCPServerParams));

                    LOG_INFO(log, "Listening tcp: {}", address.toString());
                }
                else if (security_config->hasTlsConfig())
                {
                    LOG_INFO(log, "tcp_port is closed because tls config is set");
                }

                /// TCP with SSL (Not supported yet)
                if (config.has("tcp_port_secure") && !security_config->hasTlsConfig())
                {
#if Poco_NetSSL_FOUND
                    auto [ca_path, cert_path, key_path] = security_config->getPaths();
                    Poco::Net::Context::Ptr context
                        = new Poco::Net::Context(Poco::Net::Context::TLSV1_2_SERVER_USE, key_path, cert_path, ca_path);
                    CertificateReloader::initSSLCallback(context, server.global_context.get());
                    Poco::Net::SecureServerSocket socket(context);
                    auto address = socket_bind_listen(
                        socket,
                        listen_host,
                        config.getInt("tcp_port_secure"),
                        /* secure = */ true);
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);
                    servers.emplace_back(new TCPServer(
                        new TCPHandlerFactory(server, /* secure= */ true),
                        server_pool,
                        socket,
                        new Poco::Net::TCPServerParams));
                    LOG_INFO(log, "Listening tcp_secure: {}", address.toString());
#else
                    throw Exception{
                        "SSL support for TCP protocol is disabled because Poco library was built without NetSSL "
                        "support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                }
                else if (security_config->hasTlsConfig())
                {
                    LOG_INFO(log, "tcp_port_secure is closed because tls config is set");
                }

                // No TCP server is normal now because we only enable the TCP server
                // under testing deployment
                if (servers.empty())
                    LOG_INFO(log, "No TCP server is created");
            }
            catch (const Poco::Net::NetException & e)
            {
                if (listen_try)
                    LOG_ERROR(
                        log,
                        "Listen [{}]: {}: {}: {}"
                        "  If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider to "
                        "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                        "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                        " Example for disabled IPv4: <listen_host>::</listen_host>",
                        listen_host,
                        e.code(),
                        e.what(),
                        e.message());
                else
                    throw;
            }
        }

        for (auto & server : servers)
            server->start();
    }

    // terminate all TCP servers when receive exit signal
    void onExit()
    {
        auto & config = server.config();

        LOG_INFO(log, "Received termination signal, stopping server...");
        LOG_INFO(log, "Waiting for current connections to close.");

        int current_connections = 0;
        for (auto & server : servers)
        {
            server->stop();
            current_connections += server->currentConnections();
        }

        String debug_msg = "Closed all listening sockets.";
        if (current_connections)
        {
            LOG_INFO(log, "{} Waiting for {} outstanding connections.", debug_msg, current_connections);
            const int sleep_max_ms = 1000 * config.getInt("shutdown_wait_unfinished", 5);
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
        else
        {
            LOG_INFO(log, debug_msg);
        }

        debug_msg = "Closed connections.";
        if (current_connections)
            LOG_INFO(
                log,
                "{} But {} remains."
                " Tip: To increase wait time add to config: <shutdown_wait_unfinished>60</shutdown_wait_unfinished>",
                debug_msg,
                current_connections);
        else
            LOG_INFO(log, debug_msg);
    }

private:
    Server & server;
    const LoggerPtr & log;
    Poco::ThreadPool server_pool;
    std::vector<std::unique_ptr<Poco::Net::TCPServer>> servers;
};

// By default init global thread pool by hardware_concurrency
// Later we will adjust it by `adjustThreadPoolSize`
void initThreadPool(Poco::Util::LayeredConfiguration & config)
{
    size_t default_num_threads = std::max(4UL, 2 * std::thread::hardware_concurrency());

    // Note: Global Thread Pool must be larger than sub thread pools.
    GlobalThreadPool::initialize(
        /*max_threads*/ default_num_threads * 20,
        /*max_free_threads*/ default_num_threads,
        /*queue_size*/ default_num_threads * 8);

    auto disaggregated_mode = getDisaggregatedMode(config);
    if (disaggregated_mode == DisaggregatedMode::Compute)
    {
        BuildReadTaskForWNPool::initialize(
            /*max_threads*/ default_num_threads,
            /*max_free_threads*/ default_num_threads / 2,
            /*queue_size*/ default_num_threads * 2);

        BuildReadTaskForWNTablePool::initialize(
            /*max_threads*/ default_num_threads,
            /*max_free_threads*/ default_num_threads / 2,
            /*queue_size*/ default_num_threads * 2);

        BuildReadTaskPool::initialize(
            /*max_threads*/ default_num_threads,
            /*max_free_threads*/ default_num_threads / 2,
            /*queue_size*/ default_num_threads * 2);

        RNWritePageCachePool::initialize(
            /*max_threads*/ default_num_threads,
            /*max_free_threads*/ default_num_threads / 2,
            /*queue_size*/ default_num_threads * 2);
    }

    if (disaggregated_mode == DisaggregatedMode::Compute || disaggregated_mode == DisaggregatedMode::Storage)
    {
        DataStoreS3Pool::initialize(
            /*max_threads*/ default_num_threads,
            /*max_free_threads*/ default_num_threads / 2,
            /*queue_size*/ default_num_threads * 2);
        S3FileCachePool::initialize(
            /*max_threads*/ default_num_threads,
            /*max_free_threads*/ default_num_threads / 2,
            /*queue_size*/ default_num_threads * 2);
    }

    if (disaggregated_mode == DisaggregatedMode::Storage)
    {
        WNEstablishDisaggTaskPool::initialize(
            /*max_threads*/ default_num_threads,
            /*max_free_threads*/ default_num_threads / 2,
            /*queue_size*/ default_num_threads * 2);
    }
}

void adjustThreadPoolSize(const Settings & settings, size_t logical_cores)
{
    // TODO: make BackgroundPool/BlockableBackgroundPool/DynamicThreadPool spawned from `GlobalThreadPool`
    size_t max_io_thread_count = std::ceil(settings.io_thread_count_scale * logical_cores);
    // Note: Global Thread Pool must be larger than sub thread pools.
    GlobalThreadPool::instance().setMaxThreads(max_io_thread_count * 200);
    GlobalThreadPool::instance().setMaxFreeThreads(max_io_thread_count);
    GlobalThreadPool::instance().setQueueSize(max_io_thread_count * 400);

    if (BuildReadTaskForWNPool::instance)
    {
        BuildReadTaskForWNPool::instance->setMaxThreads(max_io_thread_count);
        BuildReadTaskForWNPool::instance->setMaxFreeThreads(max_io_thread_count / 2);
        BuildReadTaskForWNPool::instance->setQueueSize(max_io_thread_count * 2);
    }
    if (BuildReadTaskForWNTablePool::instance)
    {
        BuildReadTaskForWNTablePool::instance->setMaxThreads(max_io_thread_count);
        BuildReadTaskForWNTablePool::instance->setMaxFreeThreads(max_io_thread_count / 2);
        BuildReadTaskForWNTablePool::instance->setQueueSize(max_io_thread_count * 2);
    }
    if (BuildReadTaskPool::instance)
    {
        BuildReadTaskPool::instance->setMaxThreads(max_io_thread_count);
        BuildReadTaskPool::instance->setMaxFreeThreads(max_io_thread_count / 2);
        BuildReadTaskPool::instance->setQueueSize(max_io_thread_count * 2);
    }
    if (DataStoreS3Pool::instance)
    {
        DataStoreS3Pool::instance->setMaxThreads(max_io_thread_count);
        DataStoreS3Pool::instance->setMaxFreeThreads(max_io_thread_count / 2);
        DataStoreS3Pool::instance->setQueueSize(max_io_thread_count * 2);
    }
    if (S3FileCachePool::instance)
    {
        S3FileCachePool::instance->setMaxThreads(max_io_thread_count);
        S3FileCachePool::instance->setMaxFreeThreads(max_io_thread_count / 2);
        S3FileCachePool::instance->setQueueSize(max_io_thread_count * 2);
    }
    if (RNWritePageCachePool::instance)
    {
        RNWritePageCachePool::instance->setMaxThreads(max_io_thread_count);
        RNWritePageCachePool::instance->setMaxFreeThreads(max_io_thread_count / 2);
        RNWritePageCachePool::instance->setQueueSize(max_io_thread_count * 2);
    }

    size_t max_cpu_thread_count = std::ceil(settings.cpu_thread_count_scale * logical_cores);
    if (WNEstablishDisaggTaskPool::instance)
    {
        // Tasks of EstablishDisaggTask is computation-intensive.
        WNEstablishDisaggTaskPool::instance->setMaxThreads(max_cpu_thread_count);
        WNEstablishDisaggTaskPool::instance->setMaxFreeThreads(max_cpu_thread_count / 2);
        WNEstablishDisaggTaskPool::instance->setQueueSize(max_cpu_thread_count * 2);
    }
}

void syncSchemaWithTiDB(
    const TiFlashStorageConfig & storage_config,
    BgStorageInitHolder & bg_init_stores,
    const std::unique_ptr<Context> & global_context,
    const LoggerPtr & log)
{
    /// Then, sync schemas with TiDB, and initialize schema sync service.
    /// If in API V2 mode, each keyspace's schema is fetch lazily.
    if (storage_config.api_version == 1)
    {
        Stopwatch watch;
        while (watch.elapsedSeconds() < global_context->getSettingsRef().ddl_restart_wait_seconds) // retry for 3 mins
        {
            try
            {
                global_context->getTMTContext().getSchemaSyncerManager()->syncSchemas(*global_context, NullspaceID);
                break;
            }
            catch (Poco::Exception & e)
            {
                const int wait_seconds = 3;
                LOG_ERROR(
                    log,
                    "Bootstrap failed because sync schema error: {}\nWe will sleep for {}"
                    " seconds and try again.",
                    e.displayText(),
                    wait_seconds);
                ::sleep(wait_seconds);
            }
        }
        LOG_DEBUG(log, "Sync schemas done.");
    }

    // Init the DeltaMergeStore instances if data exist.
    // Make the disk usage correct and prepare for serving
    // queries.
    bg_init_stores
        .start(*global_context, log, storage_config.lazily_init_store, storage_config.s3_config.isS3Enabled());

    // init schema sync service with tidb
    global_context->initializeSchemaSyncService();
}

int Server::main(const std::vector<std::string> & /*args*/)
{
    setThreadName("TiFlashMain");

    /// Initialize the labels of tiflash compute node.
    ComputeLabelHolder::instance().init(config());

    UseSSL ssl_holder;

    const auto log = Logger::get();
#ifdef FIU_ENABLE
    fiu_init(0); // init failpoint
    FailPointHelper::initRandomFailPoints(config(), log);
#endif

    UpdateMallocConfig(log);

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    tryLoadBoolConfigFromEnv(log, simd_option::ENABLE_AVX, "TIFLASH_ENABLE_AVX");
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
    tryLoadBoolConfigFromEnv(log, simd_option::ENABLE_AVX512, "TIFLASH_ENABLE_AVX512");
#endif

#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
    tryLoadBoolConfigFromEnv(log, simd_option::ENABLE_ASIMD, "TIFLASH_ENABLE_ASIMD");
#endif

#ifdef TIFLASH_ENABLE_SVE_SUPPORT
    tryLoadBoolConfigFromEnv(log, simd_option::ENABLE_SVE, "TIFLASH_ENABLE_SVE");
#endif
    registerFunctions();
    registerAggregateFunctions();
    registerWindowFunctions();
    registerTableFunctions();
    registerStorages();

    // Later we may create thread pool from GlobalThreadPool
    // init it before other components
    initThreadPool(config());

    TiFlashErrorRegistry::instance(); // This invocation is for initializing

    DM::ScanContext::initCurrentInstanceId(config(), log);

    const auto disaggregated_mode = getDisaggregatedMode(config());
    const auto use_autoscaler = useAutoScaler(config());

    // Some Storage's config is necessary for Proxy
    TiFlashStorageConfig storage_config;
    // Deprecated settings.
    // `global_capacity_quota` will be ignored if `storage_config.main_capacity_quota` is not empty.
    // "0" by default, means no quota, the actual disk capacity is used.
    size_t global_capacity_quota = 0;
    std::tie(global_capacity_quota, storage_config) = TiFlashStorageConfig::parseSettings(config(), log);
    if (!storage_config.s3_config.bucket.empty())
    {
        storage_config.s3_config.enable(/*check_requirements*/ true, log);
    }
    else if (disaggregated_mode == DisaggregatedMode::Compute && use_autoscaler)
    {
        // compute node with auto scaler, the requirements will be initted later.
        storage_config.s3_config.enable(/*check_requirements*/ false, log);
    }

    if (storage_config.format_version != 0)
    {
        if (storage_config.s3_config.isS3Enabled() && storage_config.format_version != STORAGE_FORMAT_V100.identifier)
        {
            LOG_WARNING(log, "'storage.format_version' must be set to 100 when S3 is enabled!");
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "'storage.format_version' must be set to 100 when S3 is enabled!");
        }
        setStorageFormat(storage_config.format_version);
        LOG_INFO(log, "Using format_version={} (explicit storage format detected).", storage_config.format_version);
    }
    else
    {
        if (storage_config.s3_config.isS3Enabled())
        {
            // If the user does not explicitly set format_version in the config file but
            // enables S3, then we set up a proper format version to support S3.
            setStorageFormat(STORAGE_FORMAT_V100.identifier);
            LOG_INFO(log, "Using format_version={} (infer by S3 is enabled).", STORAGE_FORMAT_V100.identifier);
        }
        else
        {
            // Use the default settings
            LOG_INFO(log, "Using format_version={} (default settings).", STORAGE_FORMAT_CURRENT.identifier);
        }
    }

    // sanitize check for disagg mode
    if (storage_config.s3_config.isS3Enabled())
    {
        if (auto disaggregated_mode = getDisaggregatedMode(config()); disaggregated_mode == DisaggregatedMode::None)
        {
            LOG_WARNING(log, "'flash.disaggregated_mode' must be set when S3 is enabled!");
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "'flash.disaggregated_mode' must be set when S3 is enabled!");
        }
    }

    LOG_INFO(log, "Using api_version={}", storage_config.api_version);

    // Set whether to use safe point v2.
    PDClientHelper::enable_safepoint_v2 = config().getBool("enable_safe_point_v2", false);

    // Init Proxy's config
    TiFlashProxyConfig proxy_conf(config(), storage_config.s3_config.isS3Enabled());
    EngineStoreServerWrap tiflash_instance_wrap{};
    auto helper = GetEngineStoreServerHelper(&tiflash_instance_wrap);

    if (STORAGE_FORMAT_CURRENT.page == PageFormat::V4)
    {
        LOG_INFO(log, "Using UniPS for proxy");
        proxy_conf.addExtraArgs("unips-enabled", "1");
    }
    else
    {
        LOG_INFO(log, "UniPS is not enabled for proxy, page_version={}", STORAGE_FORMAT_CURRENT.page);
    }

#ifdef USE_JEMALLOC
    LOG_INFO(log, "Using Jemalloc for TiFlash");
#else
    LOG_INFO(log, "Not using Jemalloc for TiFlash");
#endif

    RaftStoreProxyRunner proxy_runner(RaftStoreProxyRunner::RunRaftStoreProxyParms{&helper, proxy_conf}, log);

    if (proxy_conf.is_proxy_runnable)
    {
        proxy_runner.run();

        LOG_INFO(log, "wait for tiflash proxy initializing");
        while (!tiflash_instance_wrap.proxy_helper)
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        LOG_INFO(log, "tiflash proxy is initialized");
    }
    else
    {
        LOG_WARNING(log, "Skipped initialize TiFlash Proxy");
    }

    SCOPE_EXIT({
        if (!proxy_conf.is_proxy_runnable)
            return;

        LOG_INFO(log, "Let tiflash proxy shutdown");
        tiflash_instance_wrap.status = EngineStoreServerStatus::Terminated;
        tiflash_instance_wrap.tmt = nullptr;
        LOG_INFO(log, "Wait for tiflash proxy thread to join");
        proxy_runner.join();
        LOG_INFO(log, "tiflash proxy thread is joined");
    });

    /// get CPU/memory/disk info of this server
    diagnosticspb::ServerInfoRequest request;
    diagnosticspb::ServerInfoResponse response;
    request.set_tp(static_cast<diagnosticspb::ServerInfoType>(1));
    std::string req = request.SerializeAsString();
    ffi_get_server_info_from_proxy(reinterpret_cast<intptr_t>(&helper), strIntoView(&req), &response);
    server_info.parseSysInfo(response);
    setNumberOfLogicalCPUCores(server_info.cpu_info.logical_cores);
    computeAndSetNumberOfPhysicalCPUCores(server_info.cpu_info.logical_cores, server_info.cpu_info.physical_cores);
    LOG_INFO(log, "ServerInfo: {}", server_info.debugString());

    grpc_log = Logger::get("grpc");
    gpr_set_log_verbosity(GPR_LOG_SEVERITY_DEBUG);
    gpr_set_log_function(&printGRPCLog);

    /** Context contains all that query execution is dependent:
      *  settings, available functions, data types, aggregate functions, databases...
      */
    global_context = Context::createGlobal();
    SCOPE_EXIT({
        if (!proxy_conf.is_proxy_runnable)
            return;

        LOG_INFO(log, "Unlink tiflash_instance_wrap.tmt");
        // Reset the `tiflash_instance_wrap.tmt` before `global_context` get released, or it will be a dangling pointer
        tiflash_instance_wrap.tmt = nullptr;
    });
    global_context->setApplicationType(Context::ApplicationType::SERVER);
    global_context->getSharedContextDisagg()->disaggregated_mode = disaggregated_mode;
    global_context->getSharedContextDisagg()->use_autoscaler = use_autoscaler;

    // Must init this before KVStore.
    global_context->initializeJointThreadInfoJeallocMap();

    /// Init File Provider
    if (proxy_conf.is_proxy_runnable)
    {
        const bool enable_encryption = tiflash_instance_wrap.proxy_helper->checkEncryptionEnabled();
        if (enable_encryption && storage_config.s3_config.isS3Enabled())
        {
            LOG_INFO(log, "encryption can be enabled, method is Aes256Ctr");
            // The UniversalPageStorage has not been init yet, the UniversalPageStoragePtr in KeyspacesKeyManager is nullptr.
            KeyManagerPtr key_manager
                = std::make_shared<KeyspacesKeyManager<TiFlashRaftProxyHelper>>(tiflash_instance_wrap.proxy_helper);
            global_context->initializeFileProvider(key_manager, true);
        }
        else if (enable_encryption)
        {
            const auto method = tiflash_instance_wrap.proxy_helper->getEncryptionMethod();
            LOG_INFO(log, "encryption is enabled, method is {}", magic_enum::enum_name(method));
            KeyManagerPtr key_manager = std::make_shared<DataKeyManager>(&tiflash_instance_wrap);
            global_context->initializeFileProvider(key_manager, method != EncryptionMethod::Plaintext);
        }
        else
        {
            LOG_INFO(log, "encryption is disabled");
            KeyManagerPtr key_manager = std::make_shared<DataKeyManager>(&tiflash_instance_wrap);
            global_context->initializeFileProvider(key_manager, false);
        }
    }
    else
    {
        KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(false);
        global_context->initializeFileProvider(key_manager, false);
    }

    /// ===== Paths related configuration initialized start ===== ///
    /// Note that theses global variables should be initialized by the following order:
    // 1. capacity
    // 2. path pool
    // 3. TMTContext

    LOG_INFO(
        log,
        "disaggregated_mode={} use_autoscaler={} enable_s3={}",
        magic_enum::enum_name(global_context->getSharedContextDisagg()->disaggregated_mode),
        global_context->getSharedContextDisagg()->use_autoscaler,
        storage_config.s3_config.isS3Enabled());

    if (storage_config.s3_config.isS3Enabled())
        S3::ClientFactory::instance().init(storage_config.s3_config);

    global_context->getSharedContextDisagg()->initRemoteDataStore(
        global_context->getFileProvider(),
        storage_config.s3_config.isS3Enabled());

    const auto is_compute_mode = global_context->getSharedContextDisagg()->isDisaggregatedComputeMode();
    const auto [remote_cache_paths, remote_cache_capacity_quota]
        = storage_config.remote_cache_config.getCacheDirInfos(is_compute_mode);
    global_context->initializePathCapacityMetric( //
        global_capacity_quota, //
        storage_config.main_data_paths,
        storage_config.main_capacity_quota, //
        storage_config.latest_data_paths,
        storage_config.latest_capacity_quota,
        remote_cache_paths,
        remote_cache_capacity_quota);
    TiFlashRaftConfig raft_config = TiFlashRaftConfig::parseSettings(config(), log);
    global_context->setPathPool( //
        storage_config.main_data_paths, //
        storage_config.latest_data_paths, //
        storage_config.kvstore_data_path, //
        global_context->getPathCapacity(),
        global_context->getFileProvider());
    if (const auto & config = storage_config.remote_cache_config; config.isCacheEnabled() && is_compute_mode)
    {
        config.initCacheDir();
        FileCache::initialize(global_context->getPathCapacity(), config);
    }

    /// Determining PageStorage run mode based on current files on disk and storage config.
    /// Do it as early as possible after loading storage config.
    global_context->initializePageStorageMode(global_context->getPathPool(), STORAGE_FORMAT_CURRENT.page);

    // Use pd address to define which default_database we use by default.
    // For deployed with pd/tidb/tikv use "system", which is always exist in TiFlash.
    std::string default_database = config().getString("default_database", "system");
    Strings all_normal_path = storage_config.getAllNormalPaths();
    const std::string path = all_normal_path[0];
    global_context->setPath(path);

    /// ===== Paths related configuration initialized end ===== ///
    global_context->setSecurityConfig(config(), log);
    Redact::setRedactLog(global_context->getSecurityConfig()->redactInfoLog());

    // Create directories for 'path' and for default database, if not exist.
    for (const String & candidate_path : all_normal_path)
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
        rlimit rlim{};
        if (getrlimit(RLIMIT_NOFILE, &rlim))
            throw Poco::Exception("Cannot getrlimit");

        if (rlim.rlim_cur == rlim.rlim_max)
        {
            LOG_DEBUG(log, "rlimit on number of file descriptors is {}", rlim.rlim_cur);
        }
        else
        {
            rlim_t old = rlim.rlim_cur;
            rlim.rlim_cur = config().getUInt("max_open_files", rlim.rlim_max);
            int rc = setrlimit(RLIMIT_NOFILE, &rlim);
            if (rc != 0)
                LOG_WARNING(
                    log,
                    "Cannot set max number of file descriptors to {}"
                    ". Try to specify max_open_files according to your system limits. error: {}",
                    rlim.rlim_cur,
                    strerror(errno));
            else
                LOG_DEBUG(log, "Set max number of file descriptors to {} (was {}).", rlim.rlim_cur, old);
        }
    }

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Initialize DateLUT early, to not interfere with running time of first query.
    LOG_DEBUG(log, "Initializing DateLUT.");
    DateLUT::instance();
    LOG_TRACE(log, "Initialized DateLUT with time zone `{}`.", DateLUT::instance().getTimeZone());

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
                LOG_DEBUG(log, "Removing old temporary file {}", it->path());
                global_context->getFileProvider()->deleteRegularFile(it->path(), EncryptionPath(it->path(), ""));
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

    if (config().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(config(), "macros"));

    /// Init TiFlash metrics.
    global_context->initializeTiFlashMetrics();

    /// Initialize users config reloader.
    auto users_config_reloader = UserConfig::parseSettings(config(), config_path, global_context, log);

    /// Load global settings from default_profile and system_profile.
    /// It internally depends on UserConfig::parseSettings.
    // TODO: Parse the settings from config file at the program beginning
    global_context->setDefaultProfiles(config());
    LOG_INFO(
        log,
        "Loaded global settings from default_profile and system_profile, changed configs: {{{}}}",
        global_context->getSettingsRef().toString());

    ///
    /// The config value in global settings can only be used from here because we just loaded it from config file.
    ///

    /// Initialize the background & blockable background thread pool.
    Settings & settings = global_context->getSettingsRef();
    LOG_INFO(log, "Background & Blockable Background pool size: {}", settings.background_pool_size);
    auto & bg_pool = global_context->initializeBackgroundPool(settings.background_pool_size);
    auto & blockable_bg_pool = global_context->initializeBlockableBackgroundPool(settings.background_pool_size);
    // adjust the thread pool size according to settings and logical cores num
    adjustThreadPoolSize(settings, server_info.cpu_info.logical_cores);
    initStorageMemoryTracker(
        settings.max_memory_usage_for_all_queries.getActualBytes(server_info.memory_info.capacity),
        settings.bytes_that_rss_larger_than_limit);

    /// PageStorage run mode has been determined above
    global_context->initializeGlobalPageIdAllocator();
    if (!global_context->getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        global_context->initializeGlobalStoragePoolIfNeed(global_context->getPathPool());
        LOG_INFO(
            log,
            "Global PageStorage run mode is {}",
            magic_enum::enum_name(global_context->getPageStorageRunMode()));
    }

    /// Try to restore the StoreIdent from UniPS. There are many services that require
    /// `store_id` to generate the path to RemoteStore under disagg mode.
    std::optional<raft_serverpb::StoreIdent> store_ident;
    // Only when this node is disagg compute node and autoscaler is enabled, we don't need the WriteNodePageStorage instance
    // Disagg compute node without autoscaler still need this instance for proxy's data
    if (!(global_context->getSharedContextDisagg()->isDisaggregatedComputeMode()
          && global_context->getSharedContextDisagg()->use_autoscaler))
    {
        global_context->initializeWriteNodePageStorageIfNeed(global_context->getPathPool());
        if (auto wn_ps = global_context->tryGetWriteNodePageStorage(); wn_ps != nullptr)
        {
            if (tiflash_instance_wrap.proxy_helper->checkEncryptionEnabled() && storage_config.s3_config.isS3Enabled())
            {
                global_context->getFileProvider()->setPageStoragePtrForKeyManager(wn_ps);
            }
            store_ident = tryGetStoreIdent(wn_ps);
            if (!store_ident)
            {
                LOG_INFO(log, "StoreIdent not exist, new tiflash node");
            }
            else
            {
                LOG_INFO(log, "StoreIdent restored, {{{}}}", store_ident->ShortDebugString());
            }
        }
    }

    if (global_context->getSharedContextDisagg()->isDisaggregatedStorageMode())
    {
        global_context->getSharedContextDisagg()->initWriteNodeSnapManager();
        global_context->getSharedContextDisagg()->initFastAddPeerContext(settings.fap_handle_concurrency);
    }

    if (global_context->getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        global_context->getSharedContextDisagg()->initReadNodePageCache(
            global_context->getPathPool(),
            storage_config.remote_cache_config.getPageCacheDir(),
            storage_config.remote_cache_config.getPageCapacity());
    }

    /// Initialize RateLimiter.
    global_context->initializeRateLimiter(config(), bg_pool, blockable_bg_pool);

    global_context->setServerInfo(server_info);
    if (server_info.memory_info.capacity == 0)
    {
        LOG_ERROR(
            log,
            "Failed to get memory capacity, float-pointing memory limit config (for example, set "
            "`max_memory_usage_for_all_queries` to `0.1`) won't take effect. If you set them as float-pointing value, "
            "you can change them to integer instead.");
    }
    else
    {
        LOG_INFO(
            log,
            "Detected memory capacity {} bytes, you have config `max_memory_usage_for_all_queries` to {}, finally "
            "limit to {} bytes.",
            server_info.memory_info.capacity,
            settings.max_memory_usage_for_all_queries.toString(),
            settings.max_memory_usage_for_all_queries.getActualBytes(server_info.memory_info.capacity));
    }

    /// Initialize main config reloader.
    auto main_config_reloader = std::make_unique<ConfigReloader>(
        config_path,
        [&](ConfigurationPtr config) {
            LOG_DEBUG(log, "run main config reloader");
            buildLoggers(*config);
            global_context->setMacros(std::make_unique<Macros>(*config, "macros"));
            global_context->getTMTContext().reloadConfig(*config);
            global_context->getIORateLimiter().updateConfig(*config);
            global_context->reloadDeltaTreeConfig(*config);
            DM::SegmentReadTaskScheduler::instance().updateConfig(global_context->getSettingsRef());
            if (FileCache::instance() != nullptr)
            {
                FileCache::instance()->updateConfig(global_context->getSettingsRef());
            }
            {
                // update TiFlashSecurity and related config in client for ssl certificate reload.
                if (bool updated = global_context->getSecurityConfig()->update(*config); updated)
                {
                    auto raft_config = TiFlashRaftConfig::parseSettings(*config, log);
                    auto cluster_config
                        = getClusterConfig(global_context->getSecurityConfig(), storage_config.api_version, log);
                    global_context->getTMTContext().updateSecurityConfig(
                        std::move(raft_config),
                        std::move(cluster_config));
                    LOG_DEBUG(log, "TMTContext updated security config");
                }
            }
        },
        /* already_loaded = */ true);

    /// Reload config in SYSTEM RELOAD CONFIG query.
    global_context->setConfigReloadCallback([&]() {
        main_config_reloader->reload();

        if (users_config_reloader)
            users_config_reloader->reload();
    });

    /// Limit on total number of concurrently executed queries.
    global_context->getProcessList().setMaxSize(config().getInt("max_concurrent_queries", 0));

    /// Size of cache for marks (index of MergeTree family of tables). It is necessary.
    size_t mark_cache_size = config().getUInt64("mark_cache_size", DEFAULT_MARK_CACHE_SIZE);
    if (mark_cache_size)
        global_context->setMarkCache(mark_cache_size);

    /// Size of cache for minmax index, used by DeltaMerge engine.
    size_t minmax_index_cache_size = config().getUInt64("minmax_index_cache_size", mark_cache_size);
    if (minmax_index_cache_size)
        global_context->setMinMaxIndexCache(minmax_index_cache_size);

    /// The vector index cache by number instead of bytes. Because it use `mmap` and let the operator system decide the memory usage.
    size_t vec_index_cache_entities = config().getUInt64("vec_index_cache_entities", 1000);
    if (vec_index_cache_entities)
        global_context->setVectorIndexCache(vec_index_cache_entities);

    /// Size of max memory usage of DeltaIndex, used by DeltaMerge engine.
    /// - In non-disaggregated mode, its default value is 0, means unlimited, and it
    ///   controls the number of total bytes keep in the memory.
    /// - In disaggregated mode, its default value is memory_capacity_of_host * 0.02.
    ///   0 means cache is disabled.
    ///   We cannot support unlimited delta index cache in disaggregated mode for now,
    ///   because cache items will be never explicitly removed.
    if (global_context->getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        constexpr auto delta_index_cache_ratio = 0.02;
        constexpr auto backup_delta_index_cache_size = 1024 * 1024 * 1024; // 1GiB
        const auto default_delta_index_cache_size = server_info.memory_info.capacity > 0
            ? server_info.memory_info.capacity * delta_index_cache_ratio
            : backup_delta_index_cache_size;
        size_t n = config().getUInt64("delta_index_cache_size", default_delta_index_cache_size);
        LOG_INFO(log, "delta_index_cache_size={}", n);
        // In disaggregated compute node, we will not use DeltaIndexManager to cache the delta index.
        // Instead, we use RNDeltaIndexCache.
        global_context->getSharedContextDisagg()->initReadNodeDeltaIndexCache(n);
    }
    else
    {
        size_t n = config().getUInt64("delta_index_cache_size", 0);
        global_context->setDeltaIndexManager(n);
    }

    /// Set path for format schema files
    auto format_schema_path = Poco::File(config().getString("format_schema_path", path + "format_schemas/"));
    global_context->setFormatSchemaPath(format_schema_path.path() + "/");
    format_schema_path.createDirectories();

    LOG_INFO(log, "Loading metadata.");
    loadMetadataSystem(*global_context); // Load "system" database. Its engine keeps as Ordinary.
    /// After attaching system databases we can initialize system log.
    global_context->initializeSystemLogs();
    /// After the system database is created, attach virtual system tables (in addition to query_log and part_log)
    attachSystemTablesServer(*global_context->getDatabase("system"));

    {
        /// create TMTContext
        auto cluster_config = getClusterConfig(global_context->getSecurityConfig(), storage_config.api_version, log);
        global_context->createTMTContext(raft_config, std::move(cluster_config));
        if (store_ident)
        {
            // Many service would depends on `store_id` when disagg is enabled.
            // setup the store_id restored from store_ident ASAP
            // FIXME: (bootstrap) we should bootstrap the tiflash node more early!
            auto kvstore = global_context->getTMTContext().getKVStore();
            metapb::Store store_meta;
            store_meta.set_id(store_ident->store_id());
            store_meta.set_node_state(metapb::NodeState::Preparing);
            kvstore->setStore(store_meta);
        }
        global_context->getTMTContext().reloadConfig(config());

        // setup the kv cluster for disagg compute node fetching config
        if (S3::ClientFactory::instance().isEnabled())
        {
            auto & tmt = global_context->getTMTContext();
            S3::ClientFactory::instance().setKVCluster(tmt.getKVCluster());
        }
    }
    LOG_INFO(log, "Init S3 GC Manager");
    global_context->getTMTContext().initS3GCManager(tiflash_instance_wrap.proxy_helper);
    // Initialize the thread pool of storage before the storage engine is initialized.
    LOG_INFO(log, "dt_enable_read_thread {}", global_context->getSettingsRef().dt_enable_read_thread);
    // `DMFileReaderPool` should be constructed before and destructed after `SegmentReaderPoolManager`.
    DM::DMFileReaderPool::instance();
    DM::SegmentReaderPoolManager::instance().init(
        server_info.cpu_info.logical_cores,
        settings.dt_read_thread_count_scale);
    DM::SegmentReadTaskScheduler::instance().updateConfig(global_context->getSettingsRef());

    auto schema_cache_size = config().getInt("schema_cache_size", 10000);
    global_context->initializeSharedBlockSchemas(schema_cache_size);

    // Load remaining databases
    loadMetadata(*global_context);
    LOG_DEBUG(log, "Load metadata done.");
    BgStorageInitHolder bg_init_stores;
    if (!global_context->getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        if (global_context->getSharedContextDisagg()->notDisaggregatedMode() || store_ident.has_value())
        {
            // This node has been bootstrapped, the `store_id` is set. Or non-disagg mode,
            // do not depend on `store_id`. Start sync schema before serving any requests.
            // For the node has not been bootstrapped, this stage will be postpone.
            // FIXME: (bootstrap) we should bootstrap the tiflash node more early!
            syncSchemaWithTiDB(storage_config, bg_init_stores, global_context, log);
        }
    }
    // set default database for ch-client
    global_context->setCurrentDatabase(default_database);

    CPUAffinityManager::initCPUAffinityManager(config());
    LOG_INFO(log, "CPUAffinity: {}", CPUAffinityManager::getInstance().toString());
    SCOPE_EXIT({
        /** Ask to cancel background jobs all table engines,
          *  and also query_log.
          * It is important to do early, not in destructor of Context, because
          *  table engines could use Context on destroy.
          */
        LOG_INFO(log, "Shutting down storages.");
        // `SegmentReader` threads may hold a segment and its delta-index for read.
        // `Context::shutdown()` will destroy `DeltaIndexManager`.
        // So, stop threads explicitly before `TiFlashTestEnv::shutdown()`.
        DB::DM::SegmentReaderPoolManager::instance().stop();
        FileCache::shutdown();
        global_context->shutdown();
        if (storage_config.s3_config.isS3Enabled())
        {
            S3::ClientFactory::instance().shutdown();
        }
        LOG_DEBUG(log, "Shutted down storages.");
    });

    {
        if (proxy_conf.is_proxy_runnable && !tiflash_instance_wrap.proxy_helper)
            throw Exception("Raft Proxy Helper is not set, should not happen");
        auto & path_pool = global_context->getPathPool();
        /// initialize TMTContext
        global_context->getTMTContext().restore(path_pool, tiflash_instance_wrap.proxy_helper);
    }

    /// setting up elastic thread pool
    bool enable_elastic_threadpool = settings.enable_elastic_threadpool;
    if (enable_elastic_threadpool)
        DynamicThreadPool::global_instance = std::make_unique<DynamicThreadPool>(
            settings.elastic_threadpool_init_cap,
            std::chrono::milliseconds(settings.elastic_threadpool_shrink_period_ms));
    SCOPE_EXIT({
        if (enable_elastic_threadpool)
        {
            assert(DynamicThreadPool::global_instance);
            DynamicThreadPool::global_instance.reset();
        }
    });

    // FIXME: (bootstrap) we should bootstrap the tiflash node more early!
    if (global_context->getSharedContextDisagg()->notDisaggregatedMode()
        || /*has_been_bootstrap*/ store_ident.has_value())
    {
        // If S3 enabled, wait for all DeltaMergeStores' initialization
        // before this instance can accept requests.
        // Else it just do nothing.
        bg_init_stores.waitUntilFinish();
    }

    if (global_context->getSharedContextDisagg()->isDisaggregatedStorageMode()
        && /*has_been_bootstrap*/ store_ident.has_value())
    {
        // Only disagg write node that has been bootstrap need wait. For the write node does not bootstrap, its
        // store id is allocated later.
        // Wait until all CheckpointInfo are restored from S3
        auto wn_ps = global_context->getWriteNodePageStorage();
        wn_ps->waitUntilInitedFromRemoteStore();
    }

    {
        TcpHttpServersHolder tcp_http_servers_holder(*this, settings, log);

        main_config_reloader->addConfigObject(global_context->getSecurityConfig());
        main_config_reloader->start();
        if (users_config_reloader)
            users_config_reloader->start();

        {
            // on ARM processors it can show only enabled at current moment cores
            CurrentMetrics::set(CurrentMetrics::LogicalCPUCores, server_info.cpu_info.logical_cores);
            CurrentMetrics::set(CurrentMetrics::MemoryCapacity, server_info.memory_info.capacity);
            LOG_INFO(
                log,
                "Available RAM = {}; physical cores = {}; logical cores = {}.",
                server_info.memory_info.capacity,
                server_info.cpu_info.physical_cores,
                server_info.cpu_info.logical_cores);
        }

        LOG_INFO(log, "Ready for connections.");

        SCOPE_EXIT({
            is_cancelled = true;

            tcp_http_servers_holder.onExit();

            main_config_reloader.reset();
            users_config_reloader.reset();
        });

        /// This object will periodically calculate some metrics.
        /// should init after `createTMTContext` cause we collect some data from the TiFlash context object.
        AsynchronousMetrics async_metrics(*global_context);
        attachSystemTablesAsync(*global_context->getDatabase("system"), async_metrics);

        std::vector<std::unique_ptr<MetricsTransmitter>> metrics_transmitters;
        for (const auto & graphite_key : DB::getMultipleKeysFromConfig(config(), "", "graphite"))
        {
            metrics_transmitters.emplace_back(
                std::make_unique<MetricsTransmitter>(*global_context, async_metrics, graphite_key));
        }

        auto metrics_prometheus = std::make_unique<MetricsPrometheus>(*global_context, async_metrics);

        SessionCleaner session_cleaner(*global_context);
        auto & tmt_context = global_context->getTMTContext();

        if (proxy_conf.is_proxy_runnable)
        {
            // If a TiFlash starts before any TiKV starts, then the very first Region will be created in TiFlash's proxy and it must be the peer as a leader role.
            // This conflicts with the assumption that tiflash does not contain any Region leader peer and leads to unexpected errors
            LOG_INFO(log, "Waiting for TiKV cluster to be bootstrapped");
            while (!tmt_context.getPDClient()->isClusterBootstrapped())
            {
                const int wait_seconds = 3;
                LOG_ERROR(
                    log,
                    "Waiting for cluster to be bootstrapped, we will sleep for {} seconds and try again.",
                    wait_seconds);
                ::sleep(wait_seconds);
            }

            tiflash_instance_wrap.tmt = &tmt_context;
            LOG_INFO(log, "Let tiflash proxy start all services");
            // Set tiflash instance status to running, then wait for proxy enter running status
            tiflash_instance_wrap.status = EngineStoreServerStatus::Running;
            while (tiflash_instance_wrap.proxy_helper->getProxyStatus() == RaftProxyStatus::Idle)
                std::this_thread::sleep_for(std::chrono::milliseconds(200));

            // proxy update store-id before status set `RaftProxyStatus::Running`
            assert(tiflash_instance_wrap.proxy_helper->getProxyStatus() == RaftProxyStatus::Running);
            const auto store_id = tmt_context.getKVStore()->getStoreID(std::memory_order_seq_cst);
            if (store_ident)
            {
                RUNTIME_ASSERT(
                    store_id == store_ident->store_id(),
                    log,
                    "store id mismatch store_id={} store_ident.store_id={}",
                    store_id,
                    store_ident->store_id());
            }
            if (global_context->getSharedContextDisagg()->isDisaggregatedComputeMode())
            {
                // compute node do not need to handle read index
                LOG_INFO(log, "store_id={}, tiflash proxy is ready to serve", store_id);
            }
            else
            {
                LOG_INFO(
                    log,
                    "store_id={}, tiflash proxy is ready to serve, try to wake up all regions' leader",
                    store_id);

                if (global_context->getSharedContextDisagg()->isDisaggregatedStorageMode() && !store_ident.has_value())
                {
                    // Not disagg node done it before
                    // For the disagg node has not been bootstrap, begin the very first schema sync with TiDB.
                    // FIXME: (bootstrap) we should bootstrap the tiflash node more early!
                    syncSchemaWithTiDB(storage_config, bg_init_stores, global_context, log);
                    bg_init_stores.waitUntilFinish();
                }

                // if set 0, DO NOT enable read-index worker
                size_t runner_cnt = config().getUInt("flash.read_index_runner_count", 1);
                if (runner_cnt > 0)
                {
                    auto & kvstore_ptr = tmt_context.getKVStore();
                    kvstore_ptr->initReadIndexWorkers(
                        [&]() {
                            // get from tmt context
                            return std::chrono::milliseconds(tmt_context.readIndexWorkerTick());
                        },
                        /*running thread count*/ runner_cnt);
                    tmt_context.getKVStore()->asyncRunReadIndexWorkers();
                    WaitCheckRegionReady(tmt_context, *kvstore_ptr, terminate_signals_counter);
                }
            }
        }
        SCOPE_EXIT({
            if (!proxy_conf.is_proxy_runnable)
            {
                tmt_context.setStatusTerminated();
                return;
            }
            if (proxy_conf.is_proxy_runnable && tiflash_instance_wrap.status != EngineStoreServerStatus::Running)
            {
                LOG_ERROR(log, "Current status of engine-store is NOT Running, should not happen");
                exit(-1);
            }
            LOG_INFO(log, "Set store context status Stopping");
            tmt_context.setStatusStopping();
            {
                // Wait until there is no read-index task.
                while (tmt_context.getKVStore()->getReadIndexEvent())
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
            tmt_context.setStatusTerminated();
            tmt_context.getKVStore()->stopReadIndexWorkers();
            LOG_INFO(log, "Set store context status Terminated");
            {
                // update status and let proxy stop all services except encryption.
                tiflash_instance_wrap.status = EngineStoreServerStatus::Stopping;
                LOG_INFO(log, "Set engine store server status Stopping");
            }
            // wait proxy to stop services
            if (proxy_conf.is_proxy_runnable)
            {
                LOG_INFO(log, "Let tiflash proxy to stop all services");
                while (tiflash_instance_wrap.proxy_helper->getProxyStatus() != RaftProxyStatus::Stopped)
                    std::this_thread::sleep_for(std::chrono::milliseconds(200));
                LOG_INFO(log, "All services in tiflash proxy are stopped");
            }
        });

        {
            // Report the unix timestamp, git hash, release version
            Poco::Timestamp ts;
            GET_METRIC(tiflash_server_info, start_time).Set(ts.epochTime());
        }

        // For test mode, TaskScheduler and LAC is controlled by test case.
        // TODO: resource control is not supported for WN. So disable pipeline model and LAC.
        const bool init_pipeline_and_lac
            = !global_context->isTest() && !global_context->getSharedContextDisagg()->isDisaggregatedStorageMode();
        if (init_pipeline_and_lac)
        {
#ifdef DBMS_PUBLIC_GTEST
            LocalAdmissionController::global_instance = std::make_unique<MockLocalAdmissionController>();
#else
            LocalAdmissionController::global_instance
                = std::make_unique<LocalAdmissionController>(tmt_context.getKVCluster(), tmt_context.getEtcdClient());
#endif

            auto get_pool_size = [](const auto & setting) {
                return setting == 0 ? getNumberOfLogicalCPUCores() : static_cast<size_t>(setting);
            };
            TaskSchedulerConfig config{
                {get_pool_size(settings.pipeline_cpu_task_thread_pool_size),
                 settings.pipeline_cpu_task_thread_pool_queue_type},
                {get_pool_size(settings.pipeline_io_task_thread_pool_size),
                 settings.pipeline_io_task_thread_pool_queue_type},
            };
            RUNTIME_CHECK(!TaskScheduler::instance);
            TaskScheduler::instance = std::make_unique<TaskScheduler>(config);
            LOG_INFO(log, "init pipeline task scheduler with {}", config.toString());
        }

        SCOPE_EXIT({
            if (init_pipeline_and_lac)
            {
                assert(TaskScheduler::instance);
                TaskScheduler::instance.reset();
                // Stop LAC instead of reset, because storage layer still needs it.
                // Workload will not be throttled when LAC is stopped.
                // It's ok because flash service has already been destructed, so throllting is meaningless.
                assert(LocalAdmissionController::global_instance);
                LocalAdmissionController::global_instance->safeStop();
            }
        });

        if (settings.enable_async_grpc_client)
        {
            auto size = settings.grpc_completion_queue_pool_size;
            if (size == 0)
                size = std::thread::hardware_concurrency();
            GRPCCompletionQueuePool::global_instance = std::make_unique<GRPCCompletionQueuePool>(size);
        }

        /// startup grpc server to serve raft and/or flash services.
        FlashGrpcServerHolder flash_grpc_server_holder(this->context(), this->config(), raft_config, log);

        SCOPE_EXIT({
            // Stop LAC for AutoScaler managed CN before FlashGrpcServerHolder is destructed.
            // Because AutoScaler it will kill tiflash process when port of flash_server_addr is down.
            // And we want to make sure LAC is cleanedup.
            // The effects are there will be no resource control during [lac.safeStop(), FlashGrpcServer destruct done],
            // but it's basically ok, that duration is small(normally 100-200ms).
            if (global_context->getSharedContextDisagg()->isDisaggregatedComputeMode() && use_autoscaler
                && LocalAdmissionController::global_instance)
                LocalAdmissionController::global_instance->safeStop();
        });

        tmt_context.setStatusRunning();

        try
        {
            // Bind CPU affinity after all threads started.
            CPUAffinityManager::getInstance().bindThreadCPUAffinity();
        }
        catch (...)
        {
            LOG_ERROR(log, "CPUAffinityManager::bindThreadCPUAffinity throws exception.");
        }

        LOG_INFO(log, "Start to wait for terminal signal");
        waitForTerminationRequest();

        {
            // Set limiters stopping and wakeup threads in waitting queue.
            global_context->getIORateLimiter().setStop();
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
