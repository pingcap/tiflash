// Copyright 2023 PingCAP, Ltd.
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
#include <Common/ClickHouseRevision.h>
#include <Common/ComputeLabelHolder.h>
#include <Common/Config/ConfigReloader.h>
#include <Common/CurrentMetrics.h>
#include <Common/DynamicThreadPool.h>
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
#include <Encryption/DataKeyManager.h>
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>
#include <Encryption/RateLimiter.h>
#include <Flash/DiagnosticsService.h>
#include <Flash/FlashService.h>
#include <Flash/Mpp/GRPCCompletionQueuePool.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Functions/registerFunctions.h>
#include <IO/HTTPCommon.h>
#include <IO/IOThreadPool.h>
#include <IO/ReadHelpers.h>
#include <IO/createReadBufferFromFileBase.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Interpreters/loadMetadata.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Timestamp.h>
#include <Server/CertificateReloader.h>
#include <Server/HTTPHandlerFactory.h>
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
#include <Storages/FormatVersion.h>
#include <Storages/IManageableStorage.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Common.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/Transaction/FileEncryption.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/registerStorages.h>
#include <TableFunctions/registerTableFunctions.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <WindowFunctions/registerWindowFunctions.h>
#include <boost_wrapper/string_split.h>
#include <common/ErrorHandlers.h>
#include <common/config_common.h>
#include <common/logger_useful.h>
#include <sys/resource.h>

#include <boost/algorithm/string/classification.hpp>
#include <ext/scope_guard.h>
#include <limits>
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
        {                                            \
        }                                            \
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
        {
        }
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

std::string Server::getDefaultCorePath() const
{
    return getCanonicalPath(config().getString("path")) + "cores";
}

struct TiFlashProxyConfig
{
    static const std::string config_prefix;
    std::vector<const char *> args;
    std::unordered_map<std::string, std::string> val_map;
    bool is_proxy_runnable = false;

    // TiFlash Proxy will set the default value of "flash.proxy.addr", so we don't need to set here.
    const String engine_store_version = "engine-version";
    const String engine_store_git_hash = "engine-git-hash";
    const String engine_store_address = "engine-addr";
    const String engine_store_advertise_address = "advertise-engine-addr";
    const String pd_endpoints = "pd-endpoints";
    const String engine_label = "engine-label";
    const String engine_role_label = "engine-role-label";

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
        // todo: remove after AutoScaler is stable.
        if (disaggregated_mode == DisaggregatedMode::Compute && useAutoScaler(config))
            return;

        if (!config.has(config_prefix))
            return;

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_prefix, keys);
        {
            std::unordered_map<std::string, std::string> args_map;
            for (const auto & key : keys)
            {
                const auto k = config_prefix + "." + key;
                args_map[key] = config.getString(k);
            }
            args_map[pd_endpoints] = config.getString("raft.pd_addr");
            args_map[engine_store_version] = TiFlashBuildInfo::getReleaseVersion();
            args_map[engine_store_git_hash] = TiFlashBuildInfo::getGitHash();
            if (!args_map.count(engine_store_address))
                args_map[engine_store_address] = config.getString("flash.service_addr");
            else
                args_map[engine_store_advertise_address] = args_map[engine_store_address];

            args_map[engine_label] = getProxyLabelByDisaggregatedMode(disaggregated_mode);
            if (disaggregated_mode != DisaggregatedMode::Compute && has_s3_config)
            {
                args_map[engine_role_label] = DISAGGREGATED_MODE_WRITE_ENGINE_ROLE;
            }

            for (auto && [k, v] : args_map)
            {
                val_map.emplace("--" + k, std::move(v));
            }
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

const std::string TiFlashProxyConfig::config_prefix = "flash.proxy";

pingcap::ClusterConfig getClusterConfig(TiFlashSecurityConfigPtr security_config, const TiFlashRaftConfig & raft_config, const int api_version, const LoggerPtr & log)
{
    pingcap::ClusterConfig config;
    config.tiflash_engine_key = raft_config.engine_key;
    config.tiflash_engine_value = raft_config.engine_value;
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
    LOG_INFO(log, "update cluster config, ca_path: {}, cert_path: {}, key_path: {}, api_version: {}", ca_path, cert_path, key_path, config.api_version);
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

struct HTTPServer : Poco::Net::HTTPServer
{
    HTTPServer(Poco::Net::HTTPRequestHandlerFactory::Ptr pFactory, Poco::ThreadPool & threadPool, const Poco::Net::ServerSocket & socket, Poco::Net::HTTPServerParams::Ptr pParams)
        : Poco::Net::HTTPServer(pFactory, threadPool, socket, pParams)
    {}

protected:
    void run() override
    {
        setThreadName("HTTPServer");
        Poco::Net::HTTPServer::run();
    }
};

struct TCPServer : Poco::Net::TCPServer
{
    TCPServer(Poco::Net::TCPServerConnectionFactory::Ptr pFactory, Poco::ThreadPool & threadPool, const Poco::Net::ServerSocket & socket, Poco::Net::TCPServerParams::Ptr pParams)
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

std::unique_ptr<std::thread> initStores(Context & global_context, const LoggerPtr & log, bool lazily_init_store, EngineStoreServerWrap * tiflash_instance_wrap)
{
    // If `tiflash_instance_wrap` is not nullptr, S3 is enabled.
    // We must wait for tiflash-proxy's initializtion finished before initialzing DeltaMergeStores,
    // because we need store_id to scan dmfiles from S3.
    RUNTIME_CHECK_MSG(lazily_init_store || tiflash_instance_wrap == nullptr, "When S3 enabled, lazily_init_store must be true.");
    auto wait_proxy = [](EngineStoreServerWrap * tiflash_instance_wrap) {
        while (tiflash_instance_wrap->proxy_helper->getProxyStatus() == RaftProxyStatus::Idle)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        auto status = tiflash_instance_wrap->proxy_helper->getProxyStatus();
        RUNTIME_CHECK_MSG(status == RaftProxyStatus::Running, "RaftProxyStatus::{}", magic_enum::enum_name(status));
    };

    auto do_init_stores = [&global_context, &log, tiflash_instance_wrap, wait_proxy]() {
        if (tiflash_instance_wrap != nullptr)
        {
            wait_proxy(tiflash_instance_wrap);
        }
        auto storages = global_context.getTMTContext().getStorages().getAllStorage();
        int init_cnt = 0;
        int err_cnt = 0;
        for (auto & [table_id, storage] : storages)
        {
            // This will skip the init of storages that do not contain any data. TiFlash now sync the schema and
            // create all tables regardless the table have define TiFlash replica or not, so there may be lots
            // of empty tables in TiFlash.
            // Note that we still need to init stores that contains data (defined by the stable dir of this storage
            // is exist), or the data used size reported to PD is not correct.
            try
            {
                init_cnt += storage->initStoreIfDataDirExist() ? 1 : 0;
                LOG_INFO(log, "Storage inited done [table_id={}]", table_id);
            }
            catch (...)
            {
                err_cnt++;
                tryLogCurrentException(log, fmt::format("Storage inited fail, [table_id={}]", table_id));
            }
        }
        LOG_INFO(
            log,
            "Storage inited finish. [total_count={}] [init_count={}] [error_count={}] [datatype_fullname_count={}]",
            storages.size(),
            init_cnt,
            err_cnt,
            DataTypeFactory::instance().getFullNameCacheSize());
    };
    if (lazily_init_store)
    {
        LOG_INFO(log, "Lazily init store.");
        // apply the inited in another thread to shorten the start time of TiFlash
        return std::make_unique<std::thread>(do_init_stores);
    }
    else
    {
        LOG_INFO(log, "Not lazily init store.");
        do_init_stores();
        return nullptr;
    }
}

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
            socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ config.getBool("listen_reuse_port", false));
#endif

            socket.listen(/* backlog = */ config.getUInt("listen_backlog", 64));

            return address;
        };

        for (const auto & listen_host : listen_hosts)
        {
            /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
            try
            {
                /// HTTPS
                if (config.has("https_port"))
                {
#if Poco_NetSSL_FOUND
                    if (!security_config->hasTlsConfig())
                    {
                        LOG_ERROR(log, "https_port is set but tls config is not set");
                    }
                    auto [ca_path, cert_path, key_path] = security_config->getPaths();
                    Poco::Net::Context::Ptr context = new Poco::Net::Context(Poco::Net::Context::TLSV1_2_SERVER_USE,
                                                                             key_path,
                                                                             cert_path,
                                                                             ca_path,
                                                                             Poco::Net::Context::VerificationMode::VERIFY_STRICT);
                    auto check_common_name = [&](const Poco::Crypto::X509Certificate & cert) {
                        return server.global_context->getSecurityConfig()->checkCommonName(cert);
                    };
                    context->setAdhocVerification(check_common_name);
                    std::call_once(ssl_init_once, SSLInit);

                    Poco::Net::SecureServerSocket socket(context);
                    CertificateReloader::initSSLCallback(context, server.global_context.get());
                    auto address = socket_bind_listen(socket, listen_host, config.getInt("https_port"), /* secure = */ true);
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(
                        new HTTPServer(new HTTPHandlerFactory(server, "HTTPSHandler-factory"), server_pool, socket, http_params));

                    LOG_INFO(log, "Listening https://{}", address.toString());
#else
                    throw Exception{"HTTPS protocol is disabled because Poco library was built without NetSSL support.",
                                    ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                }
                else
                {
                    /// HTTP
                    if (security_config->hasTlsConfig())
                    {
                        throw Exception("tls config is set but https_port is not set ", ErrorCodes::INVALID_CONFIG_PARAMETER);
                    }
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config.getInt("http_port", DEFAULT_HTTP_PORT));
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(
                        new HTTPServer(new HTTPHandlerFactory(server, "HTTPHandler-factory"), server_pool, socket, http_params));

                    LOG_INFO(log, "Listening http://{}", address.toString());
                }


                /// TCP
                if (config.has("tcp_port"))
                {
                    if (security_config->hasTlsConfig())
                    {
                        LOG_ERROR(log, "tls config is set but tcp_port_secure is not set.");
                    }
                    std::call_once(ssl_init_once, SSLInit);
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config.getInt("tcp_port"));
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);
                    servers.emplace_back(new TCPServer(new TCPHandlerFactory(server), server_pool, socket, new Poco::Net::TCPServerParams));

                    LOG_INFO(log, "Listening tcp: {}", address.toString());
                }
                else if (security_config->hasTlsConfig())
                {
                    LOG_INFO(log, "tcp_port is closed because tls config is set");
                }

                /// TCP with SSL
                if (config.has("tcp_port_secure") && !security_config->hasTlsConfig())
                {
#if Poco_NetSSL_FOUND
                    auto [ca_path, cert_path, key_path] = security_config->getPaths();
                    Poco::Net::Context::Ptr context = new Poco::Net::Context(Poco::Net::Context::TLSV1_2_SERVER_USE,
                                                                             key_path,
                                                                             cert_path,
                                                                             ca_path);
                    CertificateReloader::initSSLCallback(context, server.global_context.get());
                    Poco::Net::SecureServerSocket socket(context);
                    auto address = socket_bind_listen(socket, listen_host, config.getInt("tcp_port_secure"), /* secure = */ true);
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);
                    servers.emplace_back(new TCPServer(
                        new TCPHandlerFactory(server, /* secure= */ true),
                        server_pool,
                        socket,
                        new Poco::Net::TCPServerParams));
                    LOG_INFO(log, "Listening tcp_secure: {}", address.toString());
#else
                    throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                                    ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                }
                else if (security_config->hasTlsConfig())
                {
                    LOG_INFO(log, "tcp_port_secure is closed because tls config is set");
                }

                /// At least one of TCP and HTTP servers must be created.
                if (servers.empty())
                    throw Exception("No 'tcp_port' and 'http_port' is specified in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);
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

        if (servers.empty())
            throw Exception("No servers started (add valid listen_host and 'tcp_port' or 'http_port' to configuration file.)",
                            ErrorCodes::NO_ELEMENTS_IN_CONFIG);

        for (auto & server : servers)
            server->start();
    }

    void onExit()
    {
        auto & config = server.config();

        LOG_DEBUG(log, "Received termination signal.");
        LOG_DEBUG(log, "Waiting for current connections to close.");

        int current_connections = 0;
        for (auto & server : servers)
        {
            server->stop();
            current_connections += server->currentConnections();
        }
        String debug_msg = "Closed all listening sockets.";

        if (current_connections)
            LOG_DEBUG(
                log,
                "{} Waiting for {} outstanding connections.",
                debug_msg,
                current_connections);
        else
            LOG_DEBUG(log, debug_msg);

        if (current_connections)
        {
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

        debug_msg = "Closed connections.";

        if (current_connections)
            LOG_DEBUG(
                log,
                "{} But {} remains."
                " Tip: To increase wait time add to config: <shutdown_wait_unfinished>60</shutdown_wait_unfinished>",
                debug_msg,
                current_connections);
        else
            LOG_DEBUG(log, debug_msg);
    }

private:
    Server & server;
    const LoggerPtr & log;
    Poco::ThreadPool server_pool;
    std::vector<std::unique_ptr<Poco::Net::TCPServer>> servers;
};

// By default init global thread pool by hardware_concurrency
// Later we will adjust it by `adjustThreadPoolSize`
void initThreadPool()
{
    size_t default_num_threads = std::max(4UL, 2 * std::thread::hardware_concurrency());
    GlobalThreadPool::initialize(
        /*max_threads*/ default_num_threads,
        /*max_free_threads*/ default_num_threads / 2,
        /*queue_size*/ default_num_threads * 2);
    IOThreadPool::initialize(
        /*max_threads*/ default_num_threads,
        /*max_free_threads*/ default_num_threads / 2,
        /*queue_size*/ default_num_threads * 2);
}

void adjustThreadPoolSize(const Settings & settings, size_t logical_cores)
{
    // TODO: make BackgroundPool/BlockableBackgroundPool/DynamicThreadPool spawned from `GlobalThreadPool`
    size_t max_io_thread_count = std::ceil(settings.io_thread_count_scale * logical_cores);
    // Currently, `GlobalThreadPool` is only used by `IOThreadPool`, so they have the same number of threads.

    GlobalThreadPool::instance().setMaxThreads(max_io_thread_count);
    GlobalThreadPool::instance().setMaxFreeThreads(max_io_thread_count / 2);
    GlobalThreadPool::instance().setQueueSize(max_io_thread_count * 2);

    IOThreadPool::instance->setMaxFreeThreads(max_io_thread_count);
    IOThreadPool::instance->setMaxFreeThreads(max_io_thread_count / 2);
    IOThreadPool::instance->setQueueSize(max_io_thread_count * 2);
}

int Server::main(const std::vector<std::string> & /*args*/)
{
    setThreadName("TiFlashMain");

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
    initThreadPool();

    TiFlashErrorRegistry::instance(); // This invocation is for initializing

    // Some Storage's config is necessary for Proxy
    TiFlashStorageConfig storage_config;
    // Deprecated settings.
    // `global_capacity_quota` will be ignored if `storage_config.main_capacity_quota` is not empty.
    // "0" by default, means no quota, the actual disk capacity is used.
    size_t global_capacity_quota = 0;
    std::tie(global_capacity_quota, storage_config) = TiFlashStorageConfig::parseSettings(config(), log);
    if (storage_config.format_version)
    {
        setStorageFormat(storage_config.format_version);
        LOG_INFO(log, "Using format_version={} (explicit storage format detected).", storage_config.format_version);
    }
    else
    {
        LOG_INFO(log, "Using format_version={} (default settings).", STORAGE_FORMAT_CURRENT.identifier);
    }

    LOG_INFO(log, "Using api_version={}", storage_config.api_version);

    // Init Proxy's config
    TiFlashProxyConfig proxy_conf(config(), storage_config.s3_config.isS3Enabled());
    EngineStoreServerWrap tiflash_instance_wrap{};
    auto helper = GetEngineStoreServerHelper(
        &tiflash_instance_wrap);

    if (STORAGE_FORMAT_CURRENT.page == PageFormat::V4)
    {
        LOG_INFO(log, "Using UniPS for proxy");
        proxy_conf.addExtraArgs("unips-enabled", "1");
    }
    else
    {
        LOG_INFO(log, "UniPS is not enabled for proxy, page_version={}", STORAGE_FORMAT_CURRENT.page);
    }

    RaftStoreProxyRunner proxy_runner(RaftStoreProxyRunner::RunRaftStoreProxyParms{&helper, proxy_conf}, log);

    if (proxy_conf.is_proxy_runnable)
    {
        proxy_runner.run();

        LOG_INFO(log, "wait for tiflash proxy initializing");
        while (!tiflash_instance_wrap.proxy_helper)
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        LOG_INFO(log, "tiflash proxy is initialized");
        if (tiflash_instance_wrap.proxy_helper->checkEncryptionEnabled())
        {
            auto method = tiflash_instance_wrap.proxy_helper->getEncryptionMethod();
            LOG_INFO(log, "encryption is enabled, method is {}", IntoEncryptionMethodName(method));
        }
        else
            LOG_INFO(log, "encryption is disabled");
    }

    SCOPE_EXIT({
        if (!proxy_conf.is_proxy_runnable)
        {
            proxy_runner.join();
            return;
        }
        LOG_INFO(log, "Let tiflash proxy shutdown");
        tiflash_instance_wrap.status = EngineStoreServerStatus::Terminated;
        tiflash_instance_wrap.tmt = nullptr;
        LOG_INFO(log, "Wait for tiflash proxy thread to join");
        proxy_runner.join();
        LOG_INFO(log, "tiflash proxy thread is joined");
    });

    /// get CPU/memory/disk info of this server
    if (tiflash_instance_wrap.proxy_helper)
    {
        diagnosticspb::ServerInfoRequest request;
        request.set_tp(static_cast<diagnosticspb::ServerInfoType>(1));
        diagnosticspb::ServerInfoResponse response;
        std::string req = request.SerializeAsString();
        auto * helper = tiflash_instance_wrap.proxy_helper;
        helper->fn_server_info(helper->proxy_ptr, strIntoView(&req), &response);
        server_info.parseSysInfo(response);
        setNumberOfLogicalCPUCores(server_info.cpu_info.logical_cores);
        computeAndSetNumberOfPhysicalCPUCores(server_info.cpu_info.logical_cores, server_info.cpu_info.physical_cores);
        LOG_INFO(log, "ServerInfo: {}", server_info.debugString());
    }
    else
    {
        setNumberOfLogicalCPUCores(std::thread::hardware_concurrency());
        computeAndSetNumberOfPhysicalCPUCores(std::thread::hardware_concurrency(), std::thread::hardware_concurrency() / 2);
        LOG_INFO(log, "TiFlashRaftProxyHelper is null, failed to get server info");
    }

    grpc_log = Logger::get("grpc");
    gpr_set_log_verbosity(GPR_LOG_SEVERITY_DEBUG);
    gpr_set_log_function(&printGRPCLog);

    /** Context contains all that query execution is dependent:
      *  settings, available functions, data types, aggregate functions, databases...
      */
    global_context = Context::createGlobal();
    global_context->setApplicationType(Context::ApplicationType::SERVER);
    global_context->getSharedContextDisagg()->disaggregated_mode = getDisaggregatedMode(config());
    global_context->getSharedContextDisagg()->use_autoscaler = useAutoScaler(config());

    /// Init File Provider
    bool enable_encryption = false;
    if (proxy_conf.is_proxy_runnable)
    {
        enable_encryption = tiflash_instance_wrap.proxy_helper->checkEncryptionEnabled();
        if (enable_encryption)
        {
            auto method = tiflash_instance_wrap.proxy_helper->getEncryptionMethod();
            enable_encryption = (method != EncryptionMethod::Plaintext);
        }
        KeyManagerPtr key_manager = std::make_shared<DataKeyManager>(&tiflash_instance_wrap);
        global_context->initializeFileProvider(key_manager, enable_encryption);
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
    {
        if (enable_encryption)
        {
            LOG_ERROR(log, "Cannot support S3 when encryption enabled.");
            throw Exception("Cannot support S3 when encryption enabled.");
        }
        S3::ClientFactory::instance().init(storage_config.s3_config);
    }
    if (const auto & config = storage_config.remote_cache_config; config.isCacheEnabled())
    {
        config.initCacheDir();
        FileCache::initialize(config.getDTFileCacheDir(), config.getDTFileCapacity(), config.dtfile_level, config.dtfile_cache_min_age_seconds);
    }
    global_context->getSharedContextDisagg()->initRemoteDataStore(global_context->getFileProvider(), storage_config.s3_config.isS3Enabled());

    global_context->initializePathCapacityMetric( //
        global_capacity_quota, //
        storage_config.main_data_paths,
        storage_config.main_capacity_quota, //
        storage_config.latest_data_paths,
        storage_config.latest_capacity_quota,
        global_context->getSharedContextDisagg()->isDisaggregatedComputeMode() ? storage_config.remote_cache_config.dir : "",
        global_context->getSharedContextDisagg()->isDisaggregatedComputeMode() ? storage_config.remote_cache_config.capacity : 0);
    TiFlashRaftConfig raft_config = TiFlashRaftConfig::parseSettings(config(), log);
    global_context->setPathPool( //
        storage_config.main_data_paths, //
        storage_config.latest_data_paths, //
        storage_config.kvstore_data_path, //
        global_context->getPathCapacity(),
        global_context->getFileProvider());

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

    /// Initialize the labels of tiflash compute node.
    ComputeLabelHolder::instance().init(config());

    /// Init TiFlash metrics.
    global_context->initializeTiFlashMetrics();

    /// Initialize users config reloader.
    auto users_config_reloader = UserConfig::parseSettings(config(), config_path, global_context, log);

    /// Load global settings from default_profile and system_profile.
    /// It internally depends on UserConfig::parseSettings.
    // TODO: Parse the settings from config file at the program beginning
    global_context->setDefaultProfiles(config());
    LOG_INFO(log, "Loaded global settings from default_profile and system_profile.");

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

    /// PageStorage run mode has been determined above
    if (!global_context->getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        global_context->initializeGlobalStoragePoolIfNeed(global_context->getPathPool());
        LOG_INFO(log, "Global PageStorage run mode is {}", magic_enum::enum_name(global_context->getPageStorageRunMode()));
    }

    if (global_context->getSharedContextDisagg()->isDisaggregatedStorageMode()
        || global_context->getSharedContextDisagg()->notDisaggregatedMode())
    {
        global_context->initializeWriteNodePageStorageIfNeed(global_context->getPathPool());
    }

    if (global_context->getSharedContextDisagg()->isDisaggregatedStorageMode())
    {
        global_context->getSharedContextDisagg()->initWriteNodeSnapManager();
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
        LOG_ERROR(log, "Failed to get memory capacity, float-pointing memory limit config (for example, set `max_memory_usage_for_all_queries` to `0.1`) won't take effect. If you set them as float-pointing value, you can change them to integer instead.");
    }
    else
    {
        LOG_INFO(log, fmt::format("Detected memory capacity {} bytes, you have config `max_memory_usage_for_all_queries` to {}, finally limit to {} bytes.", server_info.memory_info.capacity, settings.max_memory_usage_for_all_queries.toString(), settings.max_memory_usage_for_all_queries.getActualBytes(server_info.memory_info.capacity)));
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
            if (FileCache::instance() != nullptr)
            {
                FileCache::instance()->updateConfig(*config);
            }

            {
                // update TiFlashSecurity and related config in client for ssl certificate reload.
                bool updated = global_context->getSecurityConfig()->update(*config); // Whether the cert path or file is updated.
                if (updated)
                {
                    auto raft_config = TiFlashRaftConfig::parseSettings(*config, log);
                    auto cluster_config = getClusterConfig(global_context->getSecurityConfig(), raft_config, storage_config.api_version, log);
                    global_context->getTMTContext().updateSecurityConfig(std::move(raft_config), std::move(cluster_config));
                    LOG_DEBUG(log, "TMTContext updated security config");
                }
            }
        },
        /* already_loaded = */ true);

    /// Reload config in SYSTEM RELOAD CONFIG query.
    global_context->setConfigReloadCallback([&]() {
        main_config_reloader->reload();
        users_config_reloader->reload();
    });

    /// Limit on total number of concurrently executed queries.
    global_context->getProcessList().setMaxSize(config().getInt("max_concurrent_queries", 0));

    /// Size of cache for uncompressed blocks. Zero means disabled.
    size_t uncompressed_cache_size = config().getUInt64("uncompressed_cache_size", 0);
    if (uncompressed_cache_size)
        global_context->setUncompressedCache(uncompressed_cache_size);

    /// Size of cache for marks (index of MergeTree family of tables). It is necessary.
    size_t mark_cache_size = config().getUInt64("mark_cache_size", DEFAULT_MARK_CACHE_SIZE);
    if (mark_cache_size)
        global_context->setMarkCache(mark_cache_size);

    /// Size of cache for minmax index, used by DeltaMerge engine.
    size_t minmax_index_cache_size = config().getUInt64("minmax_index_cache_size", mark_cache_size);
    if (minmax_index_cache_size)
        global_context->setMinMaxIndexCache(minmax_index_cache_size);

    /// Size of max memory usage of DeltaIndex, used by DeltaMerge engine.
    /// This setting is currently a bit tricky:
    /// - In non-disaggregated mode, its default value is 0, means unlimited, and it
    //    controls the number of total bytes keep in the memory.
    /// - In disaggregated mode, its default value is 0. 0 means cache is disabled, and it
    ///   controls the **number** of trees keep in the memory. <-- Will be fixed.
    ///   We cannot support unlimited delta index cache in disaggregated mode for now,
    ///   because cache items will be never explicitly removed.
    size_t delta_index_cache_size = config().getUInt64("delta_index_cache_size", 0);
    if (global_context->getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        // In disaggregate compute node, we will not use DeltaIndexManager to cache the delta index.
        // Instead, we use RNDeltaIndexCache.

        // TODO: Currently RNDeltaIndexCache caches by number of entities, instead of
        // number of bytes!

        if (delta_index_cache_size > 100000)
            delta_index_cache_size = 100000; // In case of someone incorrectly uses byte size.

        global_context->getSharedContextDisagg()->initReadNodeDeltaIndexCache(delta_index_cache_size);
    }
    else
    {
        global_context->setDeltaIndexManager(delta_index_cache_size);
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
        auto cluster_config = getClusterConfig(global_context->getSecurityConfig(), raft_config, storage_config.api_version, log);
        global_context->createTMTContext(raft_config, std::move(cluster_config));
        global_context->getTMTContext().reloadConfig(config());
    }

    // Initialize the thread pool of storage before the storage engine is initialized.
    LOG_INFO(log, "dt_enable_read_thread {}", global_context->getSettingsRef().dt_enable_read_thread);
    // `DMFileReaderPool` should be constructed before and destructed after `SegmentReaderPoolManager`.
    DM::DMFileReaderPool::instance();
    DM::SegmentReaderPoolManager::instance().init(server_info.cpu_info.logical_cores, settings.dt_read_thread_count_scale);
    DM::SegmentReadTaskScheduler::instance();

    auto schema_cache_size = config().getInt("schema_cache_size", 10000);
    global_context->initializeSharedBlockSchemas(schema_cache_size);

    // Load remaining databases
    loadMetadata(*global_context);
    LOG_DEBUG(log, "Load metadata done.");
    std::unique_ptr<std::thread> init_stores_thread;
    if (!global_context->getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        /// Then, sync schemas with TiDB, and initialize schema sync service.
        /// If in API V2 mode, each keyspace's schema is fetch lazily.
        if (storage_config.api_version == 1)
        {
            for (int i = 0; i < 60; i++) // retry for 3 mins
            {
                try
                {
                    global_context->getTMTContext().getSchemaSyncer()->syncSchemas(*global_context, NullspaceID);
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

        init_stores_thread = initStores(*global_context, log, storage_config.lazily_init_store, storage_config.s3_config.isS3Enabled() ? &tiflash_instance_wrap : nullptr);

        // After schema synced, set current database.
        global_context->setCurrentDatabase(default_database);

        global_context->initializeSchemaSyncService();
    }
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
        if (storage_config.s3_config.isS3Enabled())
        {
            S3::ClientFactory::instance().shutdown();
        }
        global_context->shutdown();
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

    // For test mode, TaskScheduler is controlled by test case.
    bool enable_pipeline = settings.enable_pipeline && !global_context->isTest();
    if (enable_pipeline)
    {
        auto get_pool_size = [](const auto & setting) {
            return setting == 0 ? getNumberOfLogicalCPUCores() : static_cast<size_t>(setting);
        };
        TaskSchedulerConfig config{get_pool_size(settings.pipeline_task_thread_pool_size)};
        assert(!TaskScheduler::instance);
        TaskScheduler::instance = std::make_unique<TaskScheduler>(config);
    }
    SCOPE_EXIT({
        if (enable_pipeline)
        {
            assert(TaskScheduler::instance);
            TaskScheduler::instance.reset();
        }
    });

    if (settings.enable_async_grpc_client)
    {
        auto size = settings.grpc_completion_queue_pool_size;
        if (size == 0)
            size = std::thread::hardware_concurrency();
        GRPCCompletionQueuePool::global_instance = std::make_unique<GRPCCompletionQueuePool>(size);
    }

    if (init_stores_thread != nullptr)
    {
        if (storage_config.s3_config.isS3Enabled())
        {
            // If S3 enabled, wait for all DeltaMergeStores' initialization
            // before this instance can accept requests.
            init_stores_thread->join();
        }
        else
        {
            init_stores_thread->detach();
        }
    }
    /// Then, startup grpc server to serve raft and/or flash services.
    FlashGrpcServerHolder flash_grpc_server_holder(this->context(), this->config(), raft_config, log);

    {
        TcpHttpServersHolder tcpHttpServersHolder(*this, settings, log);

        main_config_reloader->addConfigObject(global_context->getSecurityConfig());
        main_config_reloader->start();
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

            tcpHttpServersHolder.onExit();

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
            metrics_transmitters.emplace_back(std::make_unique<MetricsTransmitter>(*global_context, async_metrics, graphite_key));
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
            tiflash_instance_wrap.status = EngineStoreServerStatus::Running;
            while (tiflash_instance_wrap.proxy_helper->getProxyStatus() == RaftProxyStatus::Idle)
                std::this_thread::sleep_for(std::chrono::milliseconds(200));

            // proxy update store-id before status set `RaftProxyStatus::Running`
            assert(tiflash_instance_wrap.proxy_helper->getProxyStatus() == RaftProxyStatus::Running);
            LOG_INFO(log, "store_id={}, tiflash proxy is ready to serve, try to wake up all regions' leader", tmt_context.getKVStore()->getStoreID(std::memory_order_seq_cst));
            size_t runner_cnt = config().getUInt("flash.read_index_runner_count", 1); // if set 0, DO NOT enable read-index worker
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
