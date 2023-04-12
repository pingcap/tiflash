#include "Server.h"

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Common/CPUAffinityManager.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Config/ConfigReloader.h>
#include <Common/CurrentMetrics.h>
#include <Common/Macros.h>
#include <Common/RedactHelpers.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/TiFlashBuildInfo.h>
#include <Common/TiFlashException.h>
#include <Common/TiFlashMetrics.h>
#include <Common/config.h>
#include <Common/escapeForFileName.h>
#include <Common/formatReadable.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/setThreadName.h>
#include <Encryption/DataKeyManager.h>
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>
#include <Encryption/RateLimiter.h>
#include <Flash/DiagnosticsService.h>
#include <Flash/FlashService.h>
#include <Functions/registerFunctions.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadHelpers.h>
#include <IO/createReadBufferFromFileBase.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/IDAsPathUpgrader.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/loadMetadata.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Timestamp.h>
#include <Server/RaftConfigParser.h>
#include <Server/StorageConfigParser.h>
#include <Server/UserConfigParser.h>
#include <Storages/FormatVersion.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/Transaction/FileEncryption.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/registerStorages.h>
#include <TableFunctions/registerTableFunctions.h>
#include <common/ErrorHandlers.h>
#include <common/config_common.h>
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
        LOG_INFO(log, "Got environment variable MIMALLOC_CONF: " << config);
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
[[maybe_unused]] void loadBooleanConfig(Poco::Logger * log, bool & target, const char * name)
{
    auto * config = getenv(name);
    if (config)
    {
        LOG_INFO(log, "Got environment variable " << name << " = " << config);
        try
        {
            auto result = std::stoul(config);
            if (result != 0 && result != 1)
            {
                LOG_ERROR(log, "Environment variable" << name << " = " << result << " is not valid");
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

static String getNormalizedPath(const String & s)
{
    return getCanonicalPath(Poco::Path{s}.toString());
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

    const char * engine_store_version = "engine-version";
    const char * engine_store_git_hash = "engine-git-hash";
    const char * engine_store_address = "engine-addr";
    const char * engine_store_advertise_address = "advertise-engine-addr";
    const char * pd_endpoints = "pd-endpoints";

    explicit TiFlashProxyConfig(Poco::Util::LayeredConfiguration & config)
    {
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

pingcap::ClusterConfig getClusterConfig(const TiFlashSecurityConfig & security_config, const TiFlashRaftConfig & raft_config)
{
    pingcap::ClusterConfig config;
    config.tiflash_engine_key = raft_config.engine_key;
    config.tiflash_engine_value = raft_config.engine_value;
    config.ca_path = security_config.ca_path;
    config.cert_path = security_config.cert_path;
    config.key_path = security_config.key_path;
    return config;
}

Poco::Logger * grpc_log = nullptr;

void printGRPCLog(gpr_log_func_args * args)
{
    std::string log_msg = std::string(args->file) + ", line number : " + std::to_string(args->line) + ", log msg : " + args->message;
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

void UpdateMallocConfig([[maybe_unused]] Poco::Logger * log)
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
    LOG_INFO(log, "Got jemalloc version: " << version);

    auto * malloc_conf = getenv("MALLOC_CONF");
    if (malloc_conf)
    {
        LOG_INFO(log, "Got environment variable MALLOC_CONF: " << malloc_conf);
    }
    else
    {
        LOG_INFO(log, "Not found environment variable MALLOC_CONF");
    }

    RUN_FAIL_RETURN(je_mallctl("opt.background_thread", (void *)&old_b, &sz_b, nullptr, 0));
    RUN_FAIL_RETURN(je_mallctl("opt.max_background_threads", (void *)&old_max_thd, &sz_st, nullptr, 0));

    LOG_INFO(log, "Got jemalloc config: opt.background_thread " << old_b << ", opt.max_background_threads " << old_max_thd);

    if (!malloc_conf && !old_b)
    {
        LOG_INFO(log, "Try to use background_thread of jemalloc to handle purging asynchronously");

        RUN_FAIL_RETURN(je_mallctl("max_background_threads", nullptr, nullptr, (void *)&new_max_thd, sz_st));
        LOG_INFO(log, "Set jemalloc.max_background_threads " << new_max_thd);

        RUN_FAIL_RETURN(je_mallctl("background_thread", nullptr, nullptr, (void *)&new_b, sz_b));
        LOG_INFO(log, "Set jemalloc.background_thread " << new_b);
    }
#endif

#if USE_MIMALLOC
#define MI_OPTION_SHOW(OPTION) LOG_INFO(log, "mimalloc." #OPTION ": " << mi_option_get(OPTION));

    int version = mi_version();
    LOG_INFO(log, "Got mimalloc version: " << (version / 100) << "." << ((version % 100) / 10) << "." << (version % 10));
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

    RaftStoreProxyRunner(RunRaftStoreProxyParms && parms_, Poco::Logger * log_)
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
    pthread_t thread;
    Poco::Logger * log;
};

// We only need this task run once.
void initStores(Context & global_context, Poco::Logger * log, bool lazily_init_store)
{
    auto do_init_stores = [&global_context, log]() {
        auto storages = global_context.getTMTContext().getStorages().getAllStorage();
        int init_cnt = 0;
        int err_cnt = 0;
        for (auto & [table_id, storage] : storages)
        {
            try
            {
                init_cnt += storage->initStoreIfDataDirExist() ? 1 : 0;
                LOG_INFO(log, "Storage inited done [table_id=" << table_id << "]");
            }
            catch (...)
            {
                err_cnt++;
                tryLogCurrentException(log, "Storage inited fail, [table_id=" + DB::toString(table_id) + "]");
            }
        }
        LOG_INFO(log,
                 "Storage inited finish. [total_count=" << storages.size() << "] [init_count=" << init_cnt << "] [error_count=" << err_cnt
                                                        << "]");
    };
    if (lazily_init_store)
    {
        LOG_INFO(log, "Lazily init store.");
        std::thread(do_init_stores).detach();
    }
    else
    {
        LOG_INFO(log, "Not lazily init store.");
        do_init_stores();
    }
}

class Server::FlashGrpcServerHolder
{
public:
    FlashGrpcServerHolder(Server & server, const TiFlashRaftConfig & raft_config, Poco::Logger * log_)
        : log(log_)
    {
        grpc::ServerBuilder builder;
        if (server.security_config.has_tls_config)
        {
            grpc::SslServerCredentialsOptions server_cred(GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY);
            auto options = server.security_config.readAndCacheSecurityInfo();
            server_cred.pem_root_certs = options.pem_root_certs;
            server_cred.pem_key_cert_pairs.push_back(
                grpc::SslServerCredentialsOptions::PemKeyCertPair{options.pem_private_key, options.pem_cert_chain});
            builder.AddListeningPort(raft_config.flash_server_addr, grpc::SslServerCredentials(server_cred));
        }
        else
        {
            builder.AddListeningPort(raft_config.flash_server_addr, grpc::InsecureServerCredentials());
        }

        /// Init and register flash service.
        flash_service = std::make_unique<FlashService>(server);
        diagnostics_service = std::make_unique<DiagnosticsService>(server);
        builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5 * 1000));
        builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 10 * 1000));
        builder.SetOption(grpc::MakeChannelArgumentOption(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1));
        builder.RegisterService(flash_service.get());
        LOG_INFO(log, "Flash service registered");
        builder.RegisterService(diagnostics_service.get());
        LOG_INFO(log, "Diagnostics service registered");

        /// Kick off grpc server.
        // Prevent TiKV from throwing "Received message larger than max (4404462 vs. 4194304)" error.
        builder.SetMaxReceiveMessageSize(-1);
        builder.SetMaxSendMessageSize(-1);
        flash_grpc_server = builder.BuildAndStart();
        LOG_INFO(log, "Flash grpc server listening on [" << raft_config.flash_server_addr << "]");
        Debug::setServiceAddr(raft_config.flash_server_addr);
    }

    ~FlashGrpcServerHolder()
    {
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
    }

private:
    Poco::Logger * log;
    std::unique_ptr<FlashService> flash_service = nullptr;
    std::unique_ptr<DiagnosticsService> diagnostics_service = nullptr;
    std::unique_ptr<grpc::Server> flash_grpc_server = nullptr;
};

class Server::TcpHttpServersHolder
{
public:
    TcpHttpServersHolder(Server & server_, const Settings & settings, Poco::Logger * log_)
        : server(server_)
        , log(log_)
        , server_pool(1, server.config().getUInt("max_connections", 1024))
    {
        auto & config = server.config();
        auto & security_config = server.security_config;

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
                /// HTTP
                if (config.has("http_port"))
                {
                    if (security_config.has_tls_config)
                    {
                        throw Exception("tls config is set but https_port is not set ", ErrorCodes::INVALID_CONFIG_PARAMETER);
                    }
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config.getInt("http_port"));
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(
                        new HTTPServer(new HTTPHandlerFactory(server, "HTTPHandler-factory"), server_pool, socket, http_params));

                    LOG_INFO(log, "Listening http://" + address.toString());
                }

                /// HTTPS
                if (config.has("https_port"))
                {
#if Poco_NetSSL_FOUND
                    if (!security_config.has_tls_config)
                    {
                        LOG_ERROR(log, "https_port is set but tls config is not set");
                    }
                    Poco::Net::Context::Ptr context = new Poco::Net::Context(Poco::Net::Context::TLSV1_2_SERVER_USE,
                                                                             security_config.key_path,
                                                                             security_config.cert_path,
                                                                             security_config.ca_path,
                                                                             Poco::Net::Context::VerificationMode::VERIFY_STRICT);
                    std::function<bool(const Poco::Crypto::X509Certificate &)> check_common_name
                        = [&](const Poco::Crypto::X509Certificate & cert) {
                              if (security_config.allowed_common_names.empty())
                              {
                                  return true;
                              }
                              return security_config.allowed_common_names.count(cert.commonName()) > 0;
                          };
                    context->setAdhocVerification(check_common_name);
                    std::call_once(ssl_init_once, SSLInit);

                    Poco::Net::SecureServerSocket socket(context);
                    auto address = socket_bind_listen(socket, listen_host, config.getInt("https_port"), /* secure = */ true);
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(
                        new HTTPServer(new HTTPHandlerFactory(server, "HTTPSHandler-factory"), server_pool, socket, http_params));

                    LOG_INFO(log, "Listening https://" + address.toString());
#else
                    throw Exception{"HTTPS protocol is disabled because Poco library was built without NetSSL support.",
                                    ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                }

                /// TCP
                if (config.has("tcp_port"))
                {
                    if (security_config.has_tls_config)
                    {
                        LOG_ERROR(log, "tls config is set but tcp_port_secure is not set.");
                    }
                    std::call_once(ssl_init_once, SSLInit);
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config.getInt("tcp_port"));
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);
                    servers.emplace_back(new TCPServer(new TCPHandlerFactory(server), server_pool, socket, new Poco::Net::TCPServerParams));

                    LOG_INFO(log, "Listening tcp: " + address.toString());
                }
                else if (security_config.has_tls_config)
                {
                    LOG_INFO(log, "tcp_port is closed because tls config is set");
                }

<<<<<<< HEAD
                /// TCP with SSL
                if (config.has("tcp_port_secure") && !security_config.has_tls_config)
=======
                /// TCP with SSL (Not supported yet)
                if (config.has("tcp_port_secure") && !security_config->hasTlsConfig())
>>>>>>> d993276831 (*: fix exception when no tcp and http server created (#7281))
                {
#if Poco_NetSSL_FOUND
                    Poco::Net::Context::Ptr context = new Poco::Net::Context(Poco::Net::Context::TLSV1_2_SERVER_USE,
                                                                             security_config.key_path,
                                                                             security_config.cert_path,
                                                                             security_config.ca_path);
                    Poco::Net::SecureServerSocket socket(context);
                    auto address = socket_bind_listen(socket, listen_host, config.getInt("tcp_port_secure"), /* secure = */ true);
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);
                    servers.emplace_back(new TCPServer(
                        new TCPHandlerFactory(server, /* secure= */ true),
                        server_pool,
                        socket,
                        new Poco::Net::TCPServerParams));
                    LOG_INFO(log, "Listening tcp_secure: " + address.toString());
#else
                    throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                                    ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                }
                else if (security_config.has_tls_config)
                {
                    LOG_INFO(log, "tcp_port is closed because tls config is set");
                }

<<<<<<< HEAD
                /// At least one of TCP and HTTP servers must be created.
                if (servers.empty())
                    throw Exception("No 'tcp_port' and 'http_port' is specified in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

                /// Interserver IO HTTP
                if (config.has("interserver_http_port") && !security_config.has_tls_config)
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config.getInt("interserver_http_port"));
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(new HTTPServer(
                        new InterserverIOHTTPHandlerFactory(server, "InterserverIOHTTPHandler-factory"),
                        server_pool,
                        socket,
                        http_params));

                    LOG_INFO(log, "Listening interserver http: " + address.toString());
                }
                else if (security_config.has_tls_config)
                {
                    LOG_INFO(log, "internal http port is closed because tls config is set");
                }
=======
                if (servers.empty())
                    LOG_WARNING(log, "No TCP and HTTP servers are created");
>>>>>>> d993276831 (*: fix exception when no tcp and http server created (#7281))
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

        LOG_DEBUG(log,
                  "Closed all listening sockets." << (current_connections
                                                          ? " Waiting for " + toString(current_connections) + " outstanding connections."
                                                          : ""));

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

        LOG_DEBUG(log,
                  "Closed connections." << (current_connections ? " But " + toString(current_connections)
                                                    + " remains."
                                                      " Tip: To increase wait time add to config: <shutdown_wait_unfinished>60</shutdown_wait_unfinished>"
                                                                : ""));
    }

    const std::vector<std::unique_ptr<Poco::Net::TCPServer>> & getServers() const { return servers; }

private:
    Server & server;
    Poco::Logger * log;
    Poco::ThreadPool server_pool;
    std::vector<std::unique_ptr<Poco::Net::TCPServer>> servers;
};

int Server::main(const std::vector<std::string> & /*args*/)
{
    setThreadName("TiFlashMain");

    Poco::Logger * log = &logger();
#ifdef FIU_ENABLE
    fiu_init(0); // init failpoint
#endif

    UpdateMallocConfig(log);

#ifdef TIFLASH_ENABLE_AVX_SUPPORT
    loadBooleanConfig(log, simd_option::ENABLE_AVX, "TIFLASH_ENABLE_AVX");
#endif

#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
    loadBooleanConfig(log, simd_option::ENABLE_AVX512, "TIFLASH_ENABLE_AVX512");
#endif

#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
    loadBooleanConfig(log, simd_option::ENABLE_ASIMD, "TIFLASH_ENABLE_ASIMD");
#endif

#ifdef TIFLASH_ENABLE_SVE_SUPPORT
    loadBooleanConfig(log, simd_option::ENABLE_SVE, "TIFLASH_ENABLE_SVE");
#endif

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();

    TiFlashErrorRegistry::instance(); // This invocation is for initializing

    TiFlashProxyConfig proxy_conf(config());
    EngineStoreServerWrap tiflash_instance_wrap{};
    auto helper = GetEngineStoreServerHelper(
        &tiflash_instance_wrap);

    RaftStoreProxyRunner proxy_runner(RaftStoreProxyRunner::RunRaftStoreProxyParms{&helper, proxy_conf}, log);

    proxy_runner.run();

    if (proxy_conf.is_proxy_runnable)
    {
        LOG_INFO(log, "wait for tiflash proxy initializing");
        while (!tiflash_instance_wrap.proxy_helper)
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        LOG_INFO(log, "tiflash proxy is initialized");
        if (tiflash_instance_wrap.proxy_helper->checkEncryptionEnabled())
        {
            auto method = tiflash_instance_wrap.proxy_helper->getEncryptionMethod();
            LOG_INFO(log, "encryption is enabled, method is " << IntoEncryptionMethodName(method));
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

    CurrentMetrics::set(CurrentMetrics::Revision, ClickHouseRevision::get());

    // print necessary grpc log.
    grpc_log = &Poco::Logger::get("grpc");
    gpr_set_log_verbosity(GPR_LOG_SEVERITY_DEBUG);
    gpr_set_log_function(&printGRPCLog);

    /** Context contains all that query execution is dependent:
      *  settings, available functions, data types, aggregate functions, databases...
      */
    global_context = std::make_unique<Context>(Context::createGlobal());
    global_context->setGlobalContext(*global_context);
    global_context->setApplicationType(Context::ApplicationType::SERVER);

    /// Init File Provider
    if (proxy_conf.is_proxy_runnable)
    {
        bool enable_encryption = tiflash_instance_wrap.proxy_helper->checkEncryptionEnabled();
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


    // TODO: remove this configuration left by ClickHouse
    std::vector<String> all_fast_path;
    if (config().has("fast_path"))
    {
        String fast_paths = config().getString("fast_path");
        Poco::trimInPlace(fast_paths);
        if (!fast_paths.empty())
        {
            Poco::StringTokenizer string_tokens(fast_paths, ",");
            for (const auto & string_token : string_tokens)
            {
                all_fast_path.emplace_back(getNormalizedPath(std::string(string_token)));
                LOG_DEBUG(log, "Fast data part candidate path: " << all_fast_path.back());
            }
        }
    }

    // Deprecated settings.
    // `global_capacity_quota` will be ignored if `storage_config.main_capacity_quota` is not empty.
    // "0" by default, means no quota, the actual disk capacity is used.
    size_t global_capacity_quota = 0;
    TiFlashStorageConfig storage_config;
    std::tie(global_capacity_quota, storage_config) = TiFlashStorageConfig::parseSettings(config(), log);

    if (storage_config.format_version)
        setStorageFormat(storage_config.format_version);

    global_context->initializePathCapacityMetric( //
        global_capacity_quota, //
        storage_config.main_data_paths,
        storage_config.main_capacity_quota, //
        storage_config.latest_data_paths,
        storage_config.latest_capacity_quota);
    TiFlashRaftConfig raft_config = TiFlashRaftConfig::parseSettings(config(), log);
    global_context->setPathPool( //
        storage_config.main_data_paths, //
        storage_config.latest_data_paths, //
        storage_config.kvstore_data_path, //
        raft_config.enable_compatible_mode, //
        global_context->getPathCapacity(),
        global_context->getFileProvider());

    // Use pd address to define which default_database we use by default.
    // For mock test, we use "default". For deployed with pd/tidb/tikv use "system", which is always exist in TiFlash.
    std::string default_database = config().getString("default_database", raft_config.pd_addrs.empty() ? "default" : "system");
    Strings all_normal_path = storage_config.getAllNormalPaths();
    const std::string path = all_normal_path[0];
    global_context->setPath(path);
    global_context->initializePartPathSelector(std::move(all_normal_path), std::move(all_fast_path));

    /// ===== Paths related configuration initialized end ===== ///

    security_config = TiFlashSecurityConfig(config(), log);
    Redact::setRedactLog(security_config.redact_info_log);

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

    /// Init TiFlash metrics.
    global_context->initializeTiFlashMetrics();

    /// Init Rate Limiter
    global_context->initializeRateLimiter(config());

    /// Initialize main config reloader.
    auto main_config_reloader = std::make_unique<ConfigReloader>(
        config_path,
        [&](ConfigurationPtr config) {
            buildLoggers(*config);
            global_context->setClustersConfig(config);
            global_context->setMacros(std::make_unique<Macros>(*config, "macros"));
            global_context->getTMTContext().reloadConfig(*config);
            global_context->getIORateLimiter().updateConfig(*config);
            global_context->reloadDeltaTreeConfig(*config);
        },
        /* already_loaded = */ true);

    /// Initialize users config reloader.
    auto users_config_reloader = UserConfig::parseSettings(config(), config_path, global_context, log);

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

    bool use_l0_opt = config().getBool("l0_optimize", false);
    global_context->setUseL0Opt(use_l0_opt);

    /// Load global settings from default_profile and system_profile.
    global_context->setDefaultProfiles(config());
    Settings & settings = global_context->getSettingsRef();

    /// Size of cache for marks (index of MergeTree family of tables). It is necessary.
    size_t mark_cache_size = config().getUInt64("mark_cache_size");
    if (mark_cache_size)
        global_context->setMarkCache(mark_cache_size);

    /// Size of cache for minmax index, used by DeltaMerge engine.
    size_t minmax_index_cache_size = config().getUInt64("minmax_index_cache_size", mark_cache_size);
    if (minmax_index_cache_size)
        global_context->setMinMaxIndexCache(minmax_index_cache_size);

    /// Size of max memory usage of DeltaIndex, used by DeltaMerge engine.
    size_t delta_index_cache_size = config().getUInt64("delta_index_cache_size", 0);
    global_context->setDeltaIndexManager(delta_index_cache_size);

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
        auto cluster_config = getClusterConfig(security_config, raft_config);
        global_context->createTMTContext(raft_config, std::move(cluster_config));
        global_context->getTMTContext().reloadConfig(config());
    }

    {
        // Note that this must do before initialize schema sync service.
        do
        {
            // Check whether we need to upgrade directories hierarchy
            // If some database can not find in TiDB, they will be dropped
            // if theirs name is not in reserved_databases.
            // Besides, database engine in reserved_databases just keep as
            // what they are.
            IDAsPathUpgrader upgrader(
                *global_context,
                /*is_mock=*/raft_config.pd_addrs.empty(),
                /*reserved_databases=*/raft_config.ignore_databases);
            if (!upgrader.needUpgrade())
                break;
            upgrader.doUpgrade();
        } while (false);

        /// Then, load remaining databases
        loadMetadata(*global_context);
    }
    LOG_DEBUG(log, "Load metadata done.");

    /// Then, sync schemas with TiDB, and initialize schema sync service.
    for (int i = 0; i < 60; i++) // retry for 3 mins
    {
        try
        {
            global_context->getTMTContext().getSchemaSyncer()->syncSchemas(*global_context);
            break;
        }
        catch (Poco::Exception & e)
        {
            const int wait_seconds = 3;
            LOG_ERROR(log,
                      "Bootstrap failed because sync schema error: " << e.displayText() << "\nWe will sleep for " << wait_seconds
                                                                     << " seconds and try again.");
            ::sleep(wait_seconds);
        }
    }
    LOG_DEBUG(log, "Sync schemas done.");

    initStores(*global_context, log, storage_config.lazily_init_store);

    // After schema synced, set current database.
    global_context->setCurrentDatabase(default_database);

    global_context->initializeSchemaSyncService();
    CPUAffinityManager::initCPUAffinityManager(config());
    LOG_INFO(log, "CPUAffinity: " << CPUAffinityManager::getInstance().toString());
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
        if (proxy_conf.is_proxy_runnable && !tiflash_instance_wrap.proxy_helper)
            throw Exception("Raft Proxy Helper is not set, should not happen");
        /// initialize TMTContext
        global_context->getTMTContext().restore(tiflash_instance_wrap.proxy_helper);
    }

    /// Then, startup grpc server to serve raft and/or flash services.
    FlashGrpcServerHolder flash_grpc_server_holder(*this, raft_config, log);

    {
        TcpHttpServersHolder tcpHttpServersHolder(*this, settings, log);

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
            is_cancelled = true;

            tcpHttpServersHolder.onExit();

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

        auto metrics_prometheus = std::make_unique<MetricsPrometheus>(*global_context, async_metrics, security_config);

        SessionCleaner session_cleaner(*global_context);
        ClusterManagerService cluster_manager_service(*global_context, config_path);

        auto & tmt_context = global_context->getTMTContext();
        if (proxy_conf.is_proxy_runnable)
        {
            tiflash_instance_wrap.tmt = &tmt_context;
            LOG_INFO(log, "Let tiflash proxy start all services");
            tiflash_instance_wrap.status = EngineStoreServerStatus::Running;
            while (tiflash_instance_wrap.proxy_helper->getProxyStatus() == RaftProxyStatus::Idle)
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            LOG_INFO(log, "tiflash proxy is ready to serve, try to wake up all regions' leader");
            WaitCheckRegionReady(tmt_context, terminate_signals_counter);
        }
        SCOPE_EXIT({
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
