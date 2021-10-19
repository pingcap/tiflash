#pragma once
#include <Common/TiFlashBuildInfo.h>
#include <Encryption/DataKeyManager.h>
#include <Encryption/MockKeyManager.h>
#include <Poco/File.h>
#include <Server/IServer.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <daemon/BaseDaemon.h>

#include <ext/scope_guard.h>
#include <string>
#include <vector>
#define _TO_STRING(X) #X
#define TO_STRING(X) _TO_STRING(X)
using RaftStoreFFIFunc = void (*)(int argc, const char * const * argv, const DB::EngineStoreServerHelper *);

namespace DTTool
{
int mainEntryTiFlashDTTool(int argc, char ** argv);
}

namespace DTTool::Bench
{
int benchEntry(const std::vector<std::string> & opts);
}

namespace DTTool::Inspect
{
struct InspectArgs
{
    bool check;
    size_t file_id;
    std::string workdir;
};
int inspectEntry(const std::vector<std::string> & opts, RaftStoreFFIFunc ffi_function);
} // namespace DTTool::Inspect

namespace DTTool::Migrate
{
struct MigrateArgs
{
    bool no_keep;
    bool dry_mode;
    size_t file_id;
    size_t version;
    size_t frame;
    DB::ChecksumAlgo algorithm;
    std::string workdir;
};
int migrateEntry(const std::vector<std::string> & opts, RaftStoreFFIFunc ffi_function);
} // namespace DTTool::Migrate

namespace DTTool
{
template <typename Func, typename Args>
struct CLIService : public BaseDaemon
{
    struct TiFlashProxyConfig
    {
        static const std::string config_prefix;
        std::vector<const char *> args;
        std::unordered_map<std::string, std::string> val_map;
        bool is_proxy_runnable = false;

        static constexpr char ENGINE_STORE_VERSION[] = "engine-version";
        static constexpr char ENGINE_STORE_GIT_HASH[] = "engine-git-hash";
        static constexpr char ENGINE_STORE_ADDRESS[] = "engine-addr";
        static constexpr char ENGINE_STORE_ADVERTISE_ADDRESS[] = "advertise-engine-addr";
        static constexpr char PD_ENDPOINTS[] = "pd-endpoints";

        explicit TiFlashProxyConfig(Poco::Util::LayeredConfiguration & config);
    };

    struct RaftStoreProxyRunner : boost::noncopyable
    {
        struct RunRaftStoreProxyParms
        {
            const DB::EngineStoreServerHelper * helper;
            const TiFlashProxyConfig & conf;
            const RaftStoreFFIFunc ffi_function;

            /// set big enough stack size to avoid runtime error like stack-overflow.
            size_t stack_size = 1024 * 1024 * 20;
        };

        explicit RaftStoreProxyRunner(RunRaftStoreProxyParms && parms_);

        void join();

        void run();

    private:
        static void * runRaftStoreProxyFfi(void * pv);

    private:
        RunRaftStoreProxyParms parms;
        pthread_t thread;
    };

    Func func;
    RaftStoreFFIFunc ffi_function;
    const Args & args;
    std::unique_ptr<DB::Context> global_context;

    explicit CLIService(Func func_, const Args & args_, const std::string & config_file, RaftStoreFFIFunc ffi_function = nullptr);

    int main(const std::vector<std::string> &) override;
};

template <typename Func, typename Args>
CLIService<Func, Args>::TiFlashProxyConfig::TiFlashProxyConfig(Poco::Util::LayeredConfiguration & config)
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
        args_map[PD_ENDPOINTS] = config.getString("raft.pd_addr");
        args_map[ENGINE_STORE_VERSION] = TiFlashBuildInfo::getReleaseVersion();
        args_map[ENGINE_STORE_GIT_HASH] = TiFlashBuildInfo::getGitHash();
        if (!args_map.count(ENGINE_STORE_ADDRESS))
            args_map[ENGINE_STORE_ADDRESS] = config.getString("flash.service_addr");
        else
            args_map[ENGINE_STORE_ADVERTISE_ADDRESS] = args_map[ENGINE_STORE_ADDRESS];

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
template <typename Func, typename Args>
CLIService<Func, Args>::RaftStoreProxyRunner::RaftStoreProxyRunner(CLIService::RaftStoreProxyRunner::RunRaftStoreProxyParms && parms_)
    : parms(std::move(parms_))
{}
template <typename Func, typename Args>
void CLIService<Func, Args>::RaftStoreProxyRunner::join()
{
    if (!parms.conf.is_proxy_runnable)
        return;
    pthread_join(thread, nullptr);
}
template <typename Func, typename Args>
void CLIService<Func, Args>::RaftStoreProxyRunner::run()
{
    if (!parms.conf.is_proxy_runnable)
        return;
    pthread_attr_t attribute;
    pthread_attr_init(&attribute);
    pthread_attr_setstacksize(&attribute, parms.stack_size);
    pthread_create(&thread, &attribute, runRaftStoreProxyFfi, &parms);
    pthread_attr_destroy(&attribute);
}
template <typename Func, typename Args>
void * CLIService<Func, Args>::RaftStoreProxyRunner::runRaftStoreProxyFfi(void * pv)
{
    auto & parms = *static_cast<const RunRaftStoreProxyParms *>(pv);
    if (nullptr == parms.ffi_function)
    {
        throw DB::Exception("proxy is not available");
    }
    parms.ffi_function(static_cast<int>(parms.conf.args.size()), parms.conf.args.data(), parms.helper);
    return nullptr;
}

template <typename Func, typename Args>
CLIService<Func, Args>::CLIService(Func func_, const Args & args_, const std::string & config_file, RaftStoreFFIFunc ffi_function)
    : func(std::move(func_))
    , ffi_function(ffi_function)
    , args(args_)
{
    config_path = config_file;
    ConfigProcessor config_processor(config_file);
    auto loaded_config = config_processor.loadConfig();
    BaseDaemon::config().add(loaded_config.configuration);
    BaseDaemon::config().setString("config-file", config_file);
}

template <typename Func, typename Args>
int CLIService<Func, Args>::main(const std::vector<std::string> &)
{
    using namespace DB;
    TiFlashProxyConfig proxy_conf(config());
    EngineStoreServerWrap tiflash_instance_wrap{};
    auto helper = GetEngineStoreServerHelper(
        &tiflash_instance_wrap);

    typename RaftStoreProxyRunner::RunRaftStoreProxyParms parms{&helper, proxy_conf, ffi_function};
    RaftStoreProxyRunner proxy_runner(std::move(parms));

    proxy_runner.run();

    if (proxy_conf.is_proxy_runnable)
    {
        while (!tiflash_instance_wrap.proxy_helper)
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    SCOPE_EXIT({
        if (!proxy_conf.is_proxy_runnable)
        {
            proxy_runner.join();
            return;
        }
        tiflash_instance_wrap.status = EngineStoreServerStatus::Terminated;
        tiflash_instance_wrap.tmt = nullptr;
        proxy_runner.join();
    });

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

    return func(*global_context, args);
}


template <class Func, class Args>
inline const std::string CLIService<Func, Args>::TiFlashProxyConfig::config_prefix = "flash.proxy";

} // namespace DTTool