// Copyright 2025 PingCAP, Inc.
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

#pragma once

#include <Common/Logger.h>
#include <Common/TiFlashBuildInfo.h>
#include <Common/config.h> // for ENABLE_NEXT_GEN
#include <Common/setThreadName.h>
#include <Core/TiFlashDisaggregatedMode.h>
#include <Interpreters/Settings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/ServerInfo.h>
#include <Storages/FormatVersion.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>

#include <boost/noncopyable.hpp>
#include <chrono>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace DB
{
namespace FailPoints
{
extern const char force_set_proxy_state_machine_cpu_cores[];
} // namespace FailPoints

extern "C" {
void run_raftstore_proxy_ffi(int argc, const char * const * argv, const EngineStoreServerHelper *);
}

/// Manages the argument being passed to proxy, through `run_raftstore_proxy_ffi` call.
// This is different from `TiFlashRaftConfig` which serves computing.
struct TiFlashProxyConfig
{
    TiFlashProxyConfig(
        Poco::Util::LayeredConfiguration & config,
        const DisaggregatedMode disaggregated_mode,
        const bool use_autoscaler,
        const StorageFormatVersion & format_version,
        const Settings & settings,
        const LoggerPtr & log)
    {
        is_proxy_runnable = tryParseFromConfig(config, disaggregated_mode, use_autoscaler, log);

        // Enable unips according to `format_version`
        if (format_version.page == PageFormat::V4)
        {
            LOG_INFO(log, "Using UniPS for proxy");
            addExtraArgs("unips-enabled", "1");
        }
        read_index_runner_count = config.getUInt("flash.read_index_runner_count", 1);

        // Set the proxy's memory by size or ratio
        std::visit(
            [&](auto && arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (std::is_same_v<T, UInt64>)
                {
                    if (arg != 0)
                    {
                        LOG_INFO(log, "Limit proxy's memory, size={}", arg);
                        addExtraArgs("memory-limit-size", std::to_string(arg));
                    }
                }
                else if constexpr (std::is_same_v<T, double>)
                {
                    if (arg > 0 && arg <= 1.0)
                    {
                        LOG_INFO(log, "Limit proxy's memory, ratio={}", arg);
                        addExtraArgs("memory-limit-ratio", std::to_string(arg));
                    }
                }
            },
            settings.max_memory_usage_for_all_queries.get());
    }

    static TiFlashProxyConfig genForTest() { return TiFlashProxyConfig{}; }

    std::vector<const char *> getArgs() const
    {
        std::vector<const char *> args;
        args.reserve(val_map.size() + 1);
        args.push_back("TiFlash Proxy");
        for (const auto & [k, v] : val_map)
        {
            args.push_back(k.data());
            args.push_back(v.data());
        }
        return args;
    }

    bool isProxyRunnable() const { return is_proxy_runnable; }

    size_t getReadIndexRunnerCount() const { return read_index_runner_count; }

private:
    TiFlashProxyConfig()
    {
        // For test, bootstrap no proxy.
    }
    // TiFlash Proxy will set the default value of "flash.proxy.addr", so we don't need to set here.
    void addExtraArgs(const std::string & k, const std::string & v) { val_map["--" + k] = v; }

    // Try to parse start args from `config`.
    // Return true if proxy need to be started, and `val_map` will be filled with the
    // proxy start params.
    // Return false if proxy is not need.
    bool tryParseFromConfig(
        const Poco::Util::LayeredConfiguration & config,
        const DisaggregatedMode disaggregated_mode,
        const bool use_autoscaler,
        const LoggerPtr & log)
    {
        // tiflash_compute doesn't need proxy.
        if (disaggregated_mode == DisaggregatedMode::Compute && use_autoscaler)
        {
            LOG_INFO(log, "TiFlash Proxy will not start because AutoScale Disaggregated Compute Mode is specified.");
            return false;
        }

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("flash.proxy", keys);
        if (!config.has("raft.pd_addr"))
        {
            LOG_WARNING(log, "TiFlash Proxy will not start because `raft.pd_addr` is not configured.");
            if (!keys.empty())
                LOG_WARNING(log, "`flash.proxy.*` is ignored because TiFlash Proxy will not start.");

            return false;
        }

        {
            // config items start from `flash.proxy.`
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
            String extra_label;
            if (disaggregated_mode == DisaggregatedMode::Storage)
            {
                // For tiflash write node, it should report a extra label with "key" == "engine-role-label"
                // to distinguish with the node under non-disagg mode
                extra_label = fmt::format("engine_role={}", DISAGGREGATED_MODE_WRITE_ENGINE_ROLE);
            }
            else if (disaggregated_mode == DisaggregatedMode::Compute)
            {
                // For compute node, explicitly add a label with `exclusive=no-data` to avoid Region
                // being placed to the compute node.
                // Related logic in pd-server:
                // https://github.com/tikv/pd/blob/v8.5.0/pkg/schedule/placement/label_constraint.go#L69-L95
                extra_label = "exclusive=no-data";
            }
            if (args_map.contains("labels"))
                extra_label = fmt::format("{},{}", args_map["labels"], extra_label);
            // For non-disagg mode, no extra labels is required
            if (!extra_label.empty())
            {
                args_map["labels"] = extra_label;
            }

#if ENABLE_NEXT_GEN
            if (config.has("blacklist_file"))
                args_map["blacklist-file"] = config.getString("blacklist_file");
#endif

            for (auto && [k, v] : args_map)
                val_map.emplace("--" + k, std::move(v));
        }
        return true;
    }

    std::unordered_map<std::string, std::string> val_map;
    bool is_proxy_runnable = false;
    size_t read_index_runner_count;
};

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
        if (!parms.conf.isProxyRunnable())
            return;
        pthread_join(thread, nullptr);
    }

    void run()
    {
        if (!parms.conf.isProxyRunnable())
            return;
        pthread_attr_t attribute;
        pthread_attr_init(&attribute);
        pthread_attr_setstacksize(&attribute, parms.stack_size);
        LOG_INFO(log, "Start raft store proxy. Args: {}", parms.conf.getArgs());
        pthread_create(&thread, &attribute, runRaftStoreProxyFFI, &parms);
        pthread_attr_destroy(&attribute);
    }

private:
    static void * runRaftStoreProxyFFI(void * pv)
    {
        setThreadName("RaftStoreProxy");
        const auto & parms = *static_cast<const RunRaftStoreProxyParms *>(pv);
        const auto args = parms.conf.getArgs();
        run_raftstore_proxy_ffi(static_cast<int>(args.size()), args.data(), parms.helper);
        return nullptr;
    }

    RunRaftStoreProxyParms parms;
    pthread_t thread{};
    const LoggerPtr & log;
};

struct ProxyStateMachine
{
    ProxyStateMachine(LoggerPtr log_, TiFlashProxyConfig && proxy_conf_)
        : log(std::move(log_))
        , proxy_conf(std::move(proxy_conf_))
    {
        helper = GetEngineStoreServerHelper(&tiflash_instance_wrap);
        proxy_runner = std::make_unique<RaftStoreProxyRunner>(
            RaftStoreProxyRunner::RunRaftStoreProxyParms{&helper, proxy_conf},
            log);
    }


    // A TikvServer will be bootstrapped, FFI mechanism is enabled.
    // However, the raftstore service is not started until we call `startProxyService`.
    void runProxy()
    {
        if (proxy_conf.isProxyRunnable())
        {
            proxy_runner->run();

            LOG_INFO(log, "wait for tiflash proxy initializing");
            while (!tiflash_instance_wrap.proxy_helper)
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            LOG_INFO(log, "tiflash proxy is initialized");
        }
        else
        {
            LOG_WARNING(log, "Skipped initialize TiFlash Proxy");
        }
    }

    void initKVStore(
        TMTContext & tmt_context,
        std::optional<raft_serverpb::StoreIdent> & store_ident,
        size_t memory_limit)
    {
        auto kvstore = tmt_context.getKVStore();
        if (store_ident)
        {
            // Many service would depends on `store_id` when disagg is enabled.
            // setup the store_id restored from store_ident ASAP
            // FIXME: (bootstrap) we should bootstrap the tiflash node more early!
            metapb::Store store_meta;
            store_meta.set_id(store_ident->store_id());
            store_meta.set_node_state(metapb::NodeState::Preparing);
            kvstore->setStore(store_meta);
        }
        kvstore->setKVStoreMemoryLimit(memory_limit);
        LOG_INFO(log, "Set KVStore memory limit {}", memory_limit);
    }

    /// Restore TMTContext, including KVStore and RegionTable.
    void restoreKVStore(TMTContext & tmt_context, PathPool & path_pool) const
    {
        if (proxy_conf.isProxyRunnable() && tiflash_instance_wrap.proxy_helper == nullptr)
            throw Exception("Raft Proxy Helper is not set, should not happen");
        tmt_context.restore(path_pool, tiflash_instance_wrap.proxy_helper);
    }

    /// Set tiflash's state to Running, and wait proxy's state to Running.
    void startProxyService(TMTContext & tmt_context, const std::optional<raft_serverpb::StoreIdent> & store_ident)
    {
        if (!proxy_conf.isProxyRunnable())
            return;
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
    }

    void waitProxyServiceReady(TMTContext & tmt_context, std::atomic_size_t & terminate_signals_counter) const
    {
        if (!proxy_conf.isProxyRunnable())
            return;

        // If set 0, DO NOT enable read-index worker
        if (proxy_conf.getReadIndexRunnerCount() > 0)
        {
            auto & kvstore_ptr = tmt_context.getKVStore();
            auto worker_tick = kvstore_ptr->getConfigRef().readIndexWorkerTick();
            kvstore_ptr->initReadIndexWorkers(
                [worker_tick]() { return std::chrono::milliseconds(worker_tick); },
                /*running thread count*/ proxy_conf.getReadIndexRunnerCount());
            tmt_context.getKVStore()->asyncRunReadIndexWorkers();
            WaitCheckRegionReady(*kvstore_ptr, terminate_signals_counter);
        }
    }

    /// Wait for all read index tasks to finish.
    void waitAllReadIndexTasksFinish(TMTContext & tmt_context)
    {
        if (!proxy_conf.isProxyRunnable())
            return;
        if (tiflash_instance_wrap.status != EngineStoreServerStatus::Running)
        {
            LOG_ERROR(log, "Current status of engine-store is NOT Running, should not happen");
            exit(-1);
        }
        // Wait until there is no read-index task.
        while (tmt_context.getKVStore()->getReadIndexEvent())
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    /// Stop all services in TMTContext and ReadIndexWorkers.
    /// Then, inform proxy to stop by setting `tiflash_instance_wrap.status`.
    void stopProxy(TMTContext & tmt_context)
    {
        if (!proxy_conf.isProxyRunnable())
            return;
        tmt_context.getKVStore()->stopReadIndexWorkers();
        {
            // update status and let proxy stop all services except encryption.
            tiflash_instance_wrap.status = EngineStoreServerStatus::Stopping;
            LOG_INFO(log, "Set engine store server status Stopping");
        }
        // wait proxy to stop services
        if (proxy_conf.isProxyRunnable())
        {
            LOG_INFO(log, "Let tiflash proxy to stop all services");
            while (tiflash_instance_wrap.proxy_helper->getProxyStatus() != RaftProxyStatus::Stopped)
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            LOG_INFO(log, "All services in tiflash proxy are stopped");
        }
    }

    // TMTContext can not be accessed after this is called.
    void destroyProxyContext()
    {
        if (!proxy_conf.isProxyRunnable())
            return;

        LOG_INFO(log, "Unlink tiflash_instance_wrap.tmt");
        // Reset the `tiflash_instance_wrap.tmt` before `global_context` get released, or it will be a dangling pointer.
        // The problem is first reported in #9037, however, we proposed an insufficient fix.
        // The real fix is in #9064 which make the `reportThreadAllocBatch` method static.
        tiflash_instance_wrap.tmt = nullptr;
    }

    /// Inform proxy to shutdown, and join the thread.
    void waitProxyStopped()
    {
        if (!proxy_conf.isProxyRunnable())
            return;

        LOG_INFO(log, "Let tiflash proxy shutdown");
        tiflash_instance_wrap.status = EngineStoreServerStatus::Terminated;
        tiflash_instance_wrap.tmt = nullptr;
        LOG_INFO(log, "Wait for tiflash proxy thread to join");
        proxy_runner->join();
        LOG_INFO(log, "tiflash proxy thread is joined");
    }

    bool isProxyRunnable() const { return proxy_conf.isProxyRunnable(); }

    bool isProxyHelperInited() const { return tiflash_instance_wrap.proxy_helper != nullptr; }

    TiFlashRaftProxyHelper * getProxyHelper() const { return tiflash_instance_wrap.proxy_helper; }

    EngineStoreServerWrap * getEngineStoreServerWrap() { return &tiflash_instance_wrap; }

    // TODO: decouple `ffi_get_server_info_from_proxy` to make it does not rely on `helper`
    void getServerInfo(ServerInfo & server_info, Settings & global_settings)
    {
        /// get CPU/memory/disk info of this server
        diagnosticspb::ServerInfoRequest request;
        diagnosticspb::ServerInfoResponse response;
        request.set_tp(static_cast<diagnosticspb::ServerInfoType>(1));
        std::string req = request.SerializeAsString();
#ifndef DBMS_PUBLIC_GTEST
        // In tests, no proxy is provided, and Server is not linked to.
        ffi_get_server_info_from_proxy(reinterpret_cast<intptr_t>(&helper), strIntoView(&req), &response);
        server_info.parseSysInfo(response);
        LOG_INFO(log, "ServerInfo: {}", server_info.debugString());
#endif
        fiu_do_on(FailPoints::force_set_proxy_state_machine_cpu_cores, {
            server_info.cpu_info.logical_cores = 12345;
        }); // Mock a server_info
        setNumberOfLogicalCPUCores(server_info.cpu_info.logical_cores);
        computeAndSetNumberOfPhysicalCPUCores(server_info.cpu_info.logical_cores, server_info.cpu_info.physical_cores);

        // If the max_threads in global_settings is "0"/"auto", it is set by
        // `getNumberOfLogicalCPUCores` in `SettingMaxThreads::getAutoValue`.
        // We should let it follow the cpu cores get from proxy.
        if (global_settings.max_threads.is_auto && global_settings.max_threads.get() != getNumberOfLogicalCPUCores())
        {
            // now it should set the max_threads value according to the new logical cores
            global_settings.max_threads.setAuto();
            LOG_INFO(
                log,
                "Reset max_threads, max_threads={} logical_cores={}",
                global_settings.max_threads.get(),
                getNumberOfLogicalCPUCores());
        }
    }

private:
    LoggerPtr log;
    TiFlashProxyConfig proxy_conf;
    // The TiFlash's context of the FFI mechanism.
    // It also manages TiFlash's status which would be fetched by `fn_handle_get_engine_store_server_status`.
    EngineStoreServerWrap tiflash_instance_wrap;
    EngineStoreServerHelper helper;
    std::unique_ptr<RaftStoreProxyRunner> proxy_runner;
};
} // namespace DB
