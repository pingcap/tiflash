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

inline void getServerInfoFromProxy(
    LoggerPtr log,
    ServerInfo & server_info,
    EngineStoreServerHelper * helper,
    Settings & global_settings)
{
    /// get CPU/memory/disk info of this server
    diagnosticspb::ServerInfoRequest request;
    diagnosticspb::ServerInfoResponse response;
    request.set_tp(static_cast<diagnosticspb::ServerInfoType>(1));
    std::string req = request.SerializeAsString();
#ifndef DBMS_PUBLIC_GTEST
    // In tests, no proxy is provided, and Server is not linked to.
    ffi_get_server_info_from_proxy(reinterpret_cast<intptr_t>(helper), strIntoView(&req), &response);
    server_info.parseSysInfo(response);
    LOG_INFO(log, "ServerInfo: {}", server_info.debugString());
#else
    UNUSED(helper);
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
} // namespace DB
