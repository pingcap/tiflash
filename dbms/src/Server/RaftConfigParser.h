// Copyright 2022 PingCAP, Ltd.
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
#include <Core/Types.h>
#include <Storages/Transaction/StorageEngineType.h>

#include <unordered_map>
#include <unordered_set>

namespace Poco
{
class Logger;
namespace Util
{
class LayeredConfiguration;
}
} // namespace Poco

namespace DB
{
struct TiFlashRaftConfig
{
    const std::string engine_key = "engine";
    const std::string engine_value = "tiflash";
    Strings pd_addrs;
    std::unordered_set<std::string> ignore_databases{"system"};
    // Actually it is "flash.service_addr"
    std::string flash_server_addr;
    bool enable_compatible_mode = true;

    static constexpr TiDB::StorageEngine DEFAULT_ENGINE = TiDB::StorageEngine::DT;
    bool disable_bg_flush = false;
    TiDB::StorageEngine engine = DEFAULT_ENGINE;
    TiDB::SnapshotApplyMethod snapshot_apply_method = TiDB::SnapshotApplyMethod::DTFile_Directory;

public:
    TiFlashRaftConfig() = default;

    static TiFlashRaftConfig parseSettings(Poco::Util::LayeredConfiguration & config, Poco::Logger * log);
};


struct TiFlashProxyConfig
{
    std::vector<const char *> args;
    std::unordered_map<std::string, std::string> val_map;
    bool is_proxy_runnable = false;
    TiDB::NodeRole role = TiDB::NodeRole::WriteNode;

    const String config_prefix = "flash.proxy";
    // TiFlash Proxy will set the default value of "flash.proxy.addr", so we don't need to set here.
    const String engine_store_version = "engine-version";
    const String engine_store_git_hash = "engine-git-hash";
    const String engine_store_address = "engine-addr";
    const String engine_store_advertise_address = "advertise-engine-addr";
    const String pd_endpoints = "pd-endpoints";
    const String engine_label = "engine-label";
    const String default_engine_label_value = "tiflash";

    explicit TiFlashProxyConfig(Poco::Util::LayeredConfiguration & config);
};

} // namespace DB
