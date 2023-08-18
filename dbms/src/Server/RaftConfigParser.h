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

#pragma once
#include <Core/Types.h>
#include <Storages/Transaction/StorageEngineType.h>

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
    TiDB::StorageEngine engine = DEFAULT_ENGINE;
    TiDB::SnapshotApplyMethod snapshot_apply_method = TiDB::SnapshotApplyMethod::DTFile_Directory;

public:
    TiFlashRaftConfig() = default;

    static TiFlashRaftConfig parseSettings(Poco::Util::LayeredConfiguration & config, Poco::Logger * log);
};

} // namespace DB
