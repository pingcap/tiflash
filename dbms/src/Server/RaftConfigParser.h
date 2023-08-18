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
#include <Common/Logger.h>
#include <Core/Types.h>
#include <Storages/Transaction/StorageEngineType.h>

#include <unordered_set>

namespace Poco
{
class Logger;
namespace Util
{
class AbstractConfiguration;
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

    // The addr that is bound for flash service
    // Actually its value is read from "flash.service_addr"
    std::string flash_server_addr;
    // The addr that other nodes connect to this tiflash
    // Actually its value is read from "flash.proxy.advertise-engine-addr".
    // If "flash.proxy.advertise-engine-addr" is not set, it will fall
    // back to be the same as `flash_server_addr`.
    std::string advertise_addr;

    bool for_unit_test = false;

    static constexpr TiDB::StorageEngine DEFAULT_ENGINE = TiDB::StorageEngine::DT;
    TiDB::StorageEngine engine = DEFAULT_ENGINE;

public:
    TiFlashRaftConfig() = default;

    static TiFlashRaftConfig parseSettings(Poco::Util::AbstractConfiguration & config, const LoggerPtr & log);
};

} // namespace DB
