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
#include <Storages/KVStore/StorageEngineType.h>

#include <unordered_set>

namespace Poco::Util
{
class LayeredConfiguration;
} // namespace Poco::Util

namespace DB
{
struct TiFlashRaftConfig
{
    Strings pd_addrs;
    std::unordered_set<std::string> ignore_databases{"system"};

    // The addr that is bound for flash service
    // Actually its value is read from "flash.service_addr"
    std::string flash_server_addr;
    // The addr that other TiFlash nodes connect to this tiflash. Its value is set by
    // following configurations. The previous configuration will override the later
    // items.
    //   - "flash.proxy.advertise-engine-addr".
    //   - "flash.proxy.engine-addr"
    //   - "flash_server_addr"
    std::string advertise_engine_addr;

    bool for_unit_test = false;

public:
    TiFlashRaftConfig() = default;

    static TiFlashRaftConfig parseSettings(Poco::Util::AbstractConfiguration & config, const LoggerPtr & log);
};

} // namespace DB
