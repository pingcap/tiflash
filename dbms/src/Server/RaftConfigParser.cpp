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

#include <Common/FmtUtils.h>
#include <IO/WriteHelpers.h>
#include <Poco/String.h>
#include <Poco/StringTokenizer.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/RaftConfigParser.h>
#include <Storages/MutableSupport.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
} // namespace ErrorCodes

/// Load raft related configs.
TiFlashRaftConfig TiFlashRaftConfig::parseSettings(Poco::Util::AbstractConfiguration & config, const LoggerPtr & log)
{
    TiFlashRaftConfig res;
    // The ip/port that flash service bind to
    res.flash_server_addr = config.getString("flash.service_addr", "0.0.0.0:3930");
    // The ip/port that other flash server connect to
    res.advertise_engine_addr = config.getString(
        "flash.proxy.advertise-engine-addr",
        config.getString("flash.proxy.engine-addr", res.flash_server_addr));

    // "addr" - The raft ip/port that raft service bind to
    // "advertise-addr" - The raft ip/port that other raft server connect to

    {
        // Check by `raft` prefix instead of check by `config.has("raft")`,
        // because when sub keys are set from cli args, `raft` will not exist.
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("raft", keys);
        if (keys.empty())
            return res;
    }

    if (config.has("raft.pd_addr"))
    {
        String pd_service_addrs = config.getString("raft.pd_addr");
        Poco::StringTokenizer string_tokens(pd_service_addrs, ",");
        for (const auto & string_token : string_tokens)
        {
            res.pd_addrs.push_back(string_token);
        }
        LOG_INFO(log, "Found pd addrs: {}", pd_service_addrs);
    }
    else
    {
        LOG_INFO(log, "Not found pd addrs.");
    }

    if (config.has("raft.ignore_databases"))
    {
        String ignore_dbs = config.getString("raft.ignore_databases");
        Poco::StringTokenizer string_tokens(ignore_dbs, ",");
        FmtBuffer fmt_buf;
        fmt_buf.joinStr(
            string_tokens.begin(),
            string_tokens.end(),
            [&res](auto arg, FmtBuffer & fb) {
                arg = Poco::trimInPlace(arg);
                res.ignore_databases.emplace(arg);
                fb.append(arg);
            },
            ", ");
        LOG_INFO(log, "Found ignore databases: {}", fmt_buf.toString());
    }

    if (config.has("raft.storage_engine"))
    {
        String s_engine = config.getString("raft.storage_engine");
        std::transform(s_engine.begin(), s_engine.end(), s_engine.begin(), [](char ch) { return std::tolower(ch); });
        if (s_engine == "tmt")
            res.engine = ::TiDB::StorageEngine::TMT;
        else if (s_engine == "dt")
            res.engine = ::TiDB::StorageEngine::DT;
        else
            res.engine = DEFAULT_ENGINE;
    }

    LOG_INFO(log, "Default storage engine [type={}]", static_cast<Int64>(res.engine));

    return res;
}

} // namespace DB
