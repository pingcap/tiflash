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
TiFlashRaftConfig TiFlashRaftConfig::parseSettings(Poco::Util::LayeredConfiguration & config, Poco::Logger * log)
{
    TiFlashRaftConfig res;
    res.flash_server_addr = config.getString("flash.service_addr", "0.0.0.0:3930");

    if (!config.has("raft"))
        return res;

    if (config.has("raft.pd_addr"))
    {
        String pd_service_addrs = config.getString("raft.pd_addr");
        Poco::StringTokenizer string_tokens(pd_service_addrs, ",");
        for (const auto & string_token : string_tokens)
        {
            res.pd_addrs.push_back(string_token);
        }
        LOG_FMT_INFO(log, "Found pd addrs: {}", pd_service_addrs);
    }
    else
    {
        LOG_FMT_INFO(log, "Not found pd addrs.");
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
        LOG_FMT_INFO(log, "Found ignore databases: {}", fmt_buf.toString());
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

    // just for test
    if (config.has("raft.enable_compatible_mode"))
    {
        res.enable_compatible_mode = config.getBool("raft.enable_compatible_mode");
    }

    if (config.has("raft.snapshot.method"))
    {
        String snapshot_method = config.getString("raft.snapshot.method");
        std::transform(snapshot_method.begin(), snapshot_method.end(), snapshot_method.begin(), [](char ch) { return std::tolower(ch); });
        if (snapshot_method == "block")
        {
            res.snapshot_apply_method = TiDB::SnapshotApplyMethod::Block;
        }
        else if (snapshot_method == "file1")
        {
            res.snapshot_apply_method = TiDB::SnapshotApplyMethod::DTFile_Directory;
        }
#if 0
        // Not generally available for this file format
        else if (snapshot_method == "file2")
        {
            res.snapshot_apply_method = TiDB::SnapshotApplyMethod::DTFile_Single;
        }
#endif
    }
    switch (res.snapshot_apply_method)
    {
    case TiDB::SnapshotApplyMethod::DTFile_Directory:
    case TiDB::SnapshotApplyMethod::DTFile_Single:
        if (res.engine != TiDB::StorageEngine::DT)
        {
            throw Exception(
                fmt::format("Illegal arguments: can not use DTFile to store snapshot data when the storage engine is not DeltaTree, [engine={}] [snapshot method={}]",
                            static_cast<Int32>(res.engine),
                            applyMethodToString(res.snapshot_apply_method)),
                ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
        break;
    default:
        break;
    }

    LOG_FMT_INFO(log, "Default storage engine [type={}] [snapshot.method={}]", static_cast<Int64>(res.engine), applyMethodToString(res.snapshot_apply_method));

    return res;
}

} // namespace DB
