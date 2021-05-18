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
        for (auto it = string_tokens.begin(); it != string_tokens.end(); it++)
        {
            res.pd_addrs.push_back(*it);
        }
        LOG_INFO(log, "Found pd addrs: " << pd_service_addrs);
    }
    else
    {
        LOG_INFO(log, "Not found pd addrs.");
    }

    if (config.has("raft.ignore_databases"))
    {
        String ignore_dbs = config.getString("raft.ignore_databases");
        Poco::StringTokenizer string_tokens(ignore_dbs, ",");
        std::stringstream ss;
        bool first = true;
        for (auto string_token : string_tokens)
        {
            string_token = Poco::trimInPlace(string_token);
            res.ignore_databases.emplace(string_token);
            if (first)
                first = false;
            else
                ss << ", ";
            ss << string_token;
        }
        LOG_INFO(log, "Found ignore databases:" << ss.str());
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

    /// "tmt" engine ONLY support disable_bg_flush = false.
    /// "dt" engine ONLY support disable_bg_flush = true.

    String disable_bg_flush_conf = "raft.disable_bg_flush";
    if (res.engine == ::TiDB::StorageEngine::TMT)
    {
        if (config.has(disable_bg_flush_conf) && config.getBool(disable_bg_flush_conf))
            throw Exception("Illegal arguments: disable background flush while using engine " + MutableSupport::txn_storage_name,
                ErrorCodes::INVALID_CONFIG_PARAMETER);
        res.disable_bg_flush = false;
    }
    else if (res.engine == ::TiDB::StorageEngine::DT)
    {
        /// If background flush is enabled, read will not triggle schema sync.
        /// Which means that we may get the wrong result with outdated schema.
        if (config.has(disable_bg_flush_conf) && !config.getBool(disable_bg_flush_conf))
            throw Exception("Illegal arguments: enable background flush while using engine " + MutableSupport::delta_tree_storage_name,
                ErrorCodes::INVALID_CONFIG_PARAMETER);
        res.disable_bg_flush = true;
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
        else if (snapshot_method == "file2")
        {
            res.snapshot_apply_method = TiDB::SnapshotApplyMethod::DTFile_Single;
        }
    }
    switch (res.snapshot_apply_method)
    {
        case TiDB::SnapshotApplyMethod::DTFile_Directory:
        case TiDB::SnapshotApplyMethod::DTFile_Single:
            if (res.engine != TiDB::StorageEngine::DT)
            {
                throw Exception(
                    "Illegal arguments: can not use DTFile to store snapshot data when the storage engine is not DeltaTree, [engine="
                        + DB::toString(static_cast<Int32>(res.engine))
                        + "] [snapshot method=" + applyMethodToString(res.snapshot_apply_method) + "]",
                    ErrorCodes::INVALID_CONFIG_PARAMETER);
            }
            break;
        default:
            break;
    }

    LOG_INFO(log,
        "Default storage engine [type=" << static_cast<Int64>(res.engine)
                                        << "] [snapshot.method=" << applyMethodToString(res.snapshot_apply_method) << "]");

    return res;
}

} // namespace DB
