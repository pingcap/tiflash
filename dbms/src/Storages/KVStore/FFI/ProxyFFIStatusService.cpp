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

#include <Common/FailPoint.h>
#include <Common/FmtUtils.h>
#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/KVStore/Decode/RegionTable.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/FFI/ProxyFFICommon.h>
#include <Storages/KVStore/FFI/ProxyFFIStatusService.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3GCManager.h>
#include <TiDB/OwnerManager.h>
#include <TiDB/Schema/SchemaSyncService.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <boost/algorithm/string.hpp>
#include <magic_enum.hpp>
#include <string>

namespace DB
{
namespace FailPoints
{
extern const char sync_schema_request_failure[];
} // namespace FailPoints

template <typename T>
HttpRequestRes buildRespWithCode(HttpRequestStatus status_code, const std::string & api_name, T && body)
{
    return HttpRequestRes{
        .status = status_code,
        .api_name = GenCppStrWithView(std::string(api_name)),
        // response body
        .res = GenCppStrWithView(std::forward<T>(body)),
    };
}

template <typename T>
HttpRequestRes buildOkResp(const std::string & api_name, T && body)
{
    return buildRespWithCode(HttpRequestStatus::Ok, api_name, std::forward<T>(body));
}

HttpRequestRes HandleHttpRequestSyncStatus(
    EngineStoreServerWrap * server,
    std::string_view path,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    TableID table_id = 0;
    pingcap::pd::KeyspaceID keyspace_id = NullspaceID;
    {
        auto log = Logger::get("HandleHttpRequestSyncStatus");
        LOG_TRACE(log, "handling sync status request, path: {}, api_name: {}", path, api_name);

        // Try to handle sync status request with old schema.
        // Old schema: /{table_id}
        // New schema: /keyspace/{keyspace_id}/table/{table_id}
        auto query = path.substr(api_name.size());
        std::vector<std::string> query_parts;
        boost::split(query_parts, query, boost::is_any_of("/"));
        if (query_parts.size() != 1
            && (query_parts.size() != 4 || query_parts[0] != "keyspace" || query_parts[2] != "table"))
        {
            LOG_ERROR(log, "invalid SyncStatus request: {}", query);
            return buildRespWithCode(HttpRequestStatus::BadRequest, api_name, "");
        }


        try
        {
            if (query_parts.size() == 4)
            {
                keyspace_id = std::stoll(query_parts[1]);
                table_id = std::stoll(query_parts[3]);
            }
            else
            {
                table_id = std::stoll(query_parts[0]);
            }
        }
        catch (...)
        {
            return buildRespWithCode(HttpRequestStatus::BadRequest, api_name, "");
        }
    }

    auto & tmt = *server->tmt;

    std::vector<RegionID> region_list;
    size_t ready_region_count = 0;

    // print 30 lag regions per table per 5min.
    const size_t max_print_region = 30;
    static const std::chrono::minutes PRINT_LOG_INTERVAL = std::chrono::minutes{5};
    static Timepoint last_print_log_time = Clock::now();
    RegionTable & region_table = tmt.getRegionTable();
    // Note that the IStorage instance could be not exist if there is only empty region for the table in this TiFlash instance.
    region_table.handleInternalRegionsByTable(keyspace_id, table_id, [&](const RegionTable::InternalRegions & regions) {
        region_list.reserve(regions.size());
        bool can_log = Clock::now() > last_print_log_time + PRINT_LOG_INTERVAL;
        FmtBuffer lag_regions_log;
        size_t print_count = 0;
        for (const auto & region : regions)
        {
            UInt64 leader_safe_ts;
            UInt64 self_safe_ts;
            if (!region_table.safeTsMgr().isSafeTSLag(region.first, &leader_safe_ts, &self_safe_ts))
            {
                region_list.push_back(region.first);
            }
            else if (can_log && print_count < max_print_region)
            {
                lag_regions_log.fmtAppend(
                    "lag_region_id={}, leader_safe_ts={}, self_safe_ts={}; ",
                    region.first,
                    leader_safe_ts,
                    self_safe_ts);
                print_count++;
                last_print_log_time = Clock::now();
            }
        }
        ready_region_count = region_list.size();
        if (ready_region_count < regions.size())
        {
            LOG_DEBUG(
                Logger::get(__FUNCTION__),
                "table_id={} total_region_count={} ready_region_count={} lag_region_info={}",
                table_id,
                regions.size(),
                ready_region_count,
                lag_regions_log.toString());
        }
    });
    FmtBuffer buf;
    buf.fmtAppend("{}\n", ready_region_count);
    buf.joinStr(
        region_list.begin(),
        region_list.end(),
        [](const RegionID & region_id, FmtBuffer & fmt_buf) { fmt_buf.fmtAppend("{}", region_id); },
        " ");
    buf.append("\n");

    return buildOkResp(api_name, buf.toString());
}

// Return store status of this tiflash node
// TiUP/tidb-operator rely on this API to check whether the tiflash node is ready
HttpRequestRes HandleHttpRequestStoreStatus(
    EngineStoreServerWrap * server,
    std::string_view,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    return buildOkResp(api_name, IntoStoreStatusName(server->tmt->getStoreStatus(std::memory_order_relaxed)));
}

// Return health status of this tiflash node
HttpRequestRes HandleHttpRequestLivez(
    EngineStoreServerWrap *,
    std::string_view,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    // Simply return ok for liveness probe
    std::string response_lines;
    response_lines += "ok\n";
    return buildOkResp(api_name, std::move(response_lines));
}

// Return "readiness" status of this tiflash node
HttpRequestRes HandleHttpRequestReadyz(
    EngineStoreServerWrap * server,
    std::string_view /*path*/,
    const std::string & api_name,
    std::string_view query,
    std::string_view /*body*/)
{
    bool verbose = false;
    if (query.find("verbose") != std::string_view::npos)
        verbose = true;

    bool is_ready = true;
    std::string response_lines;

    // Check whether store_status == "running",
    auto store_status = server->tmt->getStoreStatus(std::memory_order_relaxed);
    switch (store_status)
    {
    case TMTContext::StoreStatus::Running:
        if (verbose)
        {
            response_lines += fmt::format("[+]store_status ok\n");
        }
        break;
    default:
        if (verbose)
        {
            response_lines
                += fmt::format("[-]store_status fail: store_status={}\n", magic_enum::enum_name(store_status));
        }
        is_ready = false;
        break;
    }

    if (is_ready)
    {
        // Return "ok" and 200 status code
        response_lines += "ok\n";
        return buildOkResp(api_name, std::move(response_lines));
    }
    else
    {
        // Not ready for servering requests, return error and 500 status code
        response_lines += "fail\n";
        return buildRespWithCode(HttpRequestStatus::InternalError, api_name, std::move(response_lines));
    }
}

// A guard function to allow disaggregated API calls only in expected disaggregated mode.
// If tiflash running with disaggregated mode that is equal to `expect_mode`, then return nullopt.
// If not, return a HttpRequestRes with error message.
std::optional<HttpRequestRes> allowDisaggAPI(
    Context & global_ctx,
    const std::string & api_name,
    DisaggregatedMode expect_mode,
    std::string_view message)
{
    if (global_ctx.getSharedContextDisagg()->disaggregated_mode != expect_mode)
    {
        auto body = fmt::format(
            R"json({{"message":"{}, disagg_mode={}"}})json",
            message,
            magic_enum::enum_name(global_ctx.getSharedContextDisagg()->disaggregated_mode));
        return buildOkResp(api_name, std::move(body));
    }
    return std::nullopt;
}

/// set a flag for upload all PageData to remote store from local UniPS
HttpRequestRes HandleHttpRequestRemoteReUpload(
    EngineStoreServerWrap * server,
    std::string_view,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    auto & global_ctx = server->tmt->getContext();
    if (auto err_resp = allowDisaggAPI(global_ctx, api_name, DisaggregatedMode::Storage, "can not sync remote store");
        err_resp)
    {
        return err_resp.value();
    }

    bool flag_set = global_ctx.tryUploadAllDataToRemoteStore();
    return buildOkResp(api_name, fmt::format(R"json({{"message":"flag_set={}"}})json", flag_set));
}

// get the remote gc owner info
HttpRequestRes HandleHttpRequestRemoteOwnerInfo(
    EngineStoreServerWrap * server,
    std::string_view,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    auto & global_ctx = server->tmt->getContext();
    if (auto err_resp = allowDisaggAPI(global_ctx, api_name, DisaggregatedMode::Storage, "can not get gc owner");
        err_resp)
    {
        return err_resp.value();
    }

    const auto & owner = server->tmt->getS3GCOwnerManager();
    const auto owner_info = owner->getOwnerID();
    return buildOkResp(
        api_name,
        fmt::format(
            R"json({{"status":"{}","owner_id":"{}"}})json",
            magic_enum::enum_name(owner_info.status),
            owner_info.owner_id));
}

// resign if this node is the remote gc owner
HttpRequestRes HandleHttpRequestRemoteOwnerResign(
    EngineStoreServerWrap * server,
    std::string_view,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    auto & global_ctx = server->tmt->getContext();
    if (auto err_resp = allowDisaggAPI(global_ctx, api_name, DisaggregatedMode::Storage, "can not resign gc owner");
        err_resp)
    {
        return err_resp.value();
    }

    const auto & owner = server->tmt->getS3GCOwnerManager();
    bool has_resign = owner->resignOwner();
    String msg = has_resign ? "Done" : "This node is not the remote gc owner, can't be resigned.";
    auto body = fmt::format(R"json({{"message":"{}"}})json", msg);
    return buildOkResp(api_name, std::move(body));
}

// execute remote gc if this node is the remote gc owner
HttpRequestRes HandleHttpRequestRemoteGC(
    EngineStoreServerWrap * server,
    std::string_view,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    auto & global_ctx = server->tmt->getContext();
    if (auto err_resp = allowDisaggAPI(global_ctx, api_name, DisaggregatedMode::Storage, "can not trigger gc");
        err_resp)
    {
        return err_resp.value();
    }

    const auto & owner = server->tmt->getS3GCOwnerManager();
    const auto owner_info = owner->getOwnerID();
    bool gc_is_executed = false;
    if (owner_info.status == OwnerType::IsOwner)
    {
        server->tmt->getS3GCManager()->wake();
        gc_is_executed = true;
    }
    auto body = fmt::format(
        R"json({{"status":"{}","owner_id":"{}","execute":"{}"}})json",
        magic_enum::enum_name(owner_info.status),
        owner_info.owner_id,
        gc_is_executed);
    return buildOkResp(api_name, std::move(body));
}

RemoteCacheEvictRequest parseEvictRequest(
    const std::string_view path,
    const std::string_view api_name,
    const std::string_view query)
{
    RemoteCacheEvictRequest req{
        .evict_method = EvictMethod::ByFileType,
        .evict_type = FileSegment::FileType::Unknow,
        .reserve_size = 0,
        .force_evict = false,
        .err_msg = "",
    };
    // schema:
    // - /tiflash/remote/cache/evict/{file_type_int}
    // - /tiflash/remote/cache/evict/type/{file_type_int}
    // - /tiflash/remote/cache/evict/size/{size_in_bytes}
    auto trim_path = path.substr(api_name.size());
    if (trim_path.empty() || trim_path[0] != '/')
    {
        req.err_msg = fmt::format("invalid remote cache evict request: {}", path);
        return req;
    }
    trim_path.remove_prefix(1); // remove leading '/'

    try
    {
        if (trim_path.starts_with("size/"))
        {
            trim_path.remove_prefix(5); // remove leading 'size/'
            // reject negative size
            if (trim_path.empty() || trim_path[0] == '-')
            {
                req.err_msg = fmt::format("invalid size in remote cache evict request: {}", path);
                return req;
            }
            req.reserve_size = std::stoull(trim_path.data());
            req.evict_method = EvictMethod::ByEvictSize;

            if (!query.empty() && query.find("force") != std::string_view::npos)
                req.force_evict = true;
            return req;
        }
        else
        {
            if (trim_path.starts_with("type/"))
            {
                trim_path.remove_prefix(5); // remove leading 'type/'
            }
            auto itype = std::stoull(trim_path.data());
            std::optional<FileSegment::FileType> opt_evict_until_type
                = magic_enum::enum_cast<FileSegment::FileType>(itype);
            if (!opt_evict_until_type.has_value())
            {
                req.err_msg = fmt::format("invalid file type in remote cache evict request: {}", path);
                return req;
            }
            // successfully parse the file type
            req.evict_type = opt_evict_until_type.value();
            req.evict_method = EvictMethod::ByFileType;
            return req;
        }
    }
    catch (...)
    {
        req.err_msg = fmt::format("invalid remote cache evict request: {}", path);
    }
    return req;
}

HttpRequestRes HandleHttpRequestRemoteCacheEvict(
    EngineStoreServerWrap * server,
    std::string_view path,
    const std::string & api_name,
    std::string_view query,
    std::string_view)
{
    auto & global_ctx = server->tmt->getContext();
    if (auto err_resp
        = allowDisaggAPI(global_ctx, api_name, DisaggregatedMode::Compute, "can not trigger remote cache evict");
        err_resp)
    {
        return err_resp.value();
    }

    auto log = Logger::get("HandleHttpRequestRemoteCacheEvict");
    LOG_INFO(log, "handling remote cache evict request, path={} api_name={} query={}", path, api_name, query);
    RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, query);
    if (!req.err_msg.empty())
    {
        auto body = fmt::format(R"json({{"message":"{}"}})json", req.err_msg);
        return buildRespWithCode(HttpRequestStatus::BadRequest, api_name, std::move(body));
    }
    LOG_INFO(log, "handling remote cache evict request, req={}", req);

    auto * file_cache = DB::FileCache::instance();
    if (!file_cache)
    {
        auto body = fmt::format(R"json({{"message":"file cache is not enabled"}})json");
        return buildRespWithCode(HttpRequestStatus::InternalError, api_name, std::move(body));
    }

    switch (req.evict_method)
    {
    case EvictMethod::ByFileType:
    {
        auto released_size = file_cache->evictByFileType(req.evict_type);
        auto body = fmt::format(R"json({{"req":"{}","released_size":"{}"}})json", req, released_size);
        return buildOkResp(api_name, std::move(body));
    }
    case EvictMethod::ByEvictSize:
    {
        size_t released_size = file_cache->evictBySize(req.reserve_size, req.force_evict);
        auto body = fmt::format(R"json({{"req":"{}","released_size":"{}"}})json", req, released_size);
        return buildOkResp(api_name, std::move(body));
    }
    }
    __builtin_unreachable();
}

// Acquiring the all the region ids created in this TiFlash node with given keyspace id.
HttpRequestRes HandleHttpRequestSyncRegion(
    EngineStoreServerWrap * server,
    std::string_view path,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    pingcap::pd::KeyspaceID keyspace_id = NullspaceID;
    {
        auto log = Logger::get("HandleHttpRequestSyncRegion");
        LOG_TRACE(log, "handling sync region request, path: {}, api_name: {}", path, api_name);
        // schema: /keyspace/{keyspace_id}
        auto query = path.substr(api_name.size());
        std::vector<std::string> query_parts;
        boost::split(query_parts, query, boost::is_any_of("/"));
        if (query_parts.size() != 2 || query_parts[0] != "keyspace")
        {
            LOG_WARNING(log, "invalid SyncRegion request: {}", query);
            return buildRespWithCode(HttpRequestStatus::BadRequest, api_name, "");
        }
        try
        {
            keyspace_id = std::stoll(query_parts[1]);
        }
        catch (...)
        {
            LOG_WARNING(log, "fail to parse keyspace_id, path={}", path);
            return buildRespWithCode(HttpRequestStatus::BadRequest, api_name, "");
        }
    }

    auto & tmt = *server->tmt;
    std::stringstream ss;
    Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
    Poco::JSON::Array::Ptr list = new Poco::JSON::Array();
    RegionTable & region_table = tmt.getRegionTable();
    region_table.handleInternalRegionsByKeyspace(
        keyspace_id,
        [&](const TableID table_id, const RegionTable::InternalRegions & regions) {
            if (!tmt.getStorages().get(keyspace_id, table_id))
                return;
            for (const auto & region : regions)
            {
                list->add(region.first);
            }
        });
    json->set("count", list->size());
    json->set("regions", list);
    json->stringify(ss);
    return buildOkResp(api_name, ss.str());
}

// Acquiring load schema to sync schema from TiKV in this TiFlash node with given keyspace id.
HttpRequestRes HandleHttpRequestSyncSchema(
    EngineStoreServerWrap * server,
    std::string_view path,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    auto log = Logger::get("HandleHttpRequestSyncSchema");

    auto & global_context = server->tmt->getContext();
    // For compute node, simply return OK
    if (global_context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        return buildRespWithCode(HttpRequestStatus::Ok, api_name, "");
    }

    pingcap::pd::KeyspaceID keyspace_id = NullspaceID;
    TableID table_id = InvalidTableID;
    {
        LOG_TRACE(log, "handling sync schema request, path: {}, api_name: {}", path, api_name);

        // schema: /keyspace/{keyspace_id}/table/{table_id}
        auto query = path.substr(api_name.size());
        std::vector<std::string> query_parts;
        boost::split(query_parts, query, boost::is_any_of("/"));
        if (query_parts.size() != 4 || query_parts[0] != "keyspace" || query_parts[2] != "table")
        {
            LOG_WARNING(log, "invalid SyncSchema request: {}", query);
            return buildRespWithCode(HttpRequestStatus::BadRequest, api_name, "");
        }

        try
        {
            keyspace_id = std::stoll(query_parts[1]);
            table_id = std::stoll(query_parts[3]);
        }
        catch (...)
        {
            LOG_WARNING(log, "fail to parse keyspace_id or table_id, path={}", path);
            return buildRespWithCode(HttpRequestStatus::BadRequest, api_name, "");
        }
    }

    std::string err_msg;
    try
    {
        auto & tmt_ctx = *server->tmt;
        bool done = tmt_ctx.getSchemaSyncerManager()->syncTableSchema(global_context, keyspace_id, table_id);
        if (!done)
        {
            err_msg = "sync schema failed";
        }
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::sync_schema_request_failure);
    }
    catch (const DB::Exception & e)
    {
        err_msg = e.message();
    }
    catch (...)
    {
        err_msg = "sync schema failed, unknown exception";
    }

    if (!err_msg.empty())
    {
        Poco::JSON::Object::Ptr json = new Poco::JSON::Object();
        json->set("errMsg", err_msg);
        std::stringstream ss;
        json->stringify(ss);

        return buildRespWithCode(HttpRequestStatus::InternalError, api_name, ss.str());
    }

    return buildOkResp(api_name, "");
}

using HANDLE_HTTP_URI_METHOD = HttpRequestRes (*)(
    EngineStoreServerWrap *,
    std::string_view,
    const std::string &,
    std::string_view,
    std::string_view);

// A registry of available HTTP URI prefix (API name) and their handler methods.
static const std::map<std::string, HANDLE_HTTP_URI_METHOD> AVAILABLE_HTTP_URI = {
    {"/tiflash/sync-status/", HandleHttpRequestSyncStatus},
    {"/tiflash/sync-region/", HandleHttpRequestSyncRegion},
    {"/tiflash/sync-schema/", HandleHttpRequestSyncSchema},

    // ==== liveness, readiness APIs ==== //
    // ==== TiUP/tidb-operator rely on following APIs ==== //
    // Old API kept for compatibility
    {"/tiflash/store-status", HandleHttpRequestStoreStatus},
    // liveness. Check whether the tiflash node is alive
    {"/tiflash/livez", HandleHttpRequestLivez},
    // readiness. Check whether the tiflash node is ready for serving requests
    {"/tiflash/readyz", HandleHttpRequestReadyz},

    {"/tiflash/remote/owner/info", HandleHttpRequestRemoteOwnerInfo},
    {"/tiflash/remote/owner/resign", HandleHttpRequestRemoteOwnerResign},
    {"/tiflash/remote/gc", HandleHttpRequestRemoteGC},
    {"/tiflash/remote/upload", HandleHttpRequestRemoteReUpload},
    {"/tiflash/remote/cache/evict", HandleHttpRequestRemoteCacheEvict},
};

uint8_t CheckHttpUriAvailable(BaseBuffView path_)
{
    std::string_view path(path_.data, path_.len);
    for (const auto & [api_name, method] : AVAILABLE_HTTP_URI)
    {
        std::ignore = method;
        if (path.size() >= api_name.size() && path.substr(0, api_name.size()) == api_name)
            return true;
    }
    return false;
}

HttpRequestRes HandleHttpRequest(
    EngineStoreServerWrap * server,
    BaseBuffView path_,
    BaseBuffView query,
    BaseBuffView body)
{
    std::string_view path(path_.data, path_.len);
    for (const auto & [str, method] : AVAILABLE_HTTP_URI)
    {
        if (path.size() >= str.size() && path.substr(0, str.size()) == str)
        {
            return method(
                server,
                path,
                /*api_name=*/str,
                /*query=*/std::string_view(query.data, query.len),
                /*body=*/std::string_view(body.data, body.len));
        }
    }
    // Not found handler for this URI
    return buildRespWithCode(HttpRequestStatus::NotFound, "", "");
}

} // namespace DB
