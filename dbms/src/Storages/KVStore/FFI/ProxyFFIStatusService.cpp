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
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/S3/S3GCManager.h>
#include <TiDB/OwnerManager.h>
#include <TiDB/Schema/SchemaSyncService.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <common/logger_useful.h>
#include <fmt/core.h>

#include <boost/algorithm/string.hpp>
#include <magic_enum.hpp>

namespace DB
{
namespace FailPoints
{
extern const char sync_schema_request_failure[];
} // namespace FailPoints

HttpRequestRes HandleHttpRequestSyncStatus(
    EngineStoreServerWrap * server,
    std::string_view path,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    HttpRequestStatus status = HttpRequestStatus::Ok;
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
            status = HttpRequestStatus::ErrorParam;
            return HttpRequestRes{
                .status = status,
                .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}},
            };
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
            status = HttpRequestStatus::ErrorParam;
        }

        if (status != HttpRequestStatus::Ok)
            return HttpRequestRes{
                .status = status,
                .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}},
            };
    }

    auto & tmt = *server->tmt;

    std::vector<RegionID> region_list;
    size_t ready_region_count = 0;

    // print 30 lag regions per table per 5min.
    const size_t max_print_region = 30;
    static const std::chrono::minutes PRINT_LOG_INTERVAL = std::chrono::minutes{5};
    static Timepoint last_print_log_time = Clock::now();
    // TODO(iosmanthus): TiDB should support tiflash replica.
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
            if (!region_table.isSafeTSLag(region.first, &leader_safe_ts, &self_safe_ts))
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

    auto * s = RawCppString::New(buf.toString());
    return HttpRequestRes{
        .status = status,
        .res = CppStrWithView{
            .inner = GenRawCppPtr(s, RawCppPtrTypeImpl::String), .view = BaseBuffView{s->data(), s->size()},
        },
    };
}

// Return store status of this tiflash node
HttpRequestRes HandleHttpRequestStoreStatus(
    EngineStoreServerWrap * server,
    std::string_view,
    const std::string &,
    std::string_view,
    std::string_view)
{
    auto * name = RawCppString::New(IntoStoreStatusName(server->tmt->getStoreStatus(std::memory_order_relaxed)));
    return HttpRequestRes{
        .status = HttpRequestStatus::Ok,
        .res= CppStrWithView{
            .inner = GenRawCppPtr(name, RawCppPtrTypeImpl::String), .view = BaseBuffView{name->data(), name->size()},
        },
    };
}

/// set a flag for upload all PageData to remote store from local UniPS
HttpRequestRes HandleHttpRequestRemoteReUpload(
    EngineStoreServerWrap * server,
    std::string_view,
    const std::string &,
    std::string_view,
    std::string_view)
{
    auto & global_ctx = server->tmt->getContext();
    if (!global_ctx.getSharedContextDisagg()->isDisaggregatedStorageMode())
    {
        auto * body = RawCppString::New(fmt::format(
            R"json({{"message":"can not sync remote store on a node with disagg_mode={}"}})json",
            magic_enum::enum_name(global_ctx.getSharedContextDisagg()->disaggregated_mode)));
        return HttpRequestRes{
            .status = HttpRequestStatus::ErrorParam,
            .res = CppStrWithView{
                .inner = GenRawCppPtr(body, RawCppPtrTypeImpl::String),
                .view = BaseBuffView{body->data(), body->size()},
            },
        };
    }

    bool flag_set = global_ctx.tryUploadAllDataToRemoteStore();
    auto * body = RawCppString::New(fmt::format(R"json({{"message":"flag_set={}"}})json", flag_set));
    return HttpRequestRes{
        .status = flag_set ? HttpRequestStatus::Ok : HttpRequestStatus::ErrorParam,
        .res = CppStrWithView{
            .inner = GenRawCppPtr(body, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{body->data(), body->size()},
        },
    };
}

// get the remote gc owner info
HttpRequestRes HandleHttpRequestRemoteOwnerInfo(
    EngineStoreServerWrap * server,
    std::string_view,
    const std::string &,
    std::string_view,
    std::string_view)
{
    const auto & owner = server->tmt->getS3GCOwnerManager();
    const auto owner_info = owner->getOwnerID();
    auto * body = RawCppString::New(fmt::format(
        R"json({{"status":"{}","owner_id":"{}"}})json",
        magic_enum::enum_name(owner_info.status),
        owner_info.owner_id));
    return HttpRequestRes{
        .status = HttpRequestStatus::Ok,
        .res = CppStrWithView{
            .inner = GenRawCppPtr(body, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{body->data(), body->size()},
        },
    };
}

// resign if this node is the remote gc owner
HttpRequestRes HandleHttpRequestRemoteOwnerResign(
    EngineStoreServerWrap * server,
    std::string_view,
    const std::string &,
    std::string_view,
    std::string_view)
{
    const auto & owner = server->tmt->getS3GCOwnerManager();
    bool has_resign = owner->resignOwner();
    String msg = has_resign ? "Done" : "This node is not the remote gc owner, can't be resigned.";
    auto * body = RawCppString::New(fmt::format(R"json({{"message":"{}"}})json", msg));
    return HttpRequestRes{
        .status = HttpRequestStatus::Ok,
        .res = CppStrWithView{
            .inner = GenRawCppPtr(body, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{body->data(), body->size()},
        },
    };
}

// execute remote gc if this node is the remote gc owner
HttpRequestRes HandleHttpRequestRemoteGC(
    EngineStoreServerWrap * server,
    std::string_view,
    const std::string &,
    std::string_view,
    std::string_view)
{
    const auto & owner = server->tmt->getS3GCOwnerManager();
    const auto owner_info = owner->getOwnerID();
    bool gc_is_executed = false;
    if (owner_info.status == OwnerType::IsOwner)
    {
        server->tmt->getS3GCManager()->wake();
        gc_is_executed = true;
    }
    auto * body = RawCppString::New(fmt::format(
        R"json({{"status":"{}","owner_id":"{}","execute":"{}"}})json",
        magic_enum::enum_name(owner_info.status),
        owner_info.owner_id,
        gc_is_executed));
    return HttpRequestRes{
        .status = HttpRequestStatus::Ok,
        .res = CppStrWithView{
            .inner = GenRawCppPtr(body, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{body->data(), body->size()},
        },
    };
}

// Acquiring load schema to sync schema from TiKV in this TiFlash node with given keyspace id.
HttpRequestRes HandleHttpRequestSyncSchema(
    EngineStoreServerWrap * server,
    std::string_view path,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    pingcap::pd::KeyspaceID keyspace_id = NullspaceID;
    TableID table_id = InvalidTableID;
    HttpRequestStatus status = HttpRequestStatus::Ok;
    auto log = Logger::get("HandleHttpRequestSyncSchema");

    auto & global_context = server->tmt->getContext();
    // For compute node, simply return OK
    if (global_context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        return HttpRequestRes{
            .status = status,
            .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}},
        };
    }

    {
        LOG_TRACE(log, "handling sync schema request, path: {}, api_name: {}", path, api_name);

        // schema: /keyspace/{keyspace_id}/table/{table_id}
        auto query = path.substr(api_name.size());
        std::vector<std::string> query_parts;
        boost::split(query_parts, query, boost::is_any_of("/"));
        if (query_parts.size() != 4 || query_parts[0] != "keyspace" || query_parts[2] != "table")
        {
            LOG_ERROR(log, "invalid SyncSchema request: {}", query);
            status = HttpRequestStatus::ErrorParam;
            return HttpRequestRes{
                .status = HttpRequestStatus::ErrorParam,
                .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}}};
        }

        try
        {
            keyspace_id = std::stoll(query_parts[1]);
            table_id = std::stoll(query_parts[3]);
        }
        catch (...)
        {
            status = HttpRequestStatus::ErrorParam;
        }

        if (status != HttpRequestStatus::Ok)
            return HttpRequestRes{
                .status = status,
                .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}}};
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

        auto * s = RawCppString::New(ss.str());
        return HttpRequestRes{
            .status = HttpRequestStatus::ErrorParam,
            .res = CppStrWithView{
                .inner = GenRawCppPtr(s, RawCppPtrTypeImpl::String),
                .view = BaseBuffView{s->data(), s->size()}}};
    }

    return HttpRequestRes{
        .status = status,
        .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}}};
}

using HANDLE_HTTP_URI_METHOD = HttpRequestRes (*)(
    EngineStoreServerWrap *,
    std::string_view,
    const std::string &,
    std::string_view,
    std::string_view);

static const std::map<std::string, HANDLE_HTTP_URI_METHOD> AVAILABLE_HTTP_URI = {
    {"/tiflash/sync-status/", HandleHttpRequestSyncStatus},
    {"/tiflash/sync-schema/", HandleHttpRequestSyncSchema},
    {"/tiflash/store-status", HandleHttpRequestStoreStatus},
    {"/tiflash/remote/owner/info", HandleHttpRequestRemoteOwnerInfo},
    {"/tiflash/remote/owner/resign", HandleHttpRequestRemoteOwnerResign},
    {"/tiflash/remote/gc", HandleHttpRequestRemoteGC},
    {"/tiflash/remote/upload", HandleHttpRequestRemoteReUpload},
};

uint8_t CheckHttpUriAvailable(BaseBuffView path_)
{
    std::string_view path(path_.data, path_.len);
    for (const auto & [str, method] : AVAILABLE_HTTP_URI)
    {
        std::ignore = method;
        if (path.size() >= str.size() && path.substr(0, str.size()) == str)
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
                str,
                std::string_view(query.data, query.len),
                std::string_view(body.data, body.len));
        }
    }
    return HttpRequestRes{
        .status = HttpRequestStatus::ErrorParam,
        .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}},
    };
}

} // namespace DB
