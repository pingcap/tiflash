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
#include <Common/RedactHelpers.h>
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
<<<<<<< HEAD
=======
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
                req.err_msg = fmt::format("invalid negative size in request: {}", path);
                return req;
            }
            if (auto res = std::from_chars(trim_path.data(), trim_path.data() + trim_path.size(), req.reserve_size);
                res.ec != std::errc())
            {
                req.err_msg = fmt::format("invalid size in request: {}", path);
                return req;
            }

            req.evict_method = EvictMethod::ByEvictSize;

            // parse query as map
            auto query_map = parseHttpQueryMap(query);
            // "age"
            if (auto age_it = query_map.find("age"); age_it != query_map.end())
            {
                UInt64 min_age = 0;
                if (auto res
                    = std::from_chars(age_it->second.data(), age_it->second.data() + age_it->second.size(), min_age);
                    res.ec == std::errc())
                {
                    req.min_age = min_age;
                }
                // else just keep min_age as default
            }
            // "force"
            if (auto force_it = query_map.find("force"); force_it != query_map.end())
            {
                // any value other than "0" or "false" is considered as true
                // if no value is provided, e.g. "xxx=yyy&force", then consider it as true
                req.force_evict = !(force_it->second == "0" || force_it->second == "false");
            }
            return req;
        }
        else
        {
            if (trim_path.starts_with("type/"))
            {
                trim_path.remove_prefix(5); // remove leading 'type/'
            }
            UInt32 itype = 0;
            if (auto res = std::from_chars(trim_path.data(), trim_path.data() + trim_path.size(), itype);
                res.ec != std::errc())
            {
                req.err_msg = fmt::format("invalid file type in request: {}", path);
                return req;
            }
            std::optional<FileSegment::FileType> opt_evict_until_type
                = magic_enum::enum_cast<FileSegment::FileType>(itype);
            if (!opt_evict_until_type.has_value())
            {
                req.err_msg = fmt::format("invalid file type enum value in request: {}", path);
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

std::optional<CacheEvictType> parseCacheEvictType(std::string_view path, std::string_view api_name, String & err_msg)
{
    auto trim_path = path.substr(api_name.size());
    if (trim_path == "/mark")
        return CacheEvictType::Mark;
    if (trim_path == "/minmax")
        return CacheEvictType::MinMax;

    err_msg = fmt::format("invalid cache evict request: {}", path);
    return std::nullopt;
}

namespace
{
String buildCacheEvictOkBody(std::string_view cache_name, std::optional<std::string_view> message = std::nullopt)
{
    if (message.has_value())
        return fmt::format(R"json({{"status":"ok","cache":"{}","message":"{}"}})json", cache_name, *message);
    return fmt::format(R"json({{"status":"ok","cache":"{}"}})json", cache_name);
}

String buildJsonErrorBody(const String & err_msg)
{
    WriteBufferFromOwnString msg_buf;
    writeJSONString(err_msg, msg_buf);
    return fmt::format(R"json({{"status":"error","message":{}}})json", msg_buf.releaseStr());
}

bool evictLocalCacheAndReport(Context & global_ctx, CacheEvictType cache_type)
{
    switch (cache_type)
    {
    case CacheEvictType::Mark:
        return global_ctx.dropMarkCacheAndReport();
    case CacheEvictType::MinMax:
        return global_ctx.dropMinMaxIndexCacheAndReport();
    }
    __builtin_unreachable();
}

std::string_view cacheTypeName(CacheEvictType cache_type)
{
    switch (cache_type)
    {
    case CacheEvictType::Mark:
        return "mark";
    case CacheEvictType::MinMax:
        return "minmax";
    }
    __builtin_unreachable();
}
} // namespace

HttpRequestRes HandleHttpRequestLocalCacheEvict(
    EngineStoreServerWrap * server,
    std::string_view path,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    auto & global_ctx = server->tmt->getContext();
    auto log = Logger::get("HandleHttpRequestLocalCacheEvict");

    String err_msg;
    auto cache_type = parseCacheEvictType(path, api_name, err_msg);
    if (!cache_type.has_value())
    {
        auto body = buildJsonErrorBody(err_msg);
        LOG_WARNING(log, "invalid local cache evict request, path={} api_name={}", path, api_name);
        return buildRespWithCode(HttpRequestStatus::BadRequest, api_name, std::move(body));
    }

    const auto cache_name = cacheTypeName(*cache_type);
    // `drop*Cache()` eventually calls `LRUCache::reset()`, which clears the registry under the
    // cache mutex while leaving already-held `shared_ptr` values valid for in-flight readers.
    const bool cache_enabled = evictLocalCacheAndReport(global_ctx, *cache_type);
    LOG_INFO(log, "manual cache eviction, action=evict cache={} result={}", cache_name, cache_enabled ? "ok" : "noop");
    return buildOkResp(
        api_name,
        cache_enabled ? buildCacheEvictOkBody(cache_name) : buildCacheEvictOkBody(cache_name, "cache not enabled"));
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
    LOG_INFO(
        log,
        "handling remote cache evict request, path={} api_name={} query={}",
        Redact::stringToDebugString(path),
        api_name,
        Redact::stringToDebugString(query));
    RemoteCacheEvictRequest req = parseEvictRequest(path, api_name, query);
    if (!req.err_msg.empty())
    {
        auto body = fmt::format(R"json({{"message":"{}"}})json", req.err_msg);
        // The parse error includes the request path and must follow log redaction too.
        LOG_INFO(
            log,
            "invalid remote cache evict request, req={}",
            Redact::stringToDebugString(fmt::format("{}", req)));
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
        size_t released_size = file_cache->evictBySize(req.reserve_size, req.min_age, req.force_evict);
        auto body = fmt::format(R"json({{"req":"{}","released_size":"{}"}})json", req, released_size);
        return buildOkResp(api_name, std::move(body));
    }
    }
    __builtin_unreachable();
}

HttpRequestRes HandleHttpRequestRemoteCacheInfo(
    EngineStoreServerWrap * server,
    std::string_view /*path*/,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    auto & global_ctx = server->tmt->getContext();
    if (auto err_resp
        = allowDisaggAPI(global_ctx, api_name, DisaggregatedMode::Compute, "can not get remote cache info");
        err_resp)
    {
        return err_resp.value();
    }

    auto * file_cache = DB::FileCache::instance();
    if (!file_cache)
    {
        auto body = fmt::format(R"json({{"message":"file cache is not enabled"}})json");
        return buildRespWithCode(HttpRequestStatus::InternalError, api_name, std::move(body));
    }

    auto log = Logger::get("HandleHttpRequestRemoteCacheInfo");
    try
    {
        Poco::JSON::Array::Ptr list = new Poco::JSON::Array();
        for (const auto & [file_type, histogram] : file_cache->getCacheSizeHistogram())
        {
            Poco::JSON::Object::Ptr type_info = new Poco::JSON::Object();
            type_info->set("file_type", std::string(magic_enum::enum_name(file_type)));
            type_info->set("file_type_int", static_cast<UInt64>(file_type));
            type_info->set("histogram", histogram.toJson());
            list->add(type_info);
        }

        std::stringstream ss;
        list->stringify(ss);
        auto json_str = ss.str();
        LOG_INFO(log, "remote cache info: {}", json_str);
        return buildOkResp(api_name, json_str);
    }
    catch (...)
    {
        tryLogCurrentWarningException(log, "failed to get remote cache info");
        auto body = fmt::format(R"json({{"message":"failed to get remote cache info"}})json");
        return buildRespWithCode(HttpRequestStatus::InternalError, api_name, std::move(body));
    }
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
>>>>>>> f6e44d5d31 (security: respect log redaction for remote cache eviction requests (#11220))
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
                .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}},
            };
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
                .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0},
                },
            };
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
                .view = BaseBuffView{s->data(), s->size()},
            },
        };
    }

    return HttpRequestRes{
        .status = status,
        .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}},
    };
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
