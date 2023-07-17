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

#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>
#include <fmt/core.h>

#include <boost/algorithm/string.hpp>
#include <magic_enum.hpp>

namespace DB
{
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
        auto * log = &Poco::Logger::get("HandleHttpRequestSyncStatus");
        LOG_TRACE(log, "handling sync status request, path: {}, api_name: {}", path, api_name);

        // Try to handle sync status request with old schema.
        // Old schema: /{table_id}
        // New schema: /keyspace/{keyspace_id}/table/{table_id}
        auto query = path.substr(api_name.size());
        std::vector<std::string> query_parts;
        boost::split(query_parts, query, boost::is_any_of("/"));
        if (query_parts.size() != 1 && (query_parts.size() != 4 || query_parts[0] != "keyspace" || query_parts[2] != "table"))
        {
            LOG_ERROR(log, "invalid SyncStatus request: {}", query);
            status = HttpRequestStatus::ErrorParam;
            return HttpRequestRes{.status = status, .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}}};
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
            return HttpRequestRes{.status = status, .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}}};
    }

    std::stringstream ss;
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
                lag_regions_log.fmtAppend("lag_region_id={}, leader_safe_ts={}, self_safe_ts={}; ", region.first, leader_safe_ts, self_safe_ts);
                print_count++;
                last_print_log_time = Clock::now();
            }
        }
        ready_region_count = region_list.size();
        if (ready_region_count < regions.size())
        {
            LOG_DEBUG(Logger::get(__FUNCTION__), "table_id={}, total_region_count={}, ready_region_count={}, lag_region_info={}", table_id, regions.size(), ready_region_count, lag_regions_log.toString());
        }
    });

    ss << ready_region_count << std::endl;
    for (const auto & region_id : region_list)
        ss << region_id << ' ';
    ss << std::endl;

    auto * s = RawCppString::New(ss.str());
    return HttpRequestRes{
        .status = status,
        .res = CppStrWithView{.inner = GenRawCppPtr(s, RawCppPtrTypeImpl::String), .view = BaseBuffView{s->data(), s->size()}}};
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
        .res = CppStrWithView{
            .inner = GenRawCppPtr(name, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{name->data(), name->size()}}};
}

HttpRequestRes HandleHttpRequestRemoteStoreSync(
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
                .view = BaseBuffView{body->data(), body->size()}},
        };
    }

    bool flag_set = global_ctx.trySyncAllDataToRemoteStore();
    auto * body = RawCppString::New(fmt::format(R"json({{"message":"flag_set={}"}})json", flag_set));
    return HttpRequestRes{
        .status = flag_set ? HttpRequestStatus::Ok : HttpRequestStatus::ErrorParam,
        .res = CppStrWithView{
            .inner = GenRawCppPtr(body, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{body->data(), body->size()}},
    };
}

using HANDLE_HTTP_URI_METHOD = HttpRequestRes (*)(EngineStoreServerWrap *, std::string_view, const std::string &, std::string_view, std::string_view);

static const std::map<std::string, HANDLE_HTTP_URI_METHOD> AVAILABLE_HTTP_URI = {
    {"/tiflash/sync-status/", HandleHttpRequestSyncStatus},
    {"/tiflash/store-status", HandleHttpRequestStoreStatus},
    {"/tiflash/sync-remote-store", HandleHttpRequestRemoteStoreSync},
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

HttpRequestRes HandleHttpRequest(EngineStoreServerWrap * server, BaseBuffView path_, BaseBuffView query, BaseBuffView body)
{
    std::string_view path(path_.data, path_.len);
    for (const auto & [str, method] : AVAILABLE_HTTP_URI)
    {
        if (path.size() >= str.size() && path.substr(0, str.size()) == str)
        {
            return method(server, path, str, std::string_view(query.data, query.len), std::string_view(body.data, body.len));
        }
    }
    return HttpRequestRes{.status = HttpRequestStatus::ErrorParam, .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{nullptr, 0}}};
}

} // namespace DB
