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
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>
#include <fmt/core.h>
#include <fmt/printf.h>

#include <string>
#include <string_view>

#include "Flash/Coprocessor/tzg-metrics.h"
#include "magic_enum.hpp"
#include "mpp.pb.h"

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
    {
        std::string table_id_str(path.substr(api_name.size()));
        try
        {
            table_id = std::stoll(table_id_str);
        }
        catch (...)
        {
            status = HttpRequestStatus::ErrorParam;
        }

        if (status != HttpRequestStatus::Ok)
            return HttpRequestRes{.status = status, .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{}}};
    }

    std::stringstream ss;
    auto & tmt = *server->tmt;

    std::vector<RegionID> region_list;
    size_t ready_region_count = 0;

    // print 30 lag regions per table per 5min.
    const size_t max_print_region = 30;
    static const std::chrono::minutes PRINT_LOG_INTERVAL = std::chrono::minutes{5};
    static Timepoint last_print_log_time = Clock::now();
    // if storage is not created in ch, flash replica should not be available.
    if (tmt.getStorages().get(table_id))
    {
        RegionTable & region_table = tmt.getRegionTable();
        region_table.handleInternalRegionsByTable(table_id, [&](const RegionTable::InternalRegions & regions) {
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
    }
    ss << ready_region_count << std::endl;
    for (const auto & region_id : region_list)
        ss << region_id << ' ';
    ss << std::endl;

    auto * s = RawCppString::New(ss.str());
    return HttpRequestRes{
        .status = status,
        .res = CppStrWithView{.inner = GenRawCppPtr(s, RawCppPtrTypeImpl::String), .view = BaseBuffView{s->data(), s->size()}}};
}

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

HttpRequestRes HandleHttpRequestCompressStatus(
    EngineStoreServerWrap *,
    std::string_view path,
    const std::string &,
    std::string_view,
    std::string_view)
{
    uint64_t compressed_size;
    uint64_t uncompressed_size;
    uint64_t package;
    tzg::SnappyStatistic::CompressMethod method;
    tzg::SnappyStatistic::globalInstance().load(compressed_size, uncompressed_size, package, method);

    // double f_compressed_size_mb = compressed_size * 1.0 / 1024 / 1024;
    // double f_uncompressed_size_mb = uncompressed_size * 1.0 / 1024 / 1024;
    auto * res = RawCppString::New(fmt::format("package: {}, compressed_size: {}, uncompressed_size: {}, compress_rate: {:.2f}, method: {}",
                                               package,
                                               compressed_size,
                                               uncompressed_size,
                                               uncompressed_size ? double(compressed_size) / uncompressed_size : 0.0,
                                               magic_enum::enum_name(method)));
    static const std::string_view clean_str = "tzg-compress-and-clean";
    if (path.find(clean_str) != path.npos)
    {
        tzg::SnappyStatistic::globalInstance().clear();
        res->append(", clean statistic");
    }
    return HttpRequestRes{
        .status = HttpRequestStatus::Ok,
        .res = CppStrWithView{
            .inner = GenRawCppPtr(res, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{res->data(), res->size()}}};
}

static inline void ClearCompress()
{
    tzg::SnappyStatistic::globalInstance().clear();
    tzg::SnappyStatistic::globalInstance().clearEncodeInfo();
}

HttpRequestRes HandleHttpRequestCompressClean(
    EngineStoreServerWrap *,
    std::string_view,
    const std::string &,
    std::string_view,
    std::string_view)
{
    ClearCompress();
    auto * res = RawCppString::New(fmt::format("Clean compress info"));

    return HttpRequestRes{
        .status = HttpRequestStatus::Ok,
        .res = CppStrWithView{
            .inner = GenRawCppPtr(res, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{res->data(), res->size()}}};
}

HttpRequestRes HandleHttpRequestStreamCnt(
    EngineStoreServerWrap *,
    std::string_view,
    const std::string &,
    std::string_view,
    std::string_view)
{
    auto cnt = tzg::SnappyStatistic::globalInstance().getChunckStreamCnt();
    auto max_cnt = tzg::SnappyStatistic::globalInstance().getMaxChunckStreamCnt();
    auto * res = RawCppString::New(fmt::format("chunck-stream-cnt: {}, max-chunck-stream-cnt: {}", cnt, max_cnt));
    return HttpRequestRes{
        .status = HttpRequestStatus::Ok,
        .res = CppStrWithView{
            .inner = GenRawCppPtr(res, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{res->data(), res->size()}}};
}

HttpRequestRes HandleHttpRequestEncodeInfo(
    EngineStoreServerWrap *,
    std::string_view,
    const std::string &,
    std::string_view,
    std::string_view)
{
    std::chrono::steady_clock::duration d;
    uint64_t ec;
    std::chrono::steady_clock::duration hash_dur;
    uint64_t hash_rows;
    tzg::SnappyStatistic::globalInstance().getEncodeInfo(d, ec, hash_dur, hash_rows);
    auto * res = RawCppString::New(fmt::format("uncompress-bytes: {}, time : {}, hash-part-write: {}, hash-part-time: {}", ec, d.count(), hash_rows, hash_dur.count()));

    return HttpRequestRes{
        .status = HttpRequestStatus::Ok,
        .res = CppStrWithView{
            .inner = GenRawCppPtr(res, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{res->data(), res->size()}}};
}

HttpRequestRes HandleHttpRequestSetCompressMethod(
    EngineStoreServerWrap *,
    std::string_view path,
    const std::string & api_name,
    std::string_view,
    std::string_view)
{
    auto method_str(path.substr(api_name.size()));
    auto method = magic_enum::enum_cast<tzg::SnappyStatistic::CompressMethod>(method_str);
    if (method)
    {
        if (tzg::SnappyStatistic::globalInstance().getMethod() != *method)
        {
            ClearCompress();
        }
        tzg::SnappyStatistic::globalInstance().setMethod(*method);
        auto * res = RawCppString::New(fmt::format("Set compress method to {}", method_str));
        return HttpRequestRes{
            .status = HttpRequestStatus::Ok,
            .res = CppStrWithView{
                .inner = GenRawCppPtr(res, RawCppPtrTypeImpl::String),
                .view = BaseBuffView{res->data(), res->size()}}};
    }
    else
    {
        return HttpRequestRes{.status = HttpRequestStatus::ErrorParam, .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{}}};
    }
}


using HANDLE_HTTP_URI_METHOD = HttpRequestRes (*)(EngineStoreServerWrap *, std::string_view, const std::string &, std::string_view, std::string_view);

static const std::map<std::string, HANDLE_HTTP_URI_METHOD> AVAILABLE_HTTP_URI = {
    {"/tiflash/sync-status/", HandleHttpRequestSyncStatus},
    {"/tiflash/store-status", HandleHttpRequestStoreStatus},
    {"/tiflash/tzg-compress", HandleHttpRequestCompressStatus},
    {"/tiflash/tzg-clean-compress", HandleHttpRequestCompressClean},
    {"/tiflash/set-tzg-compress-method/", HandleHttpRequestSetCompressMethod},
    {"/tiflash/get-tzg-compress-stream-cnt", HandleHttpRequestStreamCnt},
    {"/tiflash/get-tzg-encode-info", HandleHttpRequestEncodeInfo},
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
    return HttpRequestRes{.status = HttpRequestStatus::ErrorParam, .res = CppStrWithView{.inner = GenRawCppPtr(), .view = BaseBuffView{}}};
}

} // namespace DB
