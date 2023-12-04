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

#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/ProxyFFICommon.h>
#include <Storages/Transaction/RegionTable.h>
#include <Storages/Transaction/TMTContext.h>
#include <fmt/core.h>

namespace DB
{
HttpRequestRes HandleHttpRequestTestShow(
    EngineStoreServerWrap *,
    std::string_view path,
    const std::string & api_name,
    std::string_view query,
    std::string_view body)
{
    auto * res = RawCppString::New(fmt::format(
        "api_name: {}\npath: {}\nquery: {}\nbody: {}",
        api_name,
        path,
        query,
        body));
    return HttpRequestRes{
        .status = HttpRequestStatus::Ok,
        .res = CppStrWithView{
            .inner = GenRawCppPtr(res, RawCppPtrTypeImpl::String),
            .view = BaseBuffView{res->data(), res->size()}}};
}

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
    size_t count = 0;

    // if storage is not created in ch, flash replica should not be available.
    if (tmt.getStorages().get(table_id))
    {
        tmt.getRegionTable().handleInternalRegionsByTable(table_id, [&](const RegionTable::InternalRegions & regions) {
            count = regions.size();
            region_list.reserve(regions.size());
            for (const auto & region : regions)
                region_list.push_back(region.first);
        });
    }
    ss << count << std::endl;
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

using HANDLE_HTTP_URI_METHOD = HttpRequestRes (*)(EngineStoreServerWrap *, std::string_view, const std::string &, std::string_view, std::string_view);

static const std::map<std::string, HANDLE_HTTP_URI_METHOD> AVAILABLE_HTTP_URI = {
    {"/tiflash/sync-status/", HandleHttpRequestSyncStatus},
    {"/tiflash/store-status", HandleHttpRequestStoreStatus},
    {"/tiflash/test-show", HandleHttpRequestTestShow}};

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
