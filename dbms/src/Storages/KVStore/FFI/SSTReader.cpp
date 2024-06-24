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


#include <Storages/KVStore/FFI/SSTReader.h>

#include <magic_enum.hpp>
#include <vector>

namespace DB
{
bool MonoSSTReader::remained() const
{
    auto remained = proxy_helper->sst_reader_interfaces.fn_remained(inner, type);
    if (!remained)
    {
        return false;
    }
    if (kind == SSTFormatKind::KIND_TABLET)
    {
        auto && r = range->comparableKeys();
        auto end = r.second.key.toString();
        auto key = buffToStrView(proxy_helper->sst_reader_interfaces.fn_key(inner, type));
        if (!end.empty() && key >= end)
        {
            if (!tail_checked)
            {
                LOG_INFO(
                    log,
                    "Observed extra data in tablet snapshot {} beyond {}, cf {}",
                    Redact::keyToDebugString(key.data(), key.size()),
                    r.second.key.toDebugString(),
                    magic_enum::enum_name(type));
                tail_checked = true;
            }
            return false;
        }
    }
    return remained;
}
BaseBuffView MonoSSTReader::keyView() const
{
    return proxy_helper->sst_reader_interfaces.fn_key(inner, type);
}
BaseBuffView MonoSSTReader::valueView() const
{
    return proxy_helper->sst_reader_interfaces.fn_value(inner, type);
}
void MonoSSTReader::next()
{
    return proxy_helper->sst_reader_interfaces.fn_next(inner, type);
}

size_t MonoSSTReader::approxSize() const
{
    return proxy_helper->sst_reader_interfaces.fn_approx_size(inner, type);
}

std::vector<std::string> MonoSSTReader::findSplitKeys(uint64_t splits_count) const
{
    if (type != ColumnFamilyType::Write)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "findSplitKeys can only be called on write cf");
    }
    RustStrWithViewVec v = proxy_helper->sst_reader_interfaces.fn_get_split_keys(inner, splits_count);
    std::vector<std::string> res;
    if (v.inner.ptr == nullptr)
        return res;
    // Safety: It is prohibited that we:
    // 1. dereference `v.inner`
    // 2. point to/refer to/move anything in `v.buffs`
    for (size_t i = 0; i < v.len; i++)
    {
        res.push_back(std::string(v.buffs[i].data, v.buffs[i].len));
    }
    RustGcHelper::instance().gcRustPtr(v.inner.ptr, v.inner.type);
    return res;
}

void MonoSSTReader::seek(BaseBuffView && view) const
{
    proxy_helper->sst_reader_interfaces.fn_seek(inner, type, EngineIteratorSeekType::Key, view);
}

void MonoSSTReader::seekToFirst() const
{
    proxy_helper->sst_reader_interfaces
        .fn_seek(inner, type, EngineIteratorSeekType::First, BaseBuffView{.data = nullptr, .len = 0});
}

void MonoSSTReader::seekToLast() const
{
    proxy_helper->sst_reader_interfaces
        .fn_seek(inner, type, EngineIteratorSeekType::Last, BaseBuffView{.data = nullptr, .len = 0});
}

MonoSSTReader::MonoSSTReader(
    const TiFlashRaftProxyHelper * proxy_helper_,
    SSTView view,
    RegionRangeFilter range_,
    size_t split_id_,
    size_t region_id_)
    : proxy_helper(proxy_helper_)
    , inner(proxy_helper->sst_reader_interfaces.fn_get_sst_reader(view, proxy_helper->proxy_ptr))
    , type(view.type)
    , range(range_)
    , tail_checked(false)
    , split_id(split_id_)
    , region_id(region_id_)
{
    log = &Poco::Logger::get("MonoSSTReader");
    kind = proxy_helper->sst_reader_interfaces.fn_kind(inner, view.type);
    if (kind == SSTFormatKind::KIND_TABLET)
    {
        auto && r = range->comparableKeys();
        auto start = r.first.key.toString();
        // 'z' will be added in proxy.
        LOG_INFO(
            log,
            "Seek cf {} to {}, split_id={} region_id={}",
            magic_enum::enum_name(type),
            Redact::keyToDebugString(start.data(), start.size()),
            split_id,
            region_id);
        if (!start.empty())
        {
            proxy_helper->sst_reader_interfaces
                .fn_seek(inner, view.type, EngineIteratorSeekType::Key, BaseBuffView{start.data(), start.size()});
        }
    }
}

size_t MonoSSTReader::getSplitId() const
{
    return split_id;
}

MonoSSTReader::~MonoSSTReader()
{
    proxy_helper->sst_reader_interfaces.fn_gc(inner, type);
}
} // namespace DB
