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


#include <Storages/Transaction/SSTReader.h>

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
                LOG_DEBUG(log, "Observed extra data in tablet snapshot {} beyond {}, cf {}", Redact::keyToDebugString(key.data(), key.size()), r.second.key.toDebugString(), magic_enum::enum_name(type));
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

MonoSSTReader::MonoSSTReader(const TiFlashRaftProxyHelper * proxy_helper_, SSTView view, RegionRangeFilter range_)
    : proxy_helper(proxy_helper_)
    , inner(proxy_helper->sst_reader_interfaces.fn_get_sst_reader(view, proxy_helper->proxy_ptr))
    , type(view.type)
    , range(range_)
    , tail_checked(false)
{
    log = &Poco::Logger::get("MonoSSTReader");
    kind = proxy_helper->sst_reader_interfaces.fn_kind(inner, view.type);
    if (kind == SSTFormatKind::KIND_TABLET)
    {
        auto && r = range->comparableKeys();
        auto start = r.first.key.toString();
        LOG_DEBUG(log, "Seek cf {} to {}", magic_enum::enum_name(type), Redact::keyToDebugString(start.data(), start.size()));
        if (!start.empty())
        {
            proxy_helper->sst_reader_interfaces.fn_seek(inner, view.type, EngineIteratorSeekType::Key, BaseBuffView{start.data(), start.size()});
        }
    }
}

MonoSSTReader::~MonoSSTReader()
{
    proxy_helper->sst_reader_interfaces.fn_gc(inner, type);
}
} // namespace DB
