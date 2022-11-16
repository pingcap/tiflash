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

#include <vector>

namespace DB
{
bool MonoSSTReader::remained() const
{
    return proxy_helper->sst_reader_interfaces.fn_remained(inner, type);
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

MonoSSTReader::MonoSSTReader(const TiFlashRaftProxyHelper * proxy_helper_, SSTView view)
    : proxy_helper(proxy_helper_)
    , inner(proxy_helper->sst_reader_interfaces.fn_get_sst_reader(view, proxy_helper->proxy_ptr))
    , type(view.type)
{}

MonoSSTReader::~MonoSSTReader()
{
    proxy_helper->sst_reader_interfaces.fn_gc(inner, type);
}
} // namespace DB
