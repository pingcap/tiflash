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

#include "MockSSTReader.h"

#include <Common/Exception.h>
#include <Debug/MockKVStore/MockRaftStoreProxy.h>

#include <ext/scope_guard.h>

namespace DB
{
SSTReaderPtr fn_get_sst_reader(SSTView v, RaftStoreProxyPtr)
{
    std::string s(v.path.data, v.path.len);
    auto iter = MockSSTReader::getMockSSTData().find({s, v.type});
    if (iter == MockSSTReader::getMockSSTData().end())
        throw Exception("Can not find data in MockSSTData, [key=" + s + "] [type=" + CFToName(v.type) + "]");
    auto & d = iter->second;
    if (v.path.data[0] == '!')
    {
        return MockSSTReader::ffi_get_cf_file_reader(d, SSTFormatKind::KIND_TABLET);
    }
    else
    {
        return MockSSTReader::ffi_get_cf_file_reader(d, SSTFormatKind::KIND_SST);
    }
}
uint8_t fn_remained(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto * reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    return reader->ffi_remained();
}
BaseBuffView fn_key(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto * reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    return reader->ffi_key();
}
BaseBuffView fn_value(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto * reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    return reader->ffi_val();
}
void fn_next(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto * reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    reader->ffi_next();
}
void fn_gc(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto * reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    delete reader;
}
SSTFormatKind fn_kind(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto * reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    return reader->ffi_kind();
}
void fn_seek(SSTReaderPtr ptr, ColumnFamilyType ct, EngineIteratorSeekType et, BaseBuffView bf)
{
    auto * reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    reader->ffi_seek(ptr, ct, et, bf);
}
static uint64_t fn_approx_size(SSTReaderPtr ptr, ColumnFamilyType)
{
    auto * reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    return reader->ffi_approx_size();
}

static RustStrWithViewVec fn_get_split_keys(SSTReaderPtr ptr, uint64_t splits_count)
{
    auto * reader = reinterpret_cast<MockSSTReader *>(ptr.inner);
    auto length = reader->length();
    RUNTIME_CHECK(splits_count > 1);
    auto size_per_split = length / splits_count;
    auto * vec = new std::vector<std::string>();
    auto split_key_count = splits_count - 1;
    auto it = reader->getBegin();
    BaseBuffView * buffs = createBaseBuffViewArray(split_key_count);
    for (size_t i = 0; i < split_key_count; i++)
    {
        for (size_t j = 0; j < size_per_split; j++)
        {
            it++;
        }
        vec->push_back(it->first);
        new (&buffs[i]) BaseBuffView{.data = vec->back().data(), .len = vec->back().size()};
    }
    GCMonitor::instance().add(RawObjType::MockVecOfString, 1);
    return RustStrWithViewVec{
        .buffs = buffs,
        .len = split_key_count,
        .inner = RawRustPtr{
            .ptr = new RustStrWithViewVecInner{.vec = vec, .buffs = buffs},
            .type = static_cast<RawRustPtrType>(RawObjType::MockVecOfString)}};
}

SSTReaderInterfaces make_mock_sst_reader_interface()
{
    return SSTReaderInterfaces{
        .fn_get_sst_reader = fn_get_sst_reader,
        .fn_remained = fn_remained,
        .fn_key = fn_key,
        .fn_value = fn_value,
        .fn_next = fn_next,
        .fn_gc = fn_gc,
        .fn_kind = fn_kind,
        .fn_seek = fn_seek,
        .fn_approx_size = fn_approx_size,
        .fn_get_split_keys = fn_get_split_keys,
    };
}
} // namespace DB