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

#include "MockSSTReader.h"

#include <Common/Exception.h>

namespace DB
{
SSTReaderPtr fn_get_sst_reader(SSTView v, RaftStoreProxyPtr)
{
    std::string s(v.path.data, v.path.len);
    auto iter = MockSSTReader::getMockSSTData().find({s, v.type});
    if (iter == MockSSTReader::getMockSSTData().end())
        throw Exception("Can not find data in MockSSTData, [key=" + s + "] [type=" + CFToName(v.type) + "]");
    auto & d = iter->second;
    return MockSSTReader::ffi_get_cf_file_reader(d);
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

SSTReaderInterfaces make_mock_sst_reader_interface()
{
    return SSTReaderInterfaces{
        .fn_get_sst_reader = fn_get_sst_reader,
        .fn_remained = fn_remained,
        .fn_key = fn_key,
        .fn_value = fn_value,
        .fn_next = fn_next,
        .fn_gc = fn_gc,
    };
}
} // namespace DB