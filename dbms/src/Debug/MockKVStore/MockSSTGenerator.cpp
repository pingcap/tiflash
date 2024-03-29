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

#include <Debug/MockKVStore/MockSSTGenerator.h>
#include <Debug/MockKVStore/MockSSTReader.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>

namespace DB
{
void MockSSTGenerator::finish_file(SSTFormatKind kind)
{
    if (freezed)
        return;
    auto region_id_str = std::to_string(region_id) + "_multi_" + std::to_string(c);
    region_id_str = MockSSTGenerator::encodeSSTView(kind, region_id_str);
    c++;
    sst_files.push_back(region_id_str);
    MockSSTReader::Data kv_list;
    for (auto & kv : kvs)
    {
        kv_list.emplace_back(kv.first, kv.second);
    }
    std::sort(kv_list.begin(), kv_list.end(), [](const auto & a, const auto & b) { return a.first < b.first; });
    std::scoped_lock<std::mutex> lock(MockSSTReader::mut);
    auto & mmp = MockSSTReader::getMockSSTData();
    mmp[MockSSTReader::Key{region_id_str, type}] = std::move(kv_list);
    kvs.clear();
}

std::vector<SSTView> MockSSTGenerator::ssts() const
{
    assert(freezed);
    std::vector<SSTView> sst_views;
    sst_views.reserve(sst_files.size());
    for (const auto & sst_file : sst_files)
    {
        sst_views.push_back(SSTView{
            type,
            BaseBuffView{sst_file.c_str(), sst_file.size()},
        });
    }
    return sst_views;
}

MockSSTGenerator::MockSSTGenerator(UInt64 region_id_, TableID table_id_, ColumnFamilyType type_)
    : region_id(region_id_)
    , table_id(table_id_)
    , type(type_)
    , c(0)
    , freezed(false)
{
    std::scoped_lock<std::mutex> lock(MockSSTReader::mut);
    auto & mmp = MockSSTReader::getMockSSTData();
    auto region_id_str = std::to_string(region_id) + "_multi_" + std::to_string(c);
    mmp[MockSSTReader::Key{region_id_str, type}].clear();
}

void MockSSTGenerator::insert(HandleID key, std::string val)
{
    auto k = RecordKVFormat::genKey(table_id, key, 1);
    TiKVValue v = std::move(val);
    kvs.emplace_back(k, v);
}

void MockSSTGenerator::insert_raw(std::string key, std::string val)
{
    kvs.emplace_back(std::move(key), std::move(val));
}
} // namespace DB
