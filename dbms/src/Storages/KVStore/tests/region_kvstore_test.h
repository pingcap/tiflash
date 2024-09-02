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

#pragma once

#include <Storages/DeltaMerge/Decode/SSTFilesToBlockInputStream.h>
#include <Storages/KVStore/tests/kvstore_helper.h>

namespace DB::tests
{

class RegionKVStoreTest : public KVStoreTestBase
{
public:
    RegionKVStoreTest()
    {
        log = DB::Logger::get("RegionKVStoreTest");
        test_path = TiFlashTestEnv::getTemporaryPath("/region_kvs_test");
    }

    static RegionPtr splitRegion(const RegionPtr & region, RegionMeta && meta)
    {
        return region->splitInto(std::move(meta));
    }

    static void dropTable(Context & ctx, TableID table_id);

    static void recordThreadAllocInfoForProxy(const JointThreadInfoJeallocMapPtr & m)
    {
        m->recordThreadAllocInfoForProxy();
    }
};

inline void validateSSTGeneration(
    KVStore & kvs,
    std::unique_ptr<MockRaftStoreProxy> & proxy_instance,
    UInt64 region_id,
    MockSSTGenerator & cf_data,
    ColumnFamilyType cf,
    int sst_size,
    int key_count)
{
    auto kvr1 = kvs.getRegion(region_id);
    auto r1 = proxy_instance->getRegion(region_id);
    auto proxy_helper = proxy_instance->generateProxyHelper();
    auto ssts = cf_data.ssts();
    ASSERT_EQ(ssts.size(), sst_size);
    auto make_inner_func = [](const TiFlashRaftProxyHelper * proxy_helper,
                              SSTView snap,
                              SSTReader::RegionRangeFilter range,
                              const LoggerPtr & log_) -> std::unique_ptr<MonoSSTReader> {
        auto parsed_kind = MockSSTGenerator::parseSSTViewKind(buffToStrView(snap.path));
        auto reader = std::make_unique<MonoSSTReader>(proxy_helper, snap, range, log_);
        assert(reader->sstFormatKind() == parsed_kind);
        return reader;
    };
    MultiSSTReader<MonoSSTReader, SSTView> reader{
        proxy_helper.get(),
        cf,
        make_inner_func,
        ssts,
        Logger::get(),
        kvr1->getRange(),
    };

    size_t counter = 0;
    while (reader.remained())
    {
        // repeatedly remained are called.
        reader.remained();
        reader.remained();
        counter++;
        auto v = std::string(reader.valueView().data);
        ASSERT_EQ(v, "v" + std::to_string(counter));
        reader.next();
    }
    ASSERT_EQ(counter, key_count);
}

ASTPtr parseCreateStatement(const String & statement);
TableID createDBAndTable(String db_name, String table_name);
void dropDataBase(String db_name);

} // namespace DB::tests
