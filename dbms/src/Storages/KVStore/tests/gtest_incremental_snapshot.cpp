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

#include <Storages/KVStore/MultiRaft/Disagg/IncrementalSnapshot.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/tests/region_kvstore_test.h>

namespace DB
{
namespace tests
{

TEST_F(RegionKVStoreTest, IncrementalSnapshotManager)
{
    IncrementalSnapshotManager mgr;
    auto & ctx = TiFlashTestEnv::getGlobalContext();
    KVStore & kvs = getKVS();
    auto region_id = 1;
    proxy_instance->debugAddRegions(
        kvs,
        ctx.getTMTContext(),
        {1},
        {{{RecordKVFormat::genKey(1, 0), RecordKVFormat::genKey(1, 10)}}});
    auto kvr1 = kvs.getRegion(region_id);

    std::vector<UInt64> at_10 = {1, 2};
    std::vector<UInt64> at_20 = {1, 2, 3, 4};
    std::vector<UInt64> at_30 = {2, 3, 4, 5};

    mgr.observeDeltaSummary(10, kvr1, at_10.data(), at_10.size());
    auto v = mgr.tryReuseDeltaSummary(20, kvr1, at_20.data(), at_20.size());
    ASSERT_NE(v, std::nullopt);
    ASSERT_EQ(v.value().ids.size(), 2);
    v = mgr.tryReuseDeltaSummary(30, kvr1, at_30.data(), at_30.size());
    ASSERT_EQ(v, std::nullopt);
}

} // namespace tests
} // namespace DB
