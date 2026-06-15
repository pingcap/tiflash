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

#include <Flash/Coprocessor/RequestUtils.h>
#include <Interpreters/Context.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/StorageDisaggregatedHelpers.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <kvproto/disaggregated.pb.h>
#include <kvproto/kvrpcpb.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Backoff.h>

namespace DB::tests
{

struct TestRegionInfo
{
    pingcap::kv::RegionVerID region_id;
    pingcap::coprocessor::KeyRanges ranges;
};

// A simple mock class for `dropRegion`
struct MockRegionCache
{
    std::vector<pingcap::kv::RegionVerID> dropped_id;

    void dropRegion(const pingcap::kv::RegionVerID & region_id) { dropped_id.emplace_back(region_id); }
};

class StorageDisaggregatedHelpersTest : public testing::Test
{
public:
    StorageDisaggregatedHelpersTest()
        : log(Logger::get())
    {}

protected:
    void SetUp() override {}

    static std::shared_ptr<disaggregated::EstablishDisaggTaskRequest> genRequest(
        const std::vector<TestRegionInfo> test_regions)
    {
        // Generate BatchCopTask
        pingcap::coprocessor::BatchCopTask task;
        task.store_type = pingcap::kv::StoreType::TiFlash;
        task.store_id = 12345;
        for (const auto & region : test_regions)
        {
            task.region_infos.emplace_back(pingcap::coprocessor::RegionInfo{
                .region_id = region.region_id,
                .ranges = region.ranges,
            });
        }

        auto req = std::make_shared<disaggregated::EstablishDisaggTaskRequest>();
        RequestUtils::setUpRegionInfos(task, req);
        return req;
    }

    LoggerPtr log;
};

TEST_F(StorageDisaggregatedHelpersTest, DropRegionCache)
{
    using namespace pingcap::kv;
    using namespace pingcap::coprocessor;

    auto region_cache = std::make_unique<MockRegionCache>();
    auto req = genRequest({
        TestRegionInfo{.region_id = RegionVerID{23917, 9, 98}, .ranges = KeyRanges{KeyRange("a", "b")}},
        TestRegionInfo{.region_id = RegionVerID{20, 3, 10}, .ranges = KeyRanges{KeyRange("d", "e")}},
    });

    dropRegionCache(region_cache, req, {23917});
    ASSERT_EQ(region_cache->dropped_id.size(), 1);
    ASSERT_EQ(region_cache->dropped_id[0].id, 23917);
    ASSERT_EQ(region_cache->dropped_id[0].conf_ver, 9);
    ASSERT_EQ(region_cache->dropped_id[0].ver, 98);

    region_cache->dropped_id.clear();
    dropRegionCache(region_cache, req, {20});
    ASSERT_EQ(region_cache->dropped_id.size(), 1);
    ASSERT_EQ(region_cache->dropped_id[0].id, 20);
    ASSERT_EQ(region_cache->dropped_id[0].conf_ver, 3);
    ASSERT_EQ(region_cache->dropped_id[0].ver, 10);
}

TEST_F(StorageDisaggregatedHelpersTest, DropRegionCacheWithDuplicateID)
{
    using namespace pingcap::kv;
    using namespace pingcap::coprocessor;

    auto region_cache = std::make_unique<MockRegionCache>();
    auto req = genRequest({
        TestRegionInfo{.region_id = RegionVerID{23917, 9, 100}, .ranges = KeyRanges{KeyRange("a", "d")}},
        TestRegionInfo{.region_id = RegionVerID{23917, 9, 98}, .ranges = KeyRanges{KeyRange("a", "b")}},
        TestRegionInfo{.region_id = RegionVerID{20, 3, 10}, .ranges = KeyRanges{KeyRange("d", "e")}},
    });

    dropRegionCache(region_cache, req, {23917});

    ASSERT_EQ(region_cache->dropped_id.size(), 2);
    ASSERT_EQ(region_cache->dropped_id[0].id, 23917);
    ASSERT_EQ(region_cache->dropped_id[0].conf_ver, 9);
    ASSERT_EQ(region_cache->dropped_id[0].ver, 100);
    ASSERT_EQ(region_cache->dropped_id[1].id, 23917);
    ASSERT_EQ(region_cache->dropped_id[1].conf_ver, 9);
    ASSERT_EQ(region_cache->dropped_id[1].ver, 98);
}

TEST_F(StorageDisaggregatedHelpersTest, MakeLocksForDisaggResolveKeepsNormalLock)
{
    // Baseline for the shared-lock guard: ordinary lock errors are still
    // converted to client-c locks and can continue through lock resolution.
    kvrpcpb::LockInfo lock_info;
    lock_info.set_key("row-key");
    lock_info.set_primary_lock("primary-key");
    lock_info.set_lock_version(42);
    lock_info.set_lock_ttl(3000);
    lock_info.set_lock_type(kvrpcpb::Op::Put);

    google::protobuf::RepeatedPtrField<kvrpcpb::LockInfo> lock_infos;
    lock_infos.Add()->CopyFrom(lock_info);

    auto locks = makeLocksForDisaggResolve(lock_infos);
    ASSERT_EQ(locks.size(), 1);
    ASSERT_EQ(locks[0]->key, "row-key");
    ASSERT_EQ(locks[0]->primary, "primary-key");
    ASSERT_EQ(locks[0]->txn_id, 42);
    ASSERT_EQ(locks[0]->ttl, 3000);
    ASSERT_EQ(locks[0]->lock_type, kvrpcpb::Op::Put);
}

TEST_F(StorageDisaggregatedHelpersTest, MakeLocksForDisaggResolveRejectsSharedLockWrapper)
{
    // A SharedLock LockInfo is a wrapper around holder locks, not a resolvable
    // lock for TiFlash reads. If one reaches this disaggregated resolver
    // boundary, fail fast instead of interpreting the wrapper's invalid txn
    // fields as a real pessimistic lock.
    kvrpcpb::LockInfo shared_lock_wrapper;
    shared_lock_wrapper.set_key("row-key");
    shared_lock_wrapper.set_lock_type(kvrpcpb::Op::SharedLock);

    auto * holder = shared_lock_wrapper.add_shared_lock_infos();
    holder->set_key("row-key");
    holder->set_primary_lock("holder-primary");
    holder->set_lock_version(101);
    holder->set_lock_ttl(5000);
    holder->set_lock_type(kvrpcpb::Op::Lock);

    try
    {
        auto lock = makeLockForDisaggResolve(shared_lock_wrapper);
        (void)lock;
        FAIL() << "SharedLock wrappers must not reach TiFlash disaggregated lock resolution";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
        ASSERT_NE(std::string(e.message()).find("Unexpected SharedLock"), std::string::npos);
    }
}

} // namespace DB::tests
