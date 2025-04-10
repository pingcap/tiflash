// Copyright 2024 PingCAP, Inc.
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

#include <Common/SyncPoint/Ctl.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/StoragePool/GlobalPageIdAllocator.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <future>

namespace DB::DM::tests
{

TEST(GlobalPageIdAllocatorTest, Normal)
{
    GlobalPageIdAllocator allocator;

    // Note the meta page id is allocated begin with 2
    // 1 is reserved for `DELTA_MERGE_FIRST_SEGMENT_ID` for each
    // physical table
    EXPECT_EQ(allocator.newMetaPageId(), 2);
    EXPECT_EQ(allocator.newMetaPageId(), 3);
    EXPECT_EQ(allocator.newMetaPageId(), 4);
    allocator.raiseMetaPageIdLowerBound(1024);
    EXPECT_EQ(allocator.newMetaPageId(), 1025);
    EXPECT_EQ(allocator.newMetaPageId(), 1026);
    EXPECT_EQ(allocator.newMetaPageId(), 1027);

    EXPECT_EQ(allocator.newLogPageId(), 1);
    EXPECT_EQ(allocator.newLogPageId(), 2);
    EXPECT_EQ(allocator.newLogPageId(), 3);
    allocator.raiseLogPageIdLowerBound(65536);
    EXPECT_EQ(allocator.newLogPageId(), 65537);
    EXPECT_EQ(allocator.newLogPageId(), 65538);

    EXPECT_EQ(allocator.newDataPageIdForDTFile(), 1);
    EXPECT_EQ(allocator.newDataPageIdForDTFile(), 2);
    EXPECT_EQ(allocator.newDataPageIdForDTFile(), 3);
    allocator.raiseDataPageIdLowerBound(114);
    EXPECT_EQ(allocator.newDataPageIdForDTFile(), 115);
    EXPECT_EQ(allocator.newDataPageIdForDTFile(), 116);
}

TEST(GlobalPageIdAllocatorTest, IdChangedBeforeRaiseLowerBound)
{
    GlobalPageIdAllocator allocator;

    EXPECT_EQ(allocator.newLogPageId(), 1);
    EXPECT_EQ(allocator.newLogPageId(), 2);
    EXPECT_EQ(allocator.newLogPageId(), 3);

    auto sp_raise_bound = SyncPointCtl::enableInScope("before_GlobalPageIdAllocator::raiseLowerBoundCAS_1");
    auto th_raise_lower_bound = std::async([&]() {
        allocator.raiseLogPageIdLowerBound(1024);
        EXPECT_EQ(allocator.newLogPageId(), 1025);
    });

    sp_raise_bound.waitAndPause();
    EXPECT_EQ(allocator.newLogPageId(), 4);
    sp_raise_bound.next();
    EXPECT_EQ(allocator.newLogPageId(), 5);

    // use `disable` instead of `next` to break the loop inside `raiseTargetByLowerBound`
    sp_raise_bound.disable();
    th_raise_lower_bound.get();
    EXPECT_EQ(allocator.newLogPageId(), 1026);
}

TEST(GlobalPageIdAllocatorTest, IdRaisedBeforeRaiseLowerBound)
{
    GlobalPageIdAllocator allocator;

    EXPECT_EQ(allocator.newLogPageId(), 1);
    EXPECT_EQ(allocator.newLogPageId(), 2);
    EXPECT_EQ(allocator.newLogPageId(), 3);

    auto sp_raise_bound = SyncPointCtl::enableInScope("before_GlobalPageIdAllocator::raiseLowerBoundCAS_1");
    auto th_raise_lower_bound_1 = std::async([&]() { allocator.raiseLogPageIdLowerBound(1024); });
    auto th_raise_lower_bound_2 = std::async([&]() { allocator.raiseLogPageIdLowerBound(3000); });

    sp_raise_bound.waitAndPause();

    // use `disable` instead of `next` to break the loop inside `raiseTargetByLowerBound`
    sp_raise_bound.disable();

    th_raise_lower_bound_2.get();
    EXPECT_EQ(allocator.newLogPageId(), 3001);
    th_raise_lower_bound_1.get();
    EXPECT_EQ(allocator.newLogPageId(), 3002);
}

} // namespace DB::DM::tests
