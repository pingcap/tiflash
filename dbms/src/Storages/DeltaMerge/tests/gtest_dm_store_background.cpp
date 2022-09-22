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

#include <Common/SyncPoint/SyncPoint.h>
#include <Storages/DeltaMerge/tests/gtest_dm_simple_pk_test_basic.h>

#include <future>

namespace DB
{
namespace FailPoints
{
extern const char gc_skip_update_safe_point[];
extern const char skip_check_segment_update[];
} // namespace FailPoints

namespace DM
{
namespace tests
{


class DeltaMergeStoreBackgroundTest
    : public SimplePKTestBasic
{
public:
    void SetUp() override
    {
        FailPointHelper::enableFailPoint(FailPoints::gc_skip_update_safe_point);
        FailPointHelper::enableFailPoint(FailPoints::skip_check_segment_update);
        SimplePKTestBasic::SetUp();
        global_settings_backup = db_context->getGlobalContext().getSettings();
    }

    void TearDown() override
    {
        SimplePKTestBasic::TearDown();
        FailPointHelper::disableFailPoint(FailPoints::skip_check_segment_update);
        FailPointHelper::disableFailPoint(FailPoints::gc_skip_update_safe_point);
        db_context->getGlobalContext().setSettings(global_settings_backup);
    }

protected:
    Settings global_settings_backup;
};


TEST_F(DeltaMergeStoreBackgroundTest, GCWillMergeMultipleSegments)
try
{
    ensureSegmentBreakpoints({0, 10, 40, 100});
    ASSERT_EQ(std::vector<Int64>({0, 10, 40, 100}), getSegmentBreakpoints());

    auto gc_n = store->onSyncGc(1);
    ASSERT_EQ(std::vector<Int64>{}, getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 1);
    ASSERT_EQ(0, getRowsN());
}
CATCH


TEST_F(DeltaMergeStoreBackgroundTest, GCOnlyMergeSmallSegments)
try
{
    UInt64 gc_n = 0;

    ensureSegmentBreakpoints({0, 50, 100, 150, 200});
    fill(-1000, 1000);
    ASSERT_EQ(2000, getRowsN());

    db_context->getGlobalContext().getSettingsRef().dt_segment_limit_rows = 10;
    gc_n = store->onSyncGc(100);
    ASSERT_EQ(std::vector<Int64>({0, 50, 100, 150, 200}), getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 0);

    // In this case, merge two segments will exceed small_segment_rows, so no merge will happen
    db_context->getGlobalContext().getSettingsRef().dt_segment_limit_rows = 55 * 3;
    gc_n = store->onSyncGc(100);
    ASSERT_EQ(std::vector<Int64>({0, 50, 100, 150, 200}), getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 0);

    // In this case, we will only merge two segments and then stop.
    db_context->getGlobalContext().getSettingsRef().dt_segment_limit_rows = 105 * 3;
    gc_n = store->onSyncGc(100);
    ASSERT_EQ(std::vector<Int64>({0, 100, 200}), getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 2);

    gc_n = store->onSyncGc(100);
    ASSERT_EQ(std::vector<Int64>({0, 100, 200}), getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 0);

    ASSERT_EQ(200, getRowsN(0, 200));
    ASSERT_EQ(2000, getRowsN());
}
CATCH


TEST_F(DeltaMergeStoreBackgroundTest, GCMergeAndStop)
try
{
    fill(-1000, 1000);
    flush();
    mergeDelta();

    ensureSegmentBreakpoints({0, 50, 100, 150, 200});

    // In this case, we will only merge two segments and then stop.
    db_context->getGlobalContext().getSettingsRef().dt_segment_limit_rows = 105 * 3;
    auto gc_n = store->onSyncGc(1);
    ASSERT_EQ(std::vector<Int64>({0, 100, 150, 200}), getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 1);

    ASSERT_EQ(200, getRowsN(0, 200));
    ASSERT_EQ(2000, getRowsN());
}
CATCH


TEST_F(DeltaMergeStoreBackgroundTest, GCMergeWhileFlushing)
try
{
    fill(-1000, 1000);

    ensureSegmentBreakpoints({0, 50, 100});

    // Currently, when there is a flush in progress, the segment merge in GC thread will be blocked.

    auto sp_flush_commit = SyncPointCtl::enableInScope("before_ColumnFileFlushTask::commit");
    auto sp_merge_flush_retry = SyncPointCtl::enableInScope("before_DeltaMergeStore::segmentMerge|retry_flush");

    auto th_flush = std::async([&]() {
        // Flush the first segment that GC will touch with.
        flush(-10, 0);
    });

    sp_flush_commit.waitAndPause();

    auto th_gc = std::async([&]() {
        auto gc_n = store->onSyncGc(1);
        ASSERT_EQ(gc_n, 1);
        ASSERT_EQ(store->segments.size(), 1);
    });

    // Expect merge triggered by GC is retrying... because there is a flush in progress.
    sp_merge_flush_retry.waitAndPause();

    // Finish the flush.
    sp_flush_commit.next();
    sp_flush_commit.disable();
    th_flush.wait();

    // The merge in GC should continue without any further retries.
    sp_merge_flush_retry.next();
    th_gc.wait();
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB