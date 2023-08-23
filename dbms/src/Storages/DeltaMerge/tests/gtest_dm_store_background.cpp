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

#include <Common/FailPoint.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/GCOptions.h>
#include <Storages/DeltaMerge/tests/gtest_dm_simple_pk_test_basic.h>

#include <future>
#include <random>

namespace DB
{
namespace FailPoints
{
extern const char skip_check_segment_update[];
} // namespace FailPoints

namespace DM
{
namespace tests
{
class DeltaMergeStoreGCTest : public SimplePKTestBasic
{
public:
    void SetUp() override
    {
        FailPointHelper::enableFailPoint(FailPoints::skip_check_segment_update);
        SimplePKTestBasic::SetUp();
        global_settings_backup = db_context->getGlobalContext().getSettings();
    }

    void TearDown() override
    {
        SimplePKTestBasic::TearDown();
        FailPointHelper::disableFailPoint(FailPoints::skip_check_segment_update);
        db_context->getGlobalContext().setSettings(global_settings_backup);
    }

protected:
    Settings global_settings_backup;
};


class DeltaMergeStoreGCMergeTest : public DeltaMergeStoreGCTest
{
public:
    void SetUp() override
    {
        gc_options = GCOptions::newNoneForTest();
        gc_options.do_merge = true;
        DeltaMergeStoreGCTest::SetUp();
    }

protected:
    GCOptions gc_options{};
};

TEST_F(DeltaMergeStoreGCMergeTest, MergeMultipleSegments)
try
{
    ensureSegmentBreakpoints({0, 10, 40, 100});
    ASSERT_EQ(std::vector<Int64>({0, 10, 40, 100}), getSegmentBreakpoints());

    auto gc_n = store->onSyncGc(1, gc_options);
    ASSERT_EQ(std::vector<Int64>{}, getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 1);
    ASSERT_EQ(0, getRowsN());
}
CATCH


TEST_F(DeltaMergeStoreGCMergeTest, OnlyMergeSmallSegments)
try
{
    UInt64 gc_n = 0;

    ensureSegmentBreakpoints({0, 50, 100, 150, 200});
    fill(-1000, 1000);
    ASSERT_EQ(2000, getRowsN());

    db_context->getGlobalContext().getSettingsRef().dt_segment_limit_rows = 10;
    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(std::vector<Int64>({0, 50, 100, 150, 200}), getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 0);

    // In this case, merge two segments will exceed small_segment_rows, so no merge will happen
    db_context->getGlobalContext().getSettingsRef().dt_segment_limit_rows = 55;
    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(std::vector<Int64>({0, 50, 100, 150, 200}), getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 0);

    // In this case, we will only merge two segments and then stop.
    db_context->getGlobalContext().getSettingsRef().dt_segment_limit_rows = 105;
    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(std::vector<Int64>({0, 100, 200}), getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 2);

    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(std::vector<Int64>({0, 100, 200}), getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 0);

    ASSERT_EQ(200, getRowsN(0, 200));
    ASSERT_EQ(2000, getRowsN());
}
CATCH


TEST_F(DeltaMergeStoreGCMergeTest, MergeAndStop)
try
{
    fill(-1000, 1000);
    flush();
    mergeDelta();

    ensureSegmentBreakpoints({0, 50, 100, 150, 200});

    // In this case, we will only merge two segments and then stop.
    db_context->getGlobalContext().getSettingsRef().dt_segment_limit_rows = 105;
    auto gc_n = store->onSyncGc(1, gc_options);
    ASSERT_EQ(std::vector<Int64>({0, 100, 150, 200}), getSegmentBreakpoints());
    ASSERT_EQ(gc_n, 1);

    ASSERT_EQ(200, getRowsN(0, 200));
    ASSERT_EQ(2000, getRowsN());
}
CATCH


class DeltaMergeStoreGCMergeDeltaTest : public DeltaMergeStoreGCTest
{
public:
    void SetUp() override
    {
        gc_options = GCOptions::newNoneForTest();
        gc_options.do_merge_delta = true;
        DeltaMergeStoreGCTest::SetUp();
    }

protected:
    GCOptions gc_options{};
};


TEST_F(DeltaMergeStoreGCMergeDeltaTest, DeleteRangeInMemTable)
try
{
    fill(100, 600);
    flush();
    mergeDelta();

    deleteRange(-100, 1000);

    auto gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(1, gc_n);
    ASSERT_EQ(0, getSegmentAt(0)->getStable()->getDMFilesRows());
}
CATCH


TEST_F(DeltaMergeStoreGCMergeDeltaTest, AfterLogicalSplit)
try
{
    db_context->getSettingsRef().dt_segment_stable_pack_rows = 107; // for mergeDelta
    db_context->getGlobalContext().getSettingsRef().dt_segment_stable_pack_rows = 107; // for GC

    auto gc_n = store->onSyncGc(1, gc_options);
    ASSERT_EQ(0, gc_n);

    fill(0, 1000);
    flush();
    mergeDelta();

    gc_n = store->onSyncGc(1, gc_options);
    ASSERT_EQ(0, gc_n);

    // Segments that are just logical splited out should not trigger merge delta at all.
    ensureSegmentBreakpoints({500}, /* logical_split */ true);
    gc_n = store->onSyncGc(1, gc_options);
    ASSERT_EQ(0, gc_n);

    ASSERT_EQ(2, store->segments.size());
    ASSERT_EQ(1000, getSegmentAt(0)->getStable()->getDMFilesRows());
    ASSERT_EQ(1000, getSegmentAt(500)->getStable()->getDMFilesRows());

    // Segments that are just logical splited out should not trigger merge delta at all.
    ensureSegmentBreakpoints({150, 500}, /* logical_split */ true);
    gc_n = store->onSyncGc(1, gc_options);
    ASSERT_EQ(0, gc_n);

    ASSERT_EQ(3, store->segments.size());
    ASSERT_EQ(1000, getSegmentAt(0)->getStable()->getDMFilesRows());
    ASSERT_EQ(1000, getSegmentAt(300)->getStable()->getDMFilesRows());
    ASSERT_EQ(1000, getSegmentAt(600)->getStable()->getDMFilesRows());

    // merge delta for right most segment and check again
    mergeDelta(1000, 1001);
    ASSERT_EQ(500, getSegmentAt(600)->getStable()->getDMFilesRows());

    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(1, gc_n);

    ASSERT_EQ(3, store->segments.size());
    ASSERT_EQ(1000, getSegmentAt(0)->getStable()->getDMFilesRows());
    ASSERT_EQ(350, getSegmentAt(300)->getStable()->getDMFilesRows());
    ASSERT_EQ(500, getSegmentAt(600)->getStable()->getDMFilesRows());

    // Trigger GC again, more segments will be merged delta
    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(1, gc_n);

    ASSERT_EQ(3, store->segments.size());
    ASSERT_EQ(150, getSegmentAt(0)->getStable()->getDMFilesRows());
    ASSERT_EQ(350, getSegmentAt(300)->getStable()->getDMFilesRows());
    ASSERT_EQ(500, getSegmentAt(600)->getStable()->getDMFilesRows());

    // Trigger GC again, no more merge delta.
    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(0, gc_n);
    ASSERT_EQ(3, store->segments.size());
}
CATCH


TEST_F(DeltaMergeStoreGCMergeDeltaTest, SegmentContainsPack)
try
{
    ensureSegmentBreakpoints({400, 500});
    fill(410, 450);
    flush();
    mergeDelta();

    auto gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(0, gc_n);
}
CATCH


TEST_F(DeltaMergeStoreGCMergeDeltaTest, SegmentExactlyContainsStable)
try
{
    ensureSegmentBreakpoints({400, 500});
    fill(100, 600);
    flush();
    mergeDelta();

    auto gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(0, gc_n);
}
CATCH


TEST_F(DeltaMergeStoreGCMergeDeltaTest, NoPacks)
try
{
    db_context->getSettingsRef().dt_segment_stable_pack_rows = 1;
    db_context->getGlobalContext().getSettingsRef().dt_segment_stable_pack_rows = 1;

    ensureSegmentBreakpoints({100, 200, 300});

    auto gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(0, gc_n);
}
CATCH


TEST_F(DeltaMergeStoreGCMergeDeltaTest, SegmentContainedByPack)
try
{
    for (auto pack_size : {7, 200})
    {
        reload();
        db_context->getSettingsRef().dt_segment_stable_pack_rows = pack_size; // for mergeDelta
        db_context->getGlobalContext().getSettingsRef().dt_segment_stable_pack_rows = pack_size; // for GC

        fill(0, 200);
        flush();
        mergeDelta();

        auto pack_n = static_cast<size_t>(std::ceil(200.0 / static_cast<double>(pack_size)));
        ASSERT_EQ(pack_n, getSegmentAt(0)->getStable()->getDMFilesPacks());

        auto gc_n = store->onSyncGc(100, gc_options);
        ASSERT_EQ(0, gc_n);

        ensureSegmentBreakpoints({10, 190}, /* logical_split */ true);
        gc_n = store->onSyncGc(100, gc_options);
        ASSERT_EQ(0, gc_n);

        mergeDelta(0, 1);
        mergeDelta(190, 191);

        ASSERT_EQ(10, getSegmentAt(0)->getStable()->getDMFilesRows());
        ASSERT_EQ(10, getSegmentAt(190)->getStable()->getDMFilesRows());

        ASSERT_EQ(pack_n, getSegmentAt(50)->getStable()->getDMFilesPacks());
        ASSERT_EQ(200, getSegmentAt(50)->getStable()->getDMFilesRows());

        if (pack_size == 200)
        {
            // The segment [10, 190) only overlaps with 1 pack and is contained by the pack.
            // Even it contains most of the data, it will still be GCed.
            gc_n = store->onSyncGc(50, gc_options);
            ASSERT_EQ(1, gc_n);
            ASSERT_EQ(1, getSegmentAt(150)->getStable()->getDMFilesPacks());
            ASSERT_EQ(180, getSegmentAt(150)->getStable()->getDMFilesRows());

            // There should be no more GCs.
            gc_n = store->onSyncGc(100, gc_options);
            ASSERT_EQ(0, gc_n);
        }
        else if (pack_size == 7)
        {
            // When pack size is small, we will more precisely know that most of the DTFile is still valid.
            // So in this case, no GC will happen.
            gc_n = store->onSyncGc(50, gc_options);
            ASSERT_EQ(0, gc_n);
        }
        else
        {
            FAIL();
        }
    }
}
CATCH


TEST_F(DeltaMergeStoreGCMergeDeltaTest, SmallReclaimRatioDoesNotMergeDelta)
try
{
    db_context->getSettingsRef().dt_segment_stable_pack_rows = 7;
    db_context->getGlobalContext().getSettingsRef().dt_segment_stable_pack_rows = 7;

    fill(0, 400);
    flush();
    mergeDelta();

    auto gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(0, gc_n);

    ensureSegmentBreakpoints({10}, /* logical_split */ true);
    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(0, gc_n);

    mergeDelta(0, 1);
    ASSERT_EQ(10, getSegmentAt(0)->getStable()->getDMFilesRows());
    ASSERT_EQ(400, getSegmentAt(150)->getStable()->getDMFilesRows());

    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(0, gc_n);
    ASSERT_EQ(10, getSegmentAt(0)->getStable()->getDMFilesRows());
    ASSERT_EQ(400, getSegmentAt(150)->getStable()->getDMFilesRows());
}
CATCH


TEST_F(DeltaMergeStoreGCMergeDeltaTest, SimpleBigReclaimRatio)
try
{
    db_context->getSettingsRef().dt_segment_stable_pack_rows = 7;
    db_context->getGlobalContext().getSettingsRef().dt_segment_stable_pack_rows = 7;

    fill(0, 400);
    flush();
    mergeDelta();

    auto gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(0, gc_n);

    ensureSegmentBreakpoints({10}, /* logical_split */ true);
    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(0, gc_n);

    mergeDelta(100, 101);
    ASSERT_EQ(400, getSegmentAt(0)->getStable()->getDMFilesRows());
    ASSERT_EQ(390, getSegmentAt(150)->getStable()->getDMFilesRows());

    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(1, gc_n);
    ASSERT_EQ(10, getSegmentAt(0)->getStable()->getDMFilesRows());
    ASSERT_EQ(390, getSegmentAt(150)->getStable()->getDMFilesRows());

    // GC again does not introduce new changes
    gc_n = store->onSyncGc(100, gc_options);
    ASSERT_EQ(0, gc_n);
    ASSERT_EQ(10, getSegmentAt(0)->getStable()->getDMFilesRows());
    ASSERT_EQ(390, getSegmentAt(150)->getStable()->getDMFilesRows());
}
CATCH


// This test enables GC merge and GC merge delta.
TEST_F(DeltaMergeStoreGCTest, RandomShuffleLogicalSplitAndDeleteRange)
try
{
    auto gc_options = GCOptions::newAllForTest();
    // TODO: Better to be fuzz tests, in order to reach edge cases efficiently.

    std::random_device rd;
    std::mt19937 random(rd());

    for (auto pack_size : {1, 7, 10, 200})
    {
        for (size_t random_round = 0; random_round < 10; random_round++)
        {
            LOG_INFO(logger, "Run round #{} for pack_size = {}", random_round, pack_size);

            // For each pack_size, we randomize N rounds. We should always expect everything are
            // reclaimed in each round.

            reload();
            db_context->getSettingsRef().dt_segment_stable_pack_rows = pack_size; // for mergeDelta
            db_context->getGlobalContext().getSettingsRef().dt_segment_stable_pack_rows = pack_size; // for GC

            fill(0, 100);
            fill(500, 600);
            flush();
            mergeDelta();

            auto operations = std::vector<std::function<void()>>{
                [&] { ensureSegmentBreakpoints({50}, true); },
                [&] { ensureSegmentBreakpoints({80}, true); },
                [&] { ensureSegmentBreakpoints({100}, true); },
                [&] { ensureSegmentBreakpoints({300}, true); },
                [&] { ensureSegmentBreakpoints({700}, true); },
                [&] { deleteRange(-10, 30); },
                [&] { deleteRange(30, 70); },
                [&] { deleteRange(70, 100); },
                [&] { deleteRange(400, 500); },
                [&] { deleteRange(500, 600); },
            };

            std::shuffle(std::begin(operations), std::end(operations), random);

            for (const auto & op : operations)
            {
                op();

                // There will be also a change to randomly merge some delta.
                auto merge_delta_ops = std::uniform_int_distribution<size_t>(0, 2)(random);
                for (size_t i = 0; i < merge_delta_ops; i++)
                {
                    auto merge_delta_at = std::uniform_int_distribution<Int64>(0, 700)(random);
                    mergeDelta(merge_delta_at, merge_delta_at + 1);
                }
            }

            // Finally, let's do GCs. We should expect everything are reclaimed within 10 rounds of GC.
            for (size_t gc_round = 0; gc_round < 10; gc_round++)
                store->onSyncGc(100, gc_options);

            // No more GCs are needed.
            ASSERT_EQ(0, store->onSyncGc(100, gc_options));

            // Check whether we have reclaimed everything
            ASSERT_EQ(store->segments.size(), 1);
            ASSERT_EQ(getSegmentAt(0)->getStable()->getDMFilesPacks(), 0);
        }
    }
}
CATCH


} // namespace tests
} // namespace DM
} // namespace DB