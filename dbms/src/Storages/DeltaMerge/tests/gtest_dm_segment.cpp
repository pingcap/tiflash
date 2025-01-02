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

#include <Common/CurrentMetrics.h>
#include <Common/SyncPoint/SyncPoint.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/ColumnDefine_fwd.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/Segment_fwd.h>
#include <Storages/DeltaMerge/StoragePool/GlobalPageIdAllocator.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/WriteBatchesImpl.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_dm_simple_pk_test_basic.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/PathPool.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>

#include <ctime>
#include <future>
#include <memory>


namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfRead;
extern const Metric DT_SnapshotOfReadRaw;
extern const Metric DT_SnapshotOfSegmentSplit;
extern const Metric DT_SnapshotOfSegmentMerge;
extern const Metric DT_SnapshotOfDeltaMerge;
extern const Metric DT_SnapshotOfPlaceIndex;
} // namespace CurrentMetrics

namespace DB::DM
{
extern DMFilePtr writeIntoNewDMFile(
    DMContext & dm_context, //
    const ColumnDefinesPtr & schema_snap,
    const BlockInputStreamPtr & input_stream,
    UInt64 file_id,
    const String & parent_path);
}

namespace DB::DM::tests
{
class SegmentTest : public DB::base::TiFlashStorageTestBasic
{
public:
    SegmentTest() = default;

public:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        table_columns = std::make_shared<ColumnDefines>();

        segment = buildFirstSegment();
        ASSERT_EQ(segment->segmentId(), DELTA_MERGE_FIRST_SEGMENT_ID);
    }

protected:
    SegmentPtr buildFirstSegment(
        const ColumnDefinesPtr & pre_define_columns = {},
        DB::Settings && db_settings = DB::Settings())
    {
        TiFlashStorageTestBasic::reload(std::move(db_settings));
        storage_path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        page_id_allocator = std::make_shared<GlobalPageIdAllocator>();
        storage_pool = std::make_shared<StoragePool>(
            *db_context,
            NullspaceID,
            /*ns_id*/ 100,
            *storage_path_pool,
            page_id_allocator,
            "test.t1");
        storage_pool->restore();
        ColumnDefinesPtr cols = (!pre_define_columns) ? DMTestEnv::getDefaultColumns() : pre_define_columns;
        setColumns(cols);

        return Segment::newSegment(
            Logger::get(),
            *dm_context,
            table_columns,
            RowKeyRange::newAll(false, 1),
            DELTA_MERGE_FIRST_SEGMENT_ID,
            0);
    }

    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefinesPtr & columns)
    {
        *table_columns = *columns;

        dm_context = DMContext::createUnique(
            *db_context,
            storage_path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            /*physical_table_id*/ 100,
            /*pk_col_id*/ 0,
            false,
            1,
            db_context->getSettingsRef());
    }

    const ColumnDefinesPtr & tableColumns() const { return table_columns; }

    DMContext & dmContext() { return *dm_context; }

protected:
    /// all these var lives as ref in dm_context
    GlobalPageIdAllocatorPtr page_id_allocator;
    std::shared_ptr<StoragePathPool> storage_path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context;

    // the segment we are going to test
    SegmentPtr segment;
};

TEST_F(SegmentTest, WriteRead)
try
{
    const size_t num_rows_write = 100;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        // write to segment
        segment->write(dmContext(), block);
        // estimate segment
        auto estimated_rows = segment->getEstimatedRows();
        ASSERT_EQ(estimated_rows, block.rows());

        auto estimated_bytes = segment->getEstimatedBytes();
        ASSERT_EQ(estimated_bytes, block.bytes());
    }

    {
        // check segment
        segment->check(dmContext(), "test");
    }

    { // Round 1
        {
            // read written data (only in delta)
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext(), tableColumns());
        }

        {
            // read written data (only in stable)
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
        }
    }

    const size_t num_rows_write_2 = 55;

    {
        // write more rows to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write, num_rows_write + num_rows_write_2, false);
        segment->write(dmContext(), std::move(block));
    }

    { // Round 2
        {
            // read written data (both in delta and stable)
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            ASSERT_INPUTSTREAM_NROWS(in, num_rows_write + num_rows_write_2);
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext(), tableColumns());
        }

        {
            // read written data (only in stable)
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            ASSERT_INPUTSTREAM_NROWS(in, num_rows_write + num_rows_write_2);
        }
    }
}
CATCH


TEST_F(SegmentTest, ClipBlockRows)
try
{
    constexpr auto num_rows_write = 100;
    auto block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
    segment->write(dmContext(), block);
    auto row_bytes = segment->stable->avgRowBytes(*tableColumns());
    ASSERT_EQ(row_bytes, 0);

    constexpr size_t pack_rows = 512;

    auto clipped_block_rows = Segment::clipBlockRows(
        /*max_block_bytes*/ 1024,
        pack_rows,
        /*max_block_rows*/ 1023,
        *tableColumns(),
        segment->stable);
    ASSERT_EQ(clipped_block_rows, 1023);

    clipped_block_rows = Segment::clipBlockRows(
        /*max_block_bytes*/ 1024,
        pack_rows,
        /*max_block_rows*/ 102400,
        *tableColumns(),
        segment->stable);
    ASSERT_EQ(clipped_block_rows, 1024);

    segment = segment->mergeDelta(dmContext(), tableColumns());
    row_bytes = segment->stable->avgRowBytes(*tableColumns());
    ASSERT_EQ(row_bytes, 17);

    clipped_block_rows = Segment::clipBlockRows(
        /*max_block_bytes*/ 1023,
        pack_rows,
        /*max_block_rows*/ 1024,
        *tableColumns(),
        segment->stable);
    ASSERT_EQ(clipped_block_rows, pack_rows);

    clipped_block_rows = Segment::clipBlockRows(
        /*max_block_bytes*/ 102400,
        pack_rows,
        /*max_block_rows*/ 1024,
        *tableColumns(),
        segment->stable);
    ASSERT_EQ(clipped_block_rows, 1024);

    clipped_block_rows = Segment::clipBlockRows(
        /*max_block_bytes*/ 4096,
        pack_rows,
        /*max_block_rows*/ 3000,
        *tableColumns(),
        segment->stable);
    ASSERT_EQ(clipped_block_rows, pack_rows);

    clipped_block_rows
        = Segment::clipBlockRows(/*max_block_bytes*/ 1023, pack_rows, /*max_block_rows*/ 1024, {}, segment->stable);
    ASSERT_EQ(clipped_block_rows, pack_rows);

    clipped_block_rows
        = Segment::clipBlockRows(/*max_block_bytes*/ 102400, pack_rows, /*max_block_rows*/ 1024, {}, segment->stable);
    ASSERT_EQ(clipped_block_rows, 1024);
}
CATCH

TEST_F(SegmentTest, WriteRead2)
try
{
    const size_t num_rows_write = dmContext().stable_pack_rows;
    {
        // write a block with rows all deleted
        Block block = DMTestEnv::prepareBlockWithTso(2, 100, 100 + num_rows_write, false, true);
        segment->write(dmContext(), block);
        // write not deleted rows with larger pk
        Block block2 = DMTestEnv::prepareBlockWithTso(3, 100, 100 + num_rows_write, false, false);
        segment->write(dmContext(), block2);

        // flush segment and make sure there is two packs in stable
        segment = segment->mergeDelta(dmContext(), tableColumns());
        ASSERT_EQ(segment->getStable()->getDMFilesPacks(), 2);
    }

    {
        Block block = DMTestEnv::prepareBlockWithTso(1, 100, 100 + num_rows_write, false, false);
        segment->write(dmContext(), block);
    }

    {
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        // only write two visible pks
        ASSERT_INPUTSTREAM_NROWS(in, 2);
    }
}
CATCH

TEST_F(SegmentTest, WriteReadMultiRange)
try
{
    const size_t num_rows_write = 100;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        // write to segment
        segment->write(dmContext(), block);
        // estimate segment
        auto estimated_rows = segment->getEstimatedRows();
        ASSERT_EQ(estimated_rows, block.rows());

        auto estimated_bytes = segment->getEstimatedBytes();
        ASSERT_EQ(estimated_bytes, block.bytes());
    }

    {
        // check segment
        segment->check(dmContext(), "test");
    }

    RowKeyRanges read_ranges;
    read_ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(0, 10)));
    read_ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(20, 30)));
    read_ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(110, 130)));
    const size_t expect_read_rows = 20;
    { // Round 1
        {
            // read written data (only in delta)
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), read_ranges);
            ASSERT_INPUTSTREAM_NROWS(in, expect_read_rows);
        }

        {
            // flush segment
            ASSERT_EQ(segment->getDelta()->getRows(), num_rows_write);
            segment = segment->mergeDelta(dmContext(), tableColumns());
            ASSERT_EQ(segment->getStable()->getRows(), num_rows_write);
        }

        {
            // read written data (only in stable)
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), read_ranges);
            ASSERT_INPUTSTREAM_NROWS(in, expect_read_rows);
        }
    }

    const size_t num_rows_write_2 = 55;
    const size_t expect_read_rows_2 = 40;

    {
        // write more rows to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write, num_rows_write + num_rows_write_2, false);
        segment->write(dmContext(), std::move(block));
        ASSERT_EQ(segment->getDelta()->getRows(), num_rows_write_2);
    }

    { // Round 2
        {
            // read written data (both in delta and stable)
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), read_ranges);
            ASSERT_INPUTSTREAM_NROWS(in, expect_read_rows_2);
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext(), tableColumns());
            ASSERT_EQ(segment->getStable()->getRows(), num_rows_write + num_rows_write_2);
        }

        {
            // read written data (only in stable)
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), read_ranges);
            ASSERT_INPUTSTREAM_NROWS(in, expect_read_rows_2);
        }
    }
}
CATCH

TEST_F(SegmentTest, ReadWithMoreAdvacedDeltaIndex)
try
{
    // Test the case that reading rows with an advance DeltaIndex
    //  1. Thread A creates a delta snapshot with 100 rows.
    //  2. Thread B inserts 100 rows into the delta
    //  3. Thread B reads and place 200 rows to a new DeltaTree, and update the `shared_delta_index` to 200
    //  4. Thread A read with an DeltaTree that only placed 100 rows but `placed_rows` in `shared_delta_index` with 200
    //  5. Thread A use the DeltaIndex with placed_rows = 200 to do the merge in DeltaMergeBlockInputStream
    size_t offset = 0;
    auto write_rows = [&](size_t rows) {
        Block block = DMTestEnv::prepareSimpleWriteBlock(offset, offset + rows, false);
        offset += rows;
        // write to segment
        segment->write(dmContext(), block);
    };

    {
        // check segment
        segment->check(dmContext(), "test");
    }

    // Thread A
    write_rows(100);
    ASSERT_INPUTSTREAM_NROWS(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)}),
        100);
    auto snap = segment->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);

    // Thread B
    write_rows(100);
    ASSERT_INPUTSTREAM_NROWS(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)}),
        200);

    // Thread A
    {
        auto in = segment->getInputStreamModeNormal(
            dmContext(),
            *tableColumns(),
            snap,
            {RowKeyRange::newAll(false, 1)},
            {},
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE);
        ASSERT_INPUTSTREAM_NROWS(in, 100);
    }
}
CATCH

TEST_F(SegmentTest, ReadWithMoreAdvacedDeltaIndex2)
try
{
    auto write_rows = [&](size_t offset, size_t rows) {
        Block block = DMTestEnv::prepareSimpleWriteBlock(offset, offset + rows, false);
        // write to segment
        segment->write(dmContext(), block);
    };

    // Thread A
    write_rows(0, 100);
    ASSERT_INPUTSTREAM_NROWS(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)}),
        100);
    auto snap = segment->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);

    {
        // check segment
        segment->check(dmContext(), "test");
    }

    // Thread B
    write_rows(0, 100);
    ASSERT_INPUTSTREAM_NROWS(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)}),
        100);

    // Thread A
    {
        auto in = segment->getInputStreamModeNormal(
            dmContext(),
            *tableColumns(),
            snap,
            {RowKeyRange::newAll(false, 1)},
            {},
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE);
        ASSERT_INPUTSTREAM_NROWS(in, 100);
    }
}
CATCH

TEST_F(SegmentTest, ReadWithMoreAdvacedDeltaIndexWithDeleteRange01)
try
{
    auto write_rows = [&](size_t offset, size_t rows) {
        Block block = DMTestEnv::prepareSimpleWriteBlock(offset, offset + rows, false);
        // write to segment
        segment->write(dmContext(), block);
    };

    // Thread A write [0, 100) && [100, 200)
    write_rows(0, 100);
    write_rows(100, 100);
    ASSERT_INPUTSTREAM_NROWS(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)}),
        200);
    // check segment
    segment->check(dmContext(), "test");
    auto snap_a = segment->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);

    // Thread B delete range [0, 50)
    RowKeyRange range = RowKeyRange::fromHandleRange(HandleRange(0, 50));
    segment->write(dmContext(), range);
    LOG_INFO(Logger::get(), "Thread B read");
    ASSERT_INPUTSTREAM_NROWS(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)}),
        150);

    {
        // tryClone will return an empty delta-index because `shared_delta_index.placed_deletes != delta->getDeletes()`
        auto my_delta_index
            = snap_a->delta->getSharedDeltaIndex()->tryClone(snap_a->delta->getRows(), snap_a->delta->getDeletes());
        auto [my_placed_rows, my_placed_deletes] = my_delta_index->getPlacedStatus();
        ASSERT_EQ(my_placed_rows, 0);
        ASSERT_EQ(my_placed_deletes, 0);
    }

    // Thread A read [0, 200)
    {
        LOG_INFO(Logger::get(), "Thread A read with snap_a");
        auto in = segment->getInputStreamModeNormal(
            dmContext(),
            *tableColumns(),
            snap_a,
            {RowKeyRange::newAll(false, 1)},
            {},
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE);
        ASSERT_INPUTSTREAM_NROWS(in, 200);
    }
}
CATCH


TEST_F(SegmentTest, ReadWithMoreAdvacedDeltaIndexWithDeleteRange02)
try
{
    auto write_rows = [&](size_t offset, size_t rows) {
        Block block = DMTestEnv::prepareSimpleWriteBlock(offset, offset + rows, false);
        // write to segment
        segment->write(dmContext(), block);
    };

    // Thread A write [0, 100)
    write_rows(0, 100);
    ASSERT_INPUTSTREAM_NROWS(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)}),
        100);
    // check segment
    segment->check(dmContext(), "test");
    auto snap_a = segment->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);

    // Thread B write [100, 200) && delete range [0, 50)
    write_rows(100, 100);
    RowKeyRange range = RowKeyRange::fromHandleRange(HandleRange(0, 50));
    segment->write(dmContext(), range);
    LOG_INFO(Logger::get(), "Thread B read");
    ASSERT_INPUTSTREAM_NROWS(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)}),
        150);

    // Thread A
    {
        LOG_INFO(Logger::get(), "Thread A read with snap_a");
        auto in = segment->getInputStreamModeNormal(
            dmContext(),
            *tableColumns(),
            snap_a,
            {RowKeyRange::newAll(false, 1)},
            {},
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE);
        ASSERT_INPUTSTREAM_NROWS(in, 100);
    }
}
CATCH

TEST_F(SegmentTest, ReadWithMoreAdvacedDeltaIndexComplicated)
try
{
    // Test the case that reading rows with an advance DeltaIndex
    size_t offset = 0;
    auto write_rows = [&](size_t rows, bool flush) {
        Block block = DMTestEnv::prepareSimpleWriteBlock(offset, offset + rows, false);
        offset += rows;
        // write to segment
        segment->write(dmContext(), block, flush);
    };

    // Thread C
    write_rows(100, false);
    write_rows(100, false);
    ASSERT_INPUTSTREAM_NROWS(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)}),
        200);
    auto snap_c = segment->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
    segment->check(dmContext(), "test");

    // Thread A write 100 rows to persisted_files and 100 rows to mem_tables
    write_rows(100, true);
    write_rows(100, false);
    ASSERT_INPUTSTREAM_NROWS(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)}),
        400);
    auto snap_a = segment->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
    segment->check(dmContext(), "test");

    // Thread B write 100 rows to mem_tables
    write_rows(100, false);
    LOG_INFO(Logger::get(), "Thread B read");
    ASSERT_INPUTSTREAM_NROWS(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)}),
        500);
    segment->check(dmContext(), "test");

    // Thread A
    {
        LOG_INFO(Logger::get(), "Thread A read with snap_a");
        auto in = segment->getInputStreamModeNormal(
            dmContext(),
            *tableColumns(),
            snap_a,
            {RowKeyRange::newAll(false, 1)},
            {},
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE);
        ASSERT_INPUTSTREAM_NROWS(in, 400);
    }
    // Thread C
    {
        LOG_INFO(Logger::get(), "Thread C read with snap_c");
        auto in = segment->getInputStreamModeNormal(
            dmContext(),
            *tableColumns(),
            snap_c,
            {RowKeyRange::newAll(false, 1)},
            {},
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE);
        ASSERT_INPUTSTREAM_NROWS(in, 200);
    }
}
CATCH

class SegmentDeletionRelevantPlaceTest
    : public SegmentTest
    , public testing::WithParamInterface<bool>
{
};


TEST_P(SegmentDeletionRelevantPlaceTest, ShareDelteRangeIndex)
try
{
    Settings my_settings;
    const auto enable_relevant_place = GetParam();
    my_settings.dt_enable_relevant_place = enable_relevant_place;
    this->buildFirstSegment({}, std::move(my_settings));

    const size_t num_rows_write = 300;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(dmContext(), std::move(block));
    }

    auto get_rows = [&](const RowKeyRange & range) {
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {range});
        return getInputStreamNRows(in);
    };

    // First place the block packs, so that we can only place DeleteRange below.
    get_rows(RowKeyRange::fromHandleRange(HandleRange::newAll()));

    {
        HandleRange remove(100, 200);
        segment->write(dmContext(), /*delete_range*/ {RowKeyRange::fromHandleRange(remove)});
    }

    // The first call of get_rows below will place the DeleteRange into delta index.
    // If relevant place is enabled, the placed deletes in delta-tree-index is not
    // pushed forward since we do not fully apply the delete range [100, 200).
    auto rows1 = get_rows(RowKeyRange::fromHandleRange(HandleRange(0, 150)));
    {
        auto delta = segment->getDelta();
        auto placed_rows = delta->getPlacedDeltaRows();
        auto placed_deletes = delta->getPlacedDeltaDeletes();
        ASSERT_EQ(placed_rows, num_rows_write);
        EXPECT_EQ(placed_deletes, enable_relevant_place ? 0 : 1);
    }
    auto rows2 = get_rows(RowKeyRange::fromHandleRange(HandleRange(150, 300)));
    {
        auto delta = segment->getDelta();
        auto placed_rows = delta->getPlacedDeltaRows();
        auto placed_deletes = delta->getPlacedDeltaDeletes();
        ASSERT_EQ(placed_rows, num_rows_write);
        EXPECT_EQ(placed_deletes, enable_relevant_place ? 0 : 1);
    }
    // Query with range [0, 300) will push the placed deletes forward no matter
    // relevant place is enable or not.
    auto rows3 = get_rows(RowKeyRange::fromHandleRange(HandleRange(0, 300)));
    {
        auto delta = segment->getDelta();
        auto placed_rows = delta->getPlacedDeltaRows();
        auto placed_deletes = delta->getPlacedDeltaDeletes();
        ASSERT_EQ(placed_rows, num_rows_write);
        EXPECT_EQ(placed_deletes, 1);
    }

    ASSERT_EQ(rows1, 100);
    ASSERT_EQ(rows2, 100);
    ASSERT_EQ(rows3, 200);
}
CATCH

INSTANTIATE_TEST_CASE_P(WhetherEnableRelevantPlace, SegmentDeletionRelevantPlaceTest, testing::Values(true, false));

class SegmentDeletionTest
    : public SegmentTest
    , public testing::WithParamInterface<std::tuple<bool, bool>>
{
};

TEST_P(SegmentDeletionTest, DeleteDataInDelta)
try
{
    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(dmContext(), std::move(block));
    }

    auto [read_before_delete, merge_delta_after_delete] = GetParam();
    if (read_before_delete)
    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
    }

    {
        // test delete range [1,99) for data in delta
        HandleRange remove(1, 99);
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(remove)});
        // TODO test delete range partial overlap with segment
        // TODO test delete range not included by segment
    }

    if (merge_delta_after_delete)
    {
        // flush cache before applying merge delete range or the delete range will not be compacted to stable
        segment->flushCache(dmContext());
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // read after delete range
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({DMTestEnv::pk_name}), createColumns({createColumn<Int64>({0, 99})}));
    }

    // For the case that apply merge delta after delete range, we ensure that data on disk are compacted
    if (merge_delta_after_delete)
    {
        // read raw after delete range
        auto in = segment->getInputStreamModeRaw(dmContext(), *tableColumns());
        // Only 2 rows are left on disk, others are compacted.
        ASSERT_INPUTSTREAM_NROWS(in, 2);
    }
}
CATCH

TEST_P(SegmentDeletionTest, DeleteDataInStable)
try
{
    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(dmContext(), std::move(block));
    }

    auto [read_before_delete, merge_delta_after_delete] = GetParam();
    if (read_before_delete)
    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
    }

    {
        // merge delta to create stable
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // test delete range [1,99) for data in stable
        HandleRange remove(1, 99);
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(remove)});
        // TODO test delete range partial overlap with segment
        // TODO test delete range not included by segment

        // flush segment
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    if (merge_delta_after_delete)
    {
        // flush cache before applying merge delete range or the delete range will not be compacted to stable
        segment->flushCache(dmContext());
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // read after delete range
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({DMTestEnv::pk_name}), createColumns({createColumn<Int64>({0, 99})}));
    }

    // For the case that apply merge delta after delete range, we ensure that data on disk are compacted
    if (merge_delta_after_delete)
    {
        // read raw after delete range
        auto in = segment->getInputStreamModeRaw(dmContext(), *tableColumns());
        // Only 2 rows are left on disk, others are compacted.
        ASSERT_INPUTSTREAM_NROWS(in, 2);
    }
}
CATCH

TEST_P(SegmentDeletionTest, DeleteDataInStableAndDelta)
try
{
    const size_t num_rows_write = 100;
    {
        // write [0, 50) to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        segment->write(dmContext(), std::move(block));
        // flush [0, 50) to segment's stable
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    auto [read_before_delete, merge_delta_after_delete] = GetParam();

    {
        // write [50, 100) to segment's delta
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write, false);
        segment->write(dmContext(), std::move(block));
    }

    if (read_before_delete)
    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
    }

    {
        // test delete range [1,99) for data in stable and delta
        HandleRange remove(1, 99);
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(remove)});
        // TODO test delete range partial overlap with segment
        // TODO test delete range not included by segment
    }

    if (merge_delta_after_delete)
    {
        // flush cache before applying merge delete range or the delete range will not be compacted to stable
        segment->flushCache(dmContext());
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // read after delete range
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({DMTestEnv::pk_name}), createColumns({createColumn<Int64>({0, 99})}));
    }

    // For the case that apply merge delta after delete range, we ensure that data on disk are compacted
    if (merge_delta_after_delete)
    {
        // read raw after delete range
        auto in = segment->getInputStreamModeRaw(dmContext(), *tableColumns());
        // Only 2 rows are left on disk, others are compacted.
        ASSERT_INPUTSTREAM_NROWS(in, 2);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(
    WhetherReadOrMergeDeltaBeforeDeleteRange,
    SegmentDeletionTest,
    testing::Combine(testing::Bool(), testing::Bool()));

TEST_F(SegmentTest, DeleteRead)
try
{
    const size_t num_rows_write = 64;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(dmContext(), std::move(block));
    }

    {
        // do delta-merge move data to stable
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    auto check_segment_squash_delete_range = [this](SegmentPtr & segment, const HandleRange & expect_range) {
        // set `is_update=false` to get full squash delete range
        auto snap = segment->createSnapshot(dmContext(), /*for_update*/ false, CurrentMetrics::DT_SnapshotOfRead);
        auto squash_range = snap->delta->getSquashDeleteRange(/*is_common_handle=*/false, /*rowkey_column_size=*/1);
        ASSERT_ROWKEY_RANGE_EQ(squash_range, RowKeyRange::fromHandleRange(expect_range));
    };

    {
        // Test delete range [70, 100)
        HandleRange del{70, 100};
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        SCOPED_TRACE("check after range: " + del.toDebugString()); // Add trace msg when ASSERT failed
        check_segment_squash_delete_range(segment, HandleRange{70, 100});
    }

    {
        // Read after deletion
        // The deleted range has no overlap with current data, so there should be no change
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({createColumn<Int64>(createNumbers<Int64>(0, num_rows_write))}));
    }

    {
        // Test delete range [63, 70)
        HandleRange del{63, 70};
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        SCOPED_TRACE("check after range: " + del.toDebugString());

        check_segment_squash_delete_range(segment, HandleRange{63, 100});
    }

    {
        // Read after deletion
        // The deleted range has overlap range [63, 64) with current data, so the record with Handle 63 should be deleted
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({createColumn<Int64>(createNumbers<Int64>(0, 63))}));
    }

    {
        // Test delete range [1, 32)
        HandleRange del{1, 32};
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        SCOPED_TRACE("check after range: " + del.toDebugString());
        // suqash_delete_range will consider [1, 100) maybe deleted
        check_segment_squash_delete_range(segment, HandleRange{1, 100});
    }

    {
        // Read after deletion
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});

        std::vector<Int64> pk_coldata{0};
        auto tmp = createNumbers<Int64>(32, 63);
        pk_coldata.insert(pk_coldata.end(), tmp.begin(), tmp.end());
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({DMTestEnv::pk_name}), createColumns({createColumn<Int64>(pk_coldata)}));
    }

    {
        // Test delete range [1, 32)
        // delete should be idempotent
        HandleRange del{1, 32};
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        SCOPED_TRACE("check after range: " + del.toDebugString());
        check_segment_squash_delete_range(segment, HandleRange{1, 100});
    }

    {
        // Read after deletion
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        std::vector<Int64> pk_coldata{0};
        auto tmp = createNumbers<Int64>(32, 63);
        pk_coldata.insert(pk_coldata.end(), tmp.begin(), tmp.end());
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({DMTestEnv::pk_name}), createColumns({createColumn<Int64>(pk_coldata)}));
    }

    {
        // Test delete range [0, 2)
        // There is an overlap range [0, 1)
        HandleRange del{0, 2};
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        SCOPED_TRACE("check after range: " + del.toDebugString());
        check_segment_squash_delete_range(segment, HandleRange{0, 100});
    }

    {
        // Read after deletion
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        std::vector<Int64> pk_coldata = createNumbers<Int64>(32, 63);
        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({DMTestEnv::pk_name}), createColumns({createColumn<Int64>(pk_coldata)}));
    }

    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(9, 16, false);
        segment->write(dmContext(), std::move(block));
        SCOPED_TRACE("check after write");
        // if we write some new data, we can still get the delete range
        check_segment_squash_delete_range(segment, HandleRange{0, 100});
    }

    {
        // Read after new write
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        std::vector<Int64> pk_coldata = createNumbers<Int64>(9, 16);
        auto tmp = createNumbers<Int64>(32, 63);
        pk_coldata.insert(pk_coldata.end(), tmp.begin(), tmp.end());

        ASSERT_INPUTSTREAM_COLS_UR(in, Strings({DMTestEnv::pk_name}), createColumns({createColumn<Int64>(pk_coldata)}));
    }
}
CATCH

TEST_F(SegmentTest, ColumnFileBigRangeGreaterThanSegment)
try
{
    auto write_column_file_big = [&](size_t begin, size_t end, size_t block_num) {
        WriteBatches ingest_wbs(*dm_context->storage_pool, dm_context->getWriteLimiter());
        auto delegator = storage_path_pool->getStableDiskDelegator();
        auto parent_path = delegator.choosePath();
        auto file_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);

        std::vector<BlockInputStreamPtr> streams;
        streams.reserve(block_num);
        size_t step = (end - begin) / block_num;
        for (size_t i = 0; i < block_num; ++i)
        {
            Block block = DMTestEnv::prepareSimpleWriteBlock(begin + i * step, begin + (i + 1) * step, false);
            auto stream = std::make_shared<OneBlockInputStream>(block);
            streams.push_back(stream);
        }
        auto input_stream = std::make_shared<ConcatBlockInputStream>(std::move(streams), dmContext().tracing_id);

        auto dm_file = writeIntoNewDMFile(*dm_context, table_columns, input_stream, file_id, parent_path);
        ingest_wbs.data.putExternal(file_id, /* tag */ 0);
        ingest_wbs.writeLogAndData();
        delegator.addDTFile(file_id, dm_file->getBytesOnDisk(), parent_path);

        WriteBatches wbs(*dm_context->storage_pool, dm_context->getWriteLimiter());
        auto ref_id = storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
        wbs.data.putRefPage(ref_id, dm_file->pageId());
        auto ref_file = DMFile::restore(
            dm_context->global_context.getFileProvider(),
            file_id,
            ref_id,
            parent_path,
            DMFileMeta::ReadMode::all());
        wbs.writeLogAndData();
        ASSERT_TRUE(segment->ingestDataToDelta(
            *dm_context,
            segment->getRowKeyRange(),
            {ref_file},
            /* clear_data_in_range */ false));

        ingest_wbs.rollbackWrittenLogAndData();
    };

    {
        // let segment's rowkey_range be [30, 70)
        HandleRange range(30, 70);
        segment->rowkey_range = RowKeyRange::fromHandleRange(range);
        // write ColumnFileBig with range [0, 20), [20, 40), [40, 60), [60, 80), [80, 100).
        write_column_file_big(0, 100, 5);
    }
    {
        // test built bitmap filter
        auto segment_snap = segment->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
        auto read_ranges = {RowKeyRange::newAll(false, 1)};
        auto real_ranges = segment->shrinkRowKeyRanges(read_ranges);
        auto bitmap_filter = segment->buildBitmapFilter( //
            dmContext(),
            segment_snap,
            real_ranges,
            {},
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE);
        // the bitmap only contains the overlapped packs of ColumnFileBig. So only 60 here.
        ASSERT_EQ(bitmap_filter->size(), 60);
        ASSERT_EQ(bitmap_filter->toDebugString(), "000000000011111111111111111111111111111111111111110000000000");
    }

    ColumnDefines cols_to_read{
        getExtraHandleColumnDefine(false),
    };
    {
        // test read data
        auto segment_snap = segment->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
        auto in = segment->getBitmapFilterInputStream(
            dmContext(),
            cols_to_read,
            segment_snap,
            {RowKeyRange::newAll(false, 1)},
            EMPTY_FILTER,
            {},
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOCK_SIZE);
        ASSERT_INPUTSTREAM_BLOCK_UR(
            in,
            Block({
                createColumn<Int64>(createNumbers<Int64>(30, 70)),
            }));
    }

    {
        // let segment's rowkey_range be [30, 90)
        HandleRange range(30, 90);
        segment->rowkey_range = RowKeyRange::fromHandleRange(range);
        // delete range [50, 80)
        Block block = DMTestEnv::prepareSimpleWriteBlock(
            50,
            80,
            false,
            3,
            "_tidb_rowid",
            MutSup::extra_handle_id,
            MutSup::getExtraHandleColumnIntType(),
            false,
            1,
            true,
            /*is_deleted=*/true);
        segment->write(dmContext(), std::move(block));
        // write range [80, 90)
        Block block2 = DMTestEnv::prepareSimpleWriteBlock(80, 90, false);
        segment->write(dmContext(), std::move(block2));
    }
    {
        // test read data with delete-range and new writes
        auto segment_snap = segment->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);
        auto in = segment->getBitmapFilterInputStream(
            dmContext(),
            cols_to_read,
            segment_snap,
            {RowKeyRange::newAll(false, 1)},
            EMPTY_FILTER,
            {},
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE,
            DEFAULT_BLOCK_SIZE);
        // Only the rows in [30, 50) and [80, 90) valid
        auto vec = createNumbers<Int64>(30, 50);
        vec.append_range(createNumbers<Int64>(80, 90));
        ASSERT_INPUTSTREAM_BLOCK_UR(in, Block({createColumn<Int64>(vec)}));
    }
}
CATCH

TEST_F(SegmentTest, Split)
try
{
    const size_t num_rows_write_per_batch = 100;
    const size_t num_rows_write = num_rows_write_per_batch * 2;
    {
        // write to segment and flush
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write_per_batch, false);
        segment->write(dmContext(), std::move(block), true);
    }
    {
        // write to segment and don't flush
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write_per_batch, 2 * num_rows_write_per_batch, false);
        segment->write(dmContext(), std::move(block), false);
    }

    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
    }

    const auto old_range = segment->getRowKeyRange();

    SegmentPtr new_segment;
    // test split segment
    {
        std::tie(segment, new_segment) = segment->split(dmContext(), tableColumns());
    }
    // check segment range
    const auto s1_range = segment->getRowKeyRange();
    EXPECT_EQ(*s1_range.start.value, *old_range.start.value);
    const auto s2_range = new_segment->getRowKeyRange();
    EXPECT_EQ(*s2_range.start.value, *s1_range.end.value);
    EXPECT_EQ(*s2_range.end.value, *old_range.end.value);
    // TODO check segment epoch is increase

    size_t num_rows_seg1 = getInputStreamNRows(
        segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {segment->getRowKeyRange()}));
    size_t num_rows_seg2 = getInputStreamNRows(
        new_segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {new_segment->getRowKeyRange()}));
    ASSERT_EQ(num_rows_seg1 + num_rows_seg2, num_rows_write);

    // delete rows in the right segment
    {
        new_segment->write(dmContext(), /*delete_range*/ new_segment->getRowKeyRange());
        new_segment->flushCache(dmContext());
    }

    // merge segments
    {
        segment = Segment::merge(dmContext(), tableColumns(), {segment, new_segment});
        {
            // check merged segment range
            const auto & merged_range = segment->getRowKeyRange();
            EXPECT_EQ(*merged_range.start.value, *s1_range.start.value);
            EXPECT_EQ(*merged_range.end.value, *s2_range.end.value);
            // TODO check segment epoch is increase
        }
        {
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            ASSERT_INPUTSTREAM_NROWS(in, num_rows_seg1);
        }
    }
}
CATCH

TEST_F(SegmentTest, SplitFail)
try
{
    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(dmContext(), std::move(block));
    }

    // Remove all data
    segment->write(dmContext(), RowKeyRange::fromHandleRange(HandleRange(0, 100)));
    segment->flushCache(dmContext());

    auto [a, b] = segment->split(dmContext(), tableColumns());
    EXPECT_EQ(a, SegmentPtr{});
    EXPECT_EQ(b, SegmentPtr{});
}
CATCH

TEST_F(SegmentTest, Restore)
try
{
    // compare will compares the given segments.
    // If they are equal, result will be true, otherwise it will be false.
    auto compare = [&](const SegmentPtr & seg1, const SegmentPtr & seg2, bool & result) {
        result = false;
        auto in1 = seg1->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        auto in2 = seg2->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        in1->readPrefix();
        in2->readPrefix();
        for (;;)
        {
            Block block1 = in1->read();
            Block block2 = in2->read();
            if (!block1)
            {
                ASSERT_TRUE(!block2);
                break;
            }

            ASSERT_EQ(block1.rows(), block2.rows());

            auto iter1 = block1.begin();
            auto iter2 = block2.begin();

            for (;;)
            {
                if (iter1 == block1.end())
                {
                    ASSERT_EQ(iter2, block2.end());
                    break;
                }

                auto c1 = iter1->column;
                auto c2 = iter2->column;

                ASSERT_EQ(c1->size(), c2->size());

                for (Int64 i = 0; i < static_cast<Int64>(c1->size()); i++)
                {
                    if (iter1->name == DMTestEnv::pk_name)
                    {
                        ASSERT_EQ(iter2->name, DMTestEnv::pk_name);
                        ASSERT_EQ(c1->getInt(i), c2->getInt(i));
                    }
                }

                // Call next
                iter1++;
                iter2++;
            }
        }
        in1->readSuffix();
        in2->readSuffix();

        result = true;
    };

    const size_t num_rows_write = 64;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(dmContext(), std::move(block));
        // flush segment
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    SegmentPtr new_segment = Segment::restoreSegment(Logger::get(), dmContext(), segment->segmentId());

    {
        // test compare
        bool result;
        compare(segment, new_segment, result);
        ASSERT_TRUE(result);
    }

    {
        // Do some update and restore again
        HandleRange del(0, 32);
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        new_segment = segment->restoreSegment(Logger::get(), dmContext(), segment->segmentId());
    }

    {
        // test compare
        bool result;
        compare(new_segment, new_segment, result);
        ASSERT_TRUE(result);
    }
}
CATCH

TEST_F(SegmentTest, MassiveSplit)
try
{
    Settings settings = dmContext().global_context.getSettings();
    settings.dt_segment_limit_rows = 11;
    settings.dt_segment_delta_limit_rows = 7;

    segment = buildFirstSegment(DMTestEnv::getDefaultColumns(), std::move(settings));

    size_t num_batches_written = 0;
    const size_t num_rows_per_write = 5;

    const time_t start_time = std::time(nullptr);

    std::vector<Int64> temp;
    for (;;)
    {
        {
            // Write to segment
            Block block = DMTestEnv::prepareSimpleWriteBlock(
                num_batches_written * num_rows_per_write,
                num_batches_written * num_rows_per_write + num_rows_per_write,
                false);
            segment->write(dmContext(), std::move(block));
            num_batches_written += 1;
        }

        {
            // Delete some records so that the following condition can be satisfied:
            // if pk % 5 < 2, then the record would be deleted
            // if pk % 5 >= 2, then the record would be reserved
            HandleRange del{
                static_cast<Int64>((num_batches_written - 1) * num_rows_per_write),
                static_cast<Int64>((num_batches_written - 1) * num_rows_per_write + 2)};
            segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext(), tableColumns());
        }

        for (size_t i = (num_batches_written - 1) * num_rows_per_write + 2;
             i < num_batches_written * num_rows_per_write;
             i++)
        {
            temp.push_back(static_cast<Int64>(i));
        }

        {
            // Read after writing
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            ASSERT_INPUTSTREAM_COLS_UR(in, Strings({DMTestEnv::pk_name}), createColumns({createColumn<Int64>(temp)}))
                << fmt::format("num_batches_written={} num_rows_per_write={}", num_batches_written, num_rows_per_write);
        }

        {
            // Run for long enough to make sure Split is robust.
            const time_t end_time = std::time(nullptr);
            // if ((end_time - start_time) / 60 > 10)
            if ((end_time - start_time) > 10)
            {
                return;
            }
        }
    }
}
CATCH

enum class SegmentTestMode
{
    PageStorageV2_MemoryOnly,
    PageStorageV2_DiskOnly,
    Current_MemoryOnly,
    Current_DiskOnly,
};

String testModeToString(const ::testing::TestParamInfo<SegmentTestMode> & info)
{
    return String{magic_enum::enum_name(info.param)};
}

class SegmentTest2
    : public SegmentTest
    , public testing::WithParamInterface<SegmentTestMode>
{
    const StorageFormatVersion current_version;

public:
    SegmentTest2()
        : current_version(STORAGE_FORMAT_CURRENT)
    {}

    void SetUp() override
    {
        mode = GetParam();
        switch (mode)
        {
        case SegmentTestMode::PageStorageV2_MemoryOnly:
        case SegmentTestMode::PageStorageV2_DiskOnly:
            setStorageFormat(3);
            break;
        case SegmentTestMode::Current_MemoryOnly:
        case SegmentTestMode::Current_DiskOnly:
            setStorageFormat(STORAGE_FORMAT_CURRENT);
            break;
        }

        SegmentTest::SetUp();
    }

    ~SegmentTest2() override { setStorageFormat(current_version); }

    std::pair<RowKeyRange, PageIdU64s> genDMFile(DMContext & context, const Block & block)
    {
        auto delegator = context.path_pool->getStableDiskDelegator();
        auto file_id = context.storage_pool->newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
        auto input_stream = std::make_shared<OneBlockInputStream>(block);
        auto store_path = delegator.choosePath();

        auto dmfile = writeIntoNewDMFile(
            context,
            std::make_shared<ColumnDefines>(*tableColumns()),
            input_stream,
            file_id,
            store_path);

        delegator.addDTFile(file_id, dmfile->getBytesOnDisk(), store_path);

        const auto & pk_column = block.getByPosition(0).column;
        auto min_pk = pk_column->getInt(0);
        auto max_pk = pk_column->getInt(block.rows() - 1);
        HandleRange range(min_pk, max_pk + 1);

        return {RowKeyRange::fromHandleRange(range), {file_id}};
    }

    SegmentTestMode mode{};
};

TEST_P(SegmentTest2, FlushDuringSplitAndMerge)
try
{
    size_t row_offset = 0;
    auto write_100_rows = [&, this](const SegmentPtr & segment) {
        {
            // write to segment
            Block block = DMTestEnv::prepareSimpleWriteBlock(row_offset, row_offset + 100, false);
            row_offset += 100;
            switch (mode)
            {
            case SegmentTestMode::PageStorageV2_MemoryOnly:
            case SegmentTestMode::Current_MemoryOnly:
                segment->write(dmContext(), std::move(block));
                break;
            case SegmentTestMode::PageStorageV2_DiskOnly:
            case SegmentTestMode::Current_DiskOnly:
            {
                auto delegate = dmContext().path_pool->getStableDiskDelegator();
                auto file_provider = dmContext().global_context.getFileProvider();
                auto [range, file_ids] = genDMFile(dmContext(), block);
                auto file_id = file_ids[0];
                auto file_parent_path = delegate.getDTFilePath(file_id);
                auto file
                    = DMFile::restore(file_provider, file_id, file_id, file_parent_path, DMFileMeta::ReadMode::all());
                WriteBatches wbs(*storage_pool);
                wbs.data.putExternal(file_id, 0);
                wbs.writeLogAndData();

                segment->ingestDataToDelta(dmContext(), range, {file}, false);
                break;
            }
            default:
                throw Exception("Unsupported");
            }

            segment->flushCache(dmContext());
        }
    };

    auto read_rows = [&](const SegmentPtr & segment) {
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        return getInputStreamNRows(in);
    };

    write_100_rows(segment);

    // Test split
    SegmentPtr other_segment;
    {
        WriteBatches wbs(*dmContext().storage_pool);
        auto segment_snap = segment->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfSegmentSplit);
        ASSERT_FALSE(!segment_snap);

        write_100_rows(segment);

        auto split_info = segment->prepareSplit(dmContext(), tableColumns(), segment_snap, wbs);

        wbs.writeLogAndData();
        split_info->my_stable->enableDMFilesGC(dmContext());
        split_info->other_stable->enableDMFilesGC(dmContext());

        auto lock = segment->mustGetUpdateLock();
        std::tie(segment, other_segment)
            = segment->applySplit(lock, dmContext(), segment_snap, wbs, split_info.value());

        wbs.writeAll();
    }

    {
        SegmentPtr new_segment_1 = Segment::restoreSegment(Logger::get(), dmContext(), segment->segmentId());
        SegmentPtr new_segment_2 = Segment::restoreSegment(Logger::get(), dmContext(), other_segment->segmentId());
        auto rows1 = read_rows(new_segment_1);
        auto rows2 = read_rows(new_segment_2);
        ASSERT_EQ(rows1 + rows2, (size_t)200);
    }

    // Test merge
    {
        WriteBatches wbs(*dmContext().storage_pool);

        auto left_snap = segment->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
        auto right_snap = other_segment->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
        ASSERT_FALSE(!left_snap || !right_snap);

        write_100_rows(other_segment);
        segment->flushCache(dmContext());

        auto merged_stable = Segment::prepareMerge(
            dmContext(),
            tableColumns(),
            {segment, other_segment},
            {left_snap, right_snap},
            wbs);

        wbs.writeLogAndData();
        merged_stable->enableDMFilesGC(dmContext());

        std::vector<Segment::Lock> locks;
        locks.emplace_back(segment->mustGetUpdateLock());
        locks.emplace_back(other_segment->mustGetUpdateLock());
        segment = Segment::applyMerge(
            locks,
            dmContext(),
            {segment, other_segment},
            {left_snap, right_snap},
            wbs,
            merged_stable);

        wbs.writeAll();
    }

    {
        SegmentPtr new_segment = Segment::restoreSegment(Logger::get(), dmContext(), segment->segmentId());
        auto rows = read_rows(new_segment);
        ASSERT_EQ(rows, (size_t)300);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(
    SegmentTestMode, //
    SegmentTest2,
    testing::Values(
        SegmentTestMode::PageStorageV2_MemoryOnly,
        SegmentTestMode::PageStorageV2_DiskOnly,
        SegmentTestMode::Current_MemoryOnly,
        SegmentTestMode::Current_DiskOnly),
    testModeToString);

enum class SegmentWriteType
{
    ToDisk,
    ToCache
};
class SegmentDDLTest
    : public SegmentTest
    , public testing::WithParamInterface<std::tuple<SegmentWriteType, bool>>
{
};
String paramToString(const ::testing::TestParamInfo<SegmentDDLTest::ParamType> & info)
{
    const auto [write_type, flush_before_ddl] = info.param;

    String name = (write_type == SegmentWriteType::ToDisk) ? "ToDisk_" : "ToCache";
    name += (flush_before_ddl ? "_FlushCache" : "_NotFlushCache");
    return name;
}

/// Mock a col from i8 -> i32
TEST_P(SegmentDDLTest, AlterInt8ToInt32)
try
{
    const String column_name_i8_to_i32 = "i8_to_i32";
    const ColumnID column_id_i8_to_i32 = 4;
    const ColumnDefine column_i8_before_ddl(column_id_i8_to_i32, column_name_i8_to_i32, typeFromString("Int8"));
    const ColumnDefine column_i32_after_ddl(column_id_i8_to_i32, column_name_i8_to_i32, typeFromString("Int32"));

    const auto [write_type, flush_before_ddl] = GetParam();

    // Prepare some data before ddl
    const size_t num_rows_write = 100;
    {
        /// set columns before ddl
        auto columns_before_ddl = DMTestEnv::getDefaultColumns();
        columns_before_ddl->emplace_back(column_i8_before_ddl);
        DB::Settings db_settings;
        segment = buildFirstSegment(columns_before_ddl, std::move(db_settings));

        /// write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        // add int8_col and later read it as int32
        // (mock ddl change int8 -> int32)
        block.insert(DB::tests::createColumn<Int8>(
            createSignedNumbers(0, num_rows_write),
            column_i8_before_ddl.name,
            column_id_i8_to_i32));
        switch (write_type)
        {
        case SegmentWriteType::ToDisk:
            segment->write(dmContext(), std::move(block));
            break;
        case SegmentWriteType::ToCache:
            segment->writeToCache(dmContext(), block, 0, num_rows_write);
            break;
        }
    }

    ColumnDefinesPtr columns_to_read = std::make_shared<ColumnDefines>();
    {
        *columns_to_read = *DMTestEnv::getDefaultColumns();
        columns_to_read->emplace_back(column_i32_after_ddl);
        if (flush_before_ddl)
        {
            segment->flushCache(dmContext());
        }
        setColumns(columns_to_read);
    }

    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *columns_to_read, {RowKeyRange::newAll(false, 1)});
        // check that we can read correct values
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, column_i32_after_ddl.name}),
            createColumns({//
                           createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                           createColumn<Int32>(createSignedNumbers(0, num_rows_write))}));
    }


    /// Write some data after ddl, replacing som origin rows
    {
        /// write to segment, replacing some origin rows
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write * 2, false, /* tso= */ 3);
        block.insert(DB::tests::createColumn<Int32>(
            createSignedNumbers(0, block.rows()),
            column_i32_after_ddl.name,
            column_id_i8_to_i32));
        switch (write_type)
        {
        case SegmentWriteType::ToDisk:
            segment->write(dmContext(), std::move(block));
            break;
        case SegmentWriteType::ToCache:
            segment->writeToCache(dmContext(), block, 0, block.rows());
            break;
        }
    }

    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *columns_to_read, {RowKeyRange::newAll(false, 1)});

        // check that we can read correct values
        // [0, 50) is the old signed values, [50, 100) is replaced by newer written values
        std::vector<Int64> i32_columndata = createSignedNumbers(0, num_rows_write / 2);
        auto tmp = createSignedNumbers(0, num_rows_write * 2 - num_rows_write / 2);
        i32_columndata.insert(i32_columndata.end(), tmp.begin(), tmp.end());
        ASSERT_EQ(i32_columndata.size(), 2 * num_rows_write);

        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, column_i32_after_ddl.name}),
            createColumns({//
                           createColumn<Int64>(createNumbers<Int64>(0, 2 * num_rows_write)),
                           createColumn<Int32>(i32_columndata)}));
    }

    // Flush cache and apply delta-merge, then read again
    // This will create a new stable with new schema, check the data.
    {
        segment->flushCache(dmContext());
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // check the stable data with new schema
        auto in = segment->getInputStreamModeNormal(dmContext(), *columns_to_read, {RowKeyRange::newAll(false, 1)});

        // check that we can read correct values
        std::vector<Int64> i32_columndata = createSignedNumbers(0, num_rows_write / 2);
        auto tmp = createSignedNumbers(0, num_rows_write * 2 - num_rows_write / 2);
        i32_columndata.insert(i32_columndata.end(), tmp.begin(), tmp.end());
        ASSERT_EQ(i32_columndata.size(), 2 * num_rows_write);

        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, column_i32_after_ddl.name}),
            createColumns({//
                           createColumn<Int64>(createNumbers<Int64>(0, 2 * num_rows_write)),
                           createColumn<Int32>(i32_columndata)}));
    }
}
CATCH

TEST_P(SegmentDDLTest, AddColumn)
try
{
    const String new_column_name = "i8";
    const ColumnID new_column_id = 4;
    ColumnDefine new_column_define(new_column_id, new_column_name, typeFromString("Int8"));
    const Int8 new_column_default_value_int = 16;
    new_column_define.default_value = toField(new_column_default_value_int);

    const auto [write_type, flush_before_ddl] = GetParam();

    {
        auto columns_before_ddl = DMTestEnv::getDefaultColumns();
        // Not cache any rows
        DB::Settings db_settings;
        segment = buildFirstSegment(columns_before_ddl, std::move(db_settings));
    }

    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        switch (write_type)
        {
        case SegmentWriteType::ToDisk:
            segment->write(dmContext(), std::move(block));
            break;
        case SegmentWriteType::ToCache:
            segment->writeToCache(dmContext(), block, 0, num_rows_write);
            break;
        }
    }

    auto columns_after_ddl = DMTestEnv::getDefaultColumns();
    {
        // DDL add new column with default value
        columns_after_ddl->emplace_back(new_column_define);
        if (flush_before_ddl)
        {
            // If write to cache, before apply ddl changes (change column data type), segment->flushCache must be called.
            segment->flushCache(dmContext());
        }
        setColumns(columns_after_ddl);
    }

    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *columns_after_ddl, {RowKeyRange::newAll(false, 1)});

        // check that we can read correct values
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, new_column_define.name}),
            createColumns({//
                           createColumn<Int64>(createNumbers<Int64>(0, num_rows_write)),
                           createColumn<Int8>(std::vector<Int64>(num_rows_write, new_column_default_value_int))}));
    }


    /// Write some data after ddl, replacing som origin rows
    {
        /// write to segment, replacing some origin rows
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2, num_rows_write * 2, false, /* tso= */ 3);
        auto col = DB::tests::createColumn<Int8>(
            createSignedNumbers(0, block.rows()),
            new_column_define.name,
            new_column_id);
        col.default_value = new_column_define.default_value;
        block.insert(std::move(col));
        switch (write_type)
        {
        case SegmentWriteType::ToDisk:
            segment->write(dmContext(), std::move(block));
            break;
        case SegmentWriteType::ToCache:
            segment->writeToCache(dmContext(), block, 0, block.rows());
            break;
        }
    }

    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *columns_after_ddl, {RowKeyRange::newAll(false, 1)});

        // check that we can read correct values
        // First 50 values are default value
        std::vector<Int64> i8_columndata(num_rows_write / 2, new_column_default_value_int);
        // then fill with signed number sequence
        auto tmp = createSignedNumbers(0, num_rows_write * 2 - num_rows_write / 2);
        i8_columndata.insert(i8_columndata.end(), tmp.begin(), tmp.end());
        ASSERT_EQ(i8_columndata.size(), 2 * num_rows_write);

        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, new_column_define.name}),
            createColumns({//
                           createColumn<Int64>(createNumbers<Int64>(0, 2 * num_rows_write)),
                           createColumn<Int8>(i8_columndata)}));
    }

    // Flush cache and apply delta-merge, then read again
    // This will create a new stable with new schema, check the data.
    {
        segment->flushCache(dmContext());
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // read written data after delta-merge
        auto in = segment->getInputStreamModeNormal(dmContext(), *columns_after_ddl, {RowKeyRange::newAll(false, 1)});

        // check that we can read correct values
        // First 50 values are default value
        std::vector<Int64> i8_columndata(num_rows_write / 2, new_column_default_value_int);
        // then fill with signed number sequence
        auto tmp = createSignedNumbers(0, num_rows_write * 2 - num_rows_write / 2);
        i8_columndata.insert(i8_columndata.end(), tmp.begin(), tmp.end());
        ASSERT_EQ(i8_columndata.size(), 2 * num_rows_write);

        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name, new_column_define.name}),
            createColumns({//
                           createColumn<Int64>(createNumbers<Int64>(0, 2 * num_rows_write)),
                           createColumn<Int8>(i8_columndata)}));
    }
}
CATCH

TEST_F(SegmentTest, CalculateDTFileProperty)
try
{
    Settings settings = dmContext().global_context.getSettings();
    settings.dt_segment_stable_pack_rows = 10;

    segment = buildFirstSegment(DMTestEnv::getDefaultColumns(), std::move(settings));

    const size_t num_rows_write_every_round = 100;
    const size_t write_round = 3;
    const size_t tso = 10000;
    for (size_t i = 0; i < write_round; i++)
    {
        size_t start = num_rows_write_every_round * i;
        Block block = DMTestEnv::prepareSimpleWriteBlock(start, start + num_rows_write_every_round, false, tso);
        // write to segment
        segment->write(dmContext(), block);
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        const auto & stable = segment->getStable();
        ASSERT_GT(stable->getDMFiles()[0]->getPacks(), (size_t)1);
        ASSERT_EQ(stable->getRows(), num_rows_write_every_round * write_round);
        // calculate StableProperty
        ASSERT_EQ(stable->isStablePropertyCached(), false);
        auto start = RowKeyValue::fromHandle(0);
        auto end = RowKeyValue::fromHandle(num_rows_write_every_round);
        RowKeyRange range(start, end, false, 1);
        // calculate the StableProperty for packs in the key range [0, num_rows_write_every_round)
        stable->calculateStableProperty(dmContext(), range, false);
        ASSERT_EQ(stable->isStablePropertyCached(), true);
        const auto & property = stable->getStableProperty();
        ASSERT_EQ(property.gc_hint_version, std::numeric_limits<UInt64>::max());
        ASSERT_EQ(property.num_versions, num_rows_write_every_round);
        ASSERT_EQ(property.num_puts, num_rows_write_every_round);
        ASSERT_EQ(property.num_rows, num_rows_write_every_round);
    }
}
CATCH

TEST_F(SegmentTest, CalculateDTFilePropertyWithPropertyFileDeleted)
try
{
    Settings settings = dmContext().global_context.getSettings();
    settings.dt_segment_stable_pack_rows = 10;

    segment = buildFirstSegment(DMTestEnv::getDefaultColumns(), std::move(settings));

    const size_t num_rows_write_every_round = 100;
    const size_t write_round = 3;
    const size_t tso = 10000;
    for (size_t i = 0; i < write_round; i++)
    {
        size_t start = num_rows_write_every_round * i;
        Block block = DMTestEnv::prepareSimpleWriteBlock(start, start + num_rows_write_every_round, false, tso);
        // write to segment
        segment->write(dmContext(), block);
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        const auto & stable = segment->getStable();
        const auto & dmfiles = stable->getDMFiles();
        ASSERT_GT(dmfiles[0]->getPacks(), (size_t)1);
        const auto & dmfile = dmfiles[0];
        auto file_path = dmfile->path();
        // check property file exists and then delete it
        if (!dmfile->useMetaV2())
        {
            ASSERT_EQ(Poco::File(file_path + "/property").exists(), true);
            Poco::File(file_path + "/property").remove();
            ASSERT_EQ(Poco::File(file_path + "/property").exists(), false);
        }
        // clear PackProperties to force it to calculate from scratch
        dmfile->clearPackProperties();
        ASSERT_EQ(dmfile->getPackProperties().property_size(), 0);
        // caculate StableProperty
        ASSERT_EQ(stable->isStablePropertyCached(), false);
        auto start = RowKeyValue::fromHandle(0);
        auto end = RowKeyValue::fromHandle(num_rows_write_every_round);
        RowKeyRange range(start, end, false, 1);
        // calculate the StableProperty for packs in the key range [0, num_rows_write_every_round)
        stable->calculateStableProperty(dmContext(), range, false);
        ASSERT_EQ(stable->isStablePropertyCached(), true);
        const auto & property = stable->getStableProperty();
        ASSERT_EQ(property.gc_hint_version, std::numeric_limits<UInt64>::max());
        ASSERT_EQ(property.num_versions, num_rows_write_every_round);
        ASSERT_EQ(property.num_puts, num_rows_write_every_round);
        ASSERT_EQ(property.num_rows, num_rows_write_every_round);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(
    SegmentWriteType,
    SegmentDDLTest,
    ::testing::Combine( //
        ::testing::Values(SegmentWriteType::ToDisk, SegmentWriteType::ToCache),
        ::testing::Bool()),
    paramToString);


TEST_F(SimplePKTestBasic, FlushWhileMerging)
try
{
    ensureSegmentBreakpoints({0, 50, 100});

    // Everything in memtable
    fill(-1000, 1000);
    ASSERT_EQ(1000, getSegmentAt(-10)->getDelta()->getMemTableSet()->getRows());

    auto sp_merge_prepared = SyncPointCtl::enableInScope("after_DeltaMergeStore::segmentMerge|prepare_merge");

    auto th_merge = std::async([&]() { ASSERT_TRUE(merge(-10, 20)); });

    sp_merge_prepared.waitAndPause();
    flush(-10, -9); // Flush the first segment
    sp_merge_prepared.next();
    th_merge.get();

    ASSERT_EQ(2000, getRowsN());
    ASSERT_EQ(2000, getRowsN(-1000, 1000));
}
CATCH

TEST_F(SimplePKTestBasic, MultipleFlushAndWriteWhileMerging)
try
{
    ensureSegmentBreakpoints({0, 50, 100});

    fill(-1000, 1000);

    auto sp_merge_prepared = SyncPointCtl::enableInScope("after_DeltaMergeStore::segmentMerge|prepare_merge");

    auto th_merge = std::async([&]() { ASSERT_TRUE(merge(-10, 110)); });

    sp_merge_prepared.waitAndPause();

    ASSERT_EQ(2000, getRowsN());

    flush(-10, -9);
    ASSERT_EQ(2000, getRowsN());

    fill(0, 100);
    ASSERT_EQ(2000, getRowsN());

    flush(50, 51);
    ASSERT_EQ(2000, getRowsN());

    deleteRange(30, 70);
    ASSERT_EQ(1960, getRowsN());

    sp_merge_prepared.next();
    th_merge.get();

    ASSERT_EQ(1960, getRowsN());
    ASSERT_EQ(1960, getRowsN(-1000, 1000));
}
CATCH

TEST_F(SimplePKTestBasic, MergeWhileFlushing)
try
{
    ensureSegmentBreakpoints({0, 50, 100});

    fill(-1000, 1000);
    ASSERT_EQ(2000, getRowsN());

    auto sp_flush_prepared = SyncPointCtl::enableInScope("after_DeltaValueSpace::flush|prepare_flush");

    auto th_flush = std::async([&]() { flush(10, 11); });

    sp_flush_prepared.waitAndPause();

    ASSERT_TRUE(merge(-10, 110));
    ASSERT_EQ(std::vector<Int64>({}), getSegmentBreakpoints());
    ASSERT_EQ(2000, getRowsN());

    deleteRange(30, 70);
    ASSERT_EQ(1960, getRowsN());

    sp_flush_prepared.next();
    sp_flush_prepared.waitAndNext(); // We expect one flush retry
    th_flush.get();

    ASSERT_EQ(1960, getRowsN());
}
CATCH

} // namespace DB::DM::tests
