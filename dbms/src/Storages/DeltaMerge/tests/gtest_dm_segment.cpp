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
#include <DataStreams/OneBlockInputStream.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ctime>
#include <memory>

#include "dm_basic_include.h"

namespace CurrentMetrics
{
extern const Metric DT_SnapshotOfRead;
extern const Metric DT_SnapshotOfReadRaw;
extern const Metric DT_SnapshotOfSegmentSplit;
extern const Metric DT_SnapshotOfSegmentMerge;
extern const Metric DT_SnapshotOfDeltaMerge;
extern const Metric DT_SnapshotOfPlaceIndex;
} // namespace CurrentMetrics

namespace DB
{
namespace DM
{
extern DMFilePtr writeIntoNewDMFile(DMContext & dm_context, //
                                    const ColumnDefinesPtr & schema_snap,
                                    const BlockInputStreamPtr & input_stream,
                                    UInt64 file_id,
                                    const String & parent_path,
                                    DMFileBlockOutputStream::Flags flags);
namespace tests
{
class Segment_test : public DB::base::TiFlashStorageTestBasic
{
public:
    Segment_test()
        : storage_pool()
    {}

public:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        table_columns_ = std::make_shared<ColumnDefines>();

        segment = reload();
        ASSERT_EQ(segment->segmentId(), DELTA_MERGE_FIRST_SEGMENT_ID);
    }

protected:
    SegmentPtr reload(const ColumnDefinesPtr & pre_define_columns = {}, DB::Settings && db_settings = DB::Settings())
    {
        TiFlashStorageTestBasic::reload(std::move(db_settings));
        storage_path_pool = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        storage_pool = std::make_unique<StoragePool>(*db_context, /*ns_id*/ 100, *storage_path_pool, "test.t1");
        storage_pool->restore();
        ColumnDefinesPtr cols = (!pre_define_columns) ? DMTestEnv::getDefaultColumns() : pre_define_columns;
        setColumns(cols);

        return Segment::newSegment(*dm_context_, table_columns_, RowKeyRange::newAll(false, 1), storage_pool->newMetaPageId(), 0);
    }

    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefinesPtr & columns)
    {
        *table_columns_ = *columns;

        dm_context_ = std::make_unique<DMContext>(*db_context,
                                                  *storage_path_pool,
                                                  *storage_pool,
                                                  0,
                                                  /*min_version_*/ 0,
                                                  settings.not_compress_columns,
                                                  false,
                                                  1,
                                                  db_context->getSettingsRef());
    }

    const ColumnDefinesPtr & tableColumns() const { return table_columns_; }

    DMContext & dmContext() { return *dm_context_; }

protected:
    /// all these var lives as ref in dm_context
    std::unique_ptr<StoragePathPool> storage_path_pool;
    std::unique_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns_;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context_;

    // the segment we are going to test
    SegmentPtr segment;
};

TEST_F(Segment_test, WriteRead)
try
{
    const size_t num_rows_write = 100;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        // write to segment
        segment->write(dmContext(), block);
        // estimate segment
        auto estimatedRows = segment->getEstimatedRows();
        ASSERT_EQ(estimatedRows, block.rows());

        auto estimatedBytes = segment->getEstimatedBytes();
        ASSERT_EQ(estimatedBytes, block.bytes());
    }

    {
        // check segment
        segment->check(dmContext(), "test");
    }

    { // Round 1
        {
            // read written data (only in delta)
            auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            size_t num_rows_read = 0;
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, num_rows_write);
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext(), tableColumns());
        }

        {
            // read written data (only in stable)
            auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            size_t num_rows_read = 0;
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, num_rows_write);
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
            auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            size_t num_rows_read = 0;
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, num_rows_write + num_rows_write_2);
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext(), tableColumns());
        }

        {
            // read written data (only in stable)
            auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            size_t num_rows_read = 0;
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, num_rows_write + num_rows_write_2);
        }
    }
}
CATCH

TEST_F(Segment_test, WriteRead2)
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
        ASSERT_EQ(segment->getStable()->getPacks(), 2);
    }

    {
        Block block = DMTestEnv::prepareBlockWithTso(1, 100, 100 + num_rows_write, false, false);
        segment->write(dmContext(), block);
    }

    {
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
        }
        in->readSuffix();
        // only write two visible pks
        ASSERT_EQ(num_rows_read, 2);
    }
}
CATCH

TEST_F(Segment_test, WriteReadMultiRange)
try
{
    const size_t num_rows_write = 100;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        // write to segment
        segment->write(dmContext(), block);
        // estimate segment
        auto estimatedRows = segment->getEstimatedRows();
        ASSERT_EQ(estimatedRows, block.rows());

        auto estimatedBytes = segment->getEstimatedBytes();
        ASSERT_EQ(estimatedBytes, block.bytes());
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
            auto in = segment->getInputStream(dmContext(), *tableColumns(), read_ranges);
            size_t num_rows_read = 0;
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, expect_read_rows);
        }

        {
            // flush segment
            ASSERT_EQ(segment->getDelta()->getRows(), num_rows_write);
            segment = segment->mergeDelta(dmContext(), tableColumns());
            ASSERT_EQ(segment->getStable()->getRows(), num_rows_write);
        }

        {
            // read written data (only in stable)
            auto in = segment->getInputStream(dmContext(), *tableColumns(), read_ranges);
            size_t num_rows_read = 0;
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, expect_read_rows);
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
            auto in = segment->getInputStream(dmContext(), *tableColumns(), read_ranges);
            size_t num_rows_read = 0;
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, expect_read_rows_2);
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext(), tableColumns());
            ASSERT_EQ(segment->getStable()->getRows(), num_rows_write + num_rows_write_2);
        }

        {
            // read written data (only in stable)
            auto in = segment->getInputStream(dmContext(), *tableColumns(), read_ranges);
            size_t num_rows_read = 0;
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
            }
            in->readSuffix();
            ASSERT_EQ(num_rows_read, expect_read_rows_2);
        }
    }
}
CATCH

TEST_F(Segment_test, ReadWithMoreAdvacedDeltaIndex)
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

    auto check_rows = [&](size_t expected_rows) {
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, expected_rows);
    };

    {
        // check segment
        segment->check(dmContext(), "test");
    }

    // Thread A
    write_rows(100);
    check_rows(100);
    auto snap = segment->createSnapshot(dmContext(), false, CurrentMetrics::DT_SnapshotOfRead);

    // Thread B
    write_rows(100);
    check_rows(200);

    // Thread A
    {
        auto in = segment->getInputStream(
            dmContext(),
            *tableColumns(),
            snap,
            {RowKeyRange::newAll(false, 1)},
            {},
            std::numeric_limits<UInt64>::max(),
            DEFAULT_BLOCK_SIZE);
        int num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, 100);
    }
}
CATCH

class SegmentDeletionRelevantPlace_test
    : public Segment_test
    , public testing::WithParamInterface<bool>
{
    DB::Settings getSettings()
    {
        DB::Settings settings;
        auto enable_relevant_place = GetParam();

        if (enable_relevant_place)
            settings.set("dt_enable_relevant_place", "1");
        else
            settings.set("dt_enable_relevant_place", "0");
        return settings;
    }
};


TEST_P(SegmentDeletionRelevantPlace_test, ShareDelteRangeIndex)
try
{
    const size_t num_rows_write = 300;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(dmContext(), std::move(block));
    }

    auto get_rows = [&](const RowKeyRange & range) {
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {range});
        in->readPrefix();
        size_t rows = 0;
        while (Block block = in->read())
        {
            rows += block.rows();
        }
        in->readSuffix();

        return rows;
    };

    // First place the block packs, so that we can only place DeleteRange below.
    get_rows(RowKeyRange::fromHandleRange(HandleRange::newAll()));

    {
        HandleRange remove(100, 200);
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(remove)});
    }

    // The first call of get_rows below will place the DeleteRange into delta index.
    auto rows1 = get_rows(RowKeyRange::fromHandleRange(HandleRange(0, 150)));
    auto rows2 = get_rows(RowKeyRange::fromHandleRange(HandleRange(150, 300)));

    ASSERT_EQ(rows1, (size_t)100);
    ASSERT_EQ(rows2, (size_t)100);
}
CATCH

INSTANTIATE_TEST_CASE_P(WhetherEnableRelevantPlace, SegmentDeletionRelevantPlace_test, testing::Values(true, false));

class SegmentDeletion_test
    : public Segment_test
    , public testing::WithParamInterface<std::tuple<bool, bool>>
{
};

TEST_P(SegmentDeletion_test, DeleteDataInDelta)
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), 2UL);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    EXPECT_EQ(c->getInt(0), 0);
                    EXPECT_EQ(c->getInt(1), 99);
                }
            }
        }
        in->readSuffix();
    }

    // For the case that apply merge delta after delete range, we ensure that data on disk are compacted
    if (merge_delta_after_delete)
    {
        // read raw after delete range
        auto in = segment->getInputStreamRaw(dmContext(), *tableColumns());
        in->readPrefix();
        size_t num_rows = 0;
        while (Block block = in->read())
        {
            num_rows += block.rows();
        }
        in->readSuffix();
        // Only 2 rows are left on disk, others are compacted.
        ASSERT_EQ(num_rows, 2UL);
    }
}
CATCH

TEST_P(SegmentDeletion_test, DeleteDataInStable)
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), 2UL);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    EXPECT_EQ(c->getInt(0), 0);
                    EXPECT_EQ(c->getInt(1), 99);
                }
            }
        }
        in->readSuffix();
    }

    // For the case that apply merge delta after delete range, we ensure that data on disk are compacted
    if (merge_delta_after_delete)
    {
        // read raw after delete range
        auto in = segment->getInputStreamRaw(dmContext(), *tableColumns());
        in->readPrefix();
        size_t num_rows = 0;
        while (Block block = in->read())
        {
            num_rows += block.rows();
        }
        in->readSuffix();
        // Only 2 rows are left on disk, others are compacted.
        ASSERT_EQ(num_rows, 2UL);
    }
}
CATCH

TEST_P(SegmentDeletion_test, DeleteDataInStableAndDelta)
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), 2UL);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    EXPECT_EQ(c->getInt(0), 0);
                    EXPECT_EQ(c->getInt(1), 99);
                }
            }
        }
        in->readSuffix();
    }

    // For the case that apply merge delta after delete range, we ensure that data on disk are compacted
    if (merge_delta_after_delete)
    {
        // read raw after delete range
        auto in = segment->getInputStreamRaw(dmContext(), *tableColumns());
        size_t num_rows = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows += block.rows();
        }
        in->readSuffix();
        // Only 2 rows are left on disk, others are compacted.
        ASSERT_EQ(num_rows, 2UL);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(WhetherReadOrMergeDeltaBeforeDeleteRange, SegmentDeletion_test, testing::Combine(testing::Bool(), testing::Bool()));

TEST_F(Segment_test, DeleteRead)
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
        auto squash_range = snap->delta->getSquashDeleteRange();
        ASSERT_ROWKEY_RANGE_EQ(squash_range, RowKeyRange::fromHandleRange(expect_range));
    };

    {
        // Test delete range [70, 100)
        HandleRange del{70, 100};
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        SCOPED_TRACE("check after range: " + del.toDebugString()); // Add trace msg when ASSERT failed
        check_segment_squash_delete_range(segment, HandleRange{70, 100});

        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        // The deleted range has no overlap with current data, so there should be no change
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write);
            for (auto & iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); i++)
                {
                    if (iter.name == DMTestEnv::pk_name)
                    {
                        EXPECT_EQ(c->getInt(i), i);
                    }
                }
            }
        }
        in->readSuffix();
    }

    {
        // Test delete range [63, 70)
        HandleRange del{63, 70};
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        SCOPED_TRACE("check after range: " + del.toDebugString());
        check_segment_squash_delete_range(segment, HandleRange{63, 100});

        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        // The deleted range has overlap range [63, 64) with current data, so the record with Handle 63 should be deleted
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 1);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    EXPECT_EQ(c->getInt(0), 0);
                    EXPECT_EQ(c->getInt(62), 62);
                }
                EXPECT_EQ(c->size(), 63UL);
            }
        }
        in->readSuffix();
    }

    {
        // Test delete range [1, 32)
        HandleRange del{1, 32};
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        SCOPED_TRACE("check after range: " + del.toDebugString());
        check_segment_squash_delete_range(segment, HandleRange{1, 100});
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 32);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    EXPECT_EQ(c->getInt(0), 0);
                    EXPECT_EQ(c->getInt(1), 32);
                }
            }
        }
        in->readSuffix();
    }

    {
        // Test delete range [1, 32)
        // delete should be idempotent
        HandleRange del{1, 32};
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        SCOPED_TRACE("check after range: " + del.toDebugString());
        check_segment_squash_delete_range(segment, HandleRange{1, 100});
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 32);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    EXPECT_EQ(c->getInt(0), 0);
                    EXPECT_EQ(c->getInt(1), 32);
                }
            }
        }
        in->readSuffix();
    }

    {
        // Test delete range [0, 2)
        // There is an overlap range [0, 1)
        HandleRange del{0, 2};
        segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        SCOPED_TRACE("check after range: " + del.toDebugString());
        check_segment_squash_delete_range(segment, HandleRange{0, 100});
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 33);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    EXPECT_EQ(c->getInt(0), 32);
                }
            }
        }
        in->readSuffix();
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 33 + 7);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    EXPECT_EQ(c->getInt(0), 9);
                    EXPECT_EQ(c->getInt(6), 15);
                    EXPECT_EQ(c->getInt(7), 32);
                    EXPECT_EQ(c->getInt(block.rows() - 1), 62);
                }
            }
        }
        in->readSuffix();
    }
}
CATCH

TEST_F(Segment_test, Split)
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});

        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
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

    size_t num_rows_seg1 = 0;
    size_t num_rows_seg2 = 0;
    {
        {
            auto in = segment->getInputStream(dmContext(), *tableColumns(), {segment->getRowKeyRange()});
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_seg1 += block.rows();
            }
            in->readSuffix();
        }
        {
            auto in = new_segment->getInputStream(dmContext(), *tableColumns(), {new_segment->getRowKeyRange()});
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_seg2 += block.rows();
            }
            in->readSuffix();
        }
        ASSERT_EQ(num_rows_seg1 + num_rows_seg2, num_rows_write);
    }

    // delete rows in the right segment
    {
        new_segment->write(dmContext(), /*delete_range*/ new_segment->getRowKeyRange());
        new_segment->flushCache(dmContext());
    }

    // merge segments
    {
        segment = Segment::merge(dmContext(), tableColumns(), segment, new_segment);
        {
            // check merged segment range
            const auto & merged_range = segment->getRowKeyRange();
            EXPECT_EQ(*merged_range.start.value, *s1_range.start.value);
            EXPECT_EQ(*merged_range.end.value, *s2_range.end.value);
            // TODO check segment epoch is increase
        }
        {
            size_t num_rows_read = 0;
            auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
            }
            in->readSuffix();
            EXPECT_EQ(num_rows_read, num_rows_seg1);
        }
    }
}
CATCH

TEST_F(Segment_test, SplitFail)
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

TEST_F(Segment_test, Restore)
try
{
    // compare will compares the given segments.
    // If they are equal, result will be true, otherwise it will be false.
    auto compare = [&](const SegmentPtr & seg1, const SegmentPtr & seg2, bool & result) {
        result = false;
        auto in1 = seg1->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        auto in2 = seg2->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
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

                for (Int64 i = 0; i < Int64(c1->size()); i++)
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

    SegmentPtr new_segment = Segment::restoreSegment(dmContext(), segment->segmentId());

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
        new_segment = segment->restoreSegment(dmContext(), segment->segmentId());
    }

    {
        // test compare
        bool result;
        compare(new_segment, new_segment, result);
        ASSERT_TRUE(result);
    }
}
CATCH

TEST_F(Segment_test, MassiveSplit)
try
{
    Settings settings = dmContext().db_context.getSettings();
    settings.dt_segment_limit_rows = 11;
    settings.dt_segment_delta_limit_rows = 7;

    segment = reload(DMTestEnv::getDefaultColumns(), std::move(settings));

    size_t num_batches_written = 0;
    const size_t num_rows_per_write = 5;

    const time_t start_time = std::time(nullptr);

    auto temp = std::vector<Int64>();
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
            HandleRange del{Int64((num_batches_written - 1) * num_rows_per_write),
                            Int64((num_batches_written - 1) * num_rows_per_write + 2)};
            segment->write(dmContext(), {RowKeyRange::fromHandleRange(del)});
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext(), tableColumns());
        }

        for (size_t i = (num_batches_written - 1) * num_rows_per_write + 2; i < num_batches_written * num_rows_per_write; i++)
        {
            temp.push_back(Int64(i));
        }

        {
            // Read after writing
            auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
            size_t num_rows_read = 0;
            in->readPrefix();
            while (Block block = in->read())
            {
                for (auto & iter : block)
                {
                    auto c = iter.column;
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        if (iter.name == DMTestEnv::pk_name)
                        {
                            auto expect = temp.at(i + num_rows_read);
                            EXPECT_EQ(c->getInt(Int64(i)), expect);
                        }
                    }
                }
                num_rows_read += block.rows();
            }
            in->readSuffix();
            ASSERT_EQ(num_batches_written * (num_rows_per_write - 2), num_rows_read);
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

enum Segment_test_Mode
{
    V1_BlockOnly,
    V2_BlockOnly,
    V2_FileOnly,
};

String testModeToString(const ::testing::TestParamInfo<Segment_test_Mode> & info)
{
    const auto mode = info.param;
    switch (mode)
    {
    case Segment_test_Mode::V1_BlockOnly:
        return "V1_BlockOnly";
    case Segment_test_Mode::V2_BlockOnly:
        return "V2_BlockOnly";
    case Segment_test_Mode::V2_FileOnly:
        return "V2_FileOnly";
    default:
        return "Unknown";
    }
}

class Segment_test_2 : public Segment_test
    , public testing::WithParamInterface<Segment_test_Mode>
{
public:
    Segment_test_2()
        : Segment_test()
    {}

    void SetUp() override
    {
        mode = GetParam();
        switch (mode)
        {
        case Segment_test_Mode::V1_BlockOnly:
            setStorageFormat(1);
            break;
        case Segment_test_Mode::V2_BlockOnly:
        case Segment_test_Mode::V2_FileOnly:
            setStorageFormat(2);
            break;
        }

        Segment_test::SetUp();
    }

    std::pair<RowKeyRange, PageIds> genDMFile(DMContext & context, const Block & block)
    {
        auto delegator = context.path_pool.getStableDiskDelegator();
        auto file_id = context.storage_pool.newDataPageIdForDTFile(delegator, __PRETTY_FUNCTION__);
        auto input_stream = std::make_shared<OneBlockInputStream>(block);
        auto store_path = delegator.choosePath();

        DMFileBlockOutputStream::Flags flags;
        flags.setSingleFile(DMTestEnv::getPseudoRandomNumber() % 2);

        auto dmfile
            = writeIntoNewDMFile(context, std::make_shared<ColumnDefines>(*tableColumns()), input_stream, file_id, store_path, flags);

        delegator.addDTFile(file_id, dmfile->getBytesOnDisk(), store_path);

        auto & pk_column = block.getByPosition(0).column;
        auto min_pk = pk_column->getInt(0);
        auto max_pk = pk_column->getInt(block.rows() - 1);
        HandleRange range(min_pk, max_pk + 1);

        return {RowKeyRange::fromHandleRange(range), {file_id}};
    }

    Segment_test_Mode mode;
};

TEST_P(Segment_test_2, FlushDuringSplitAndMerge)
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
            case Segment_test_Mode::V1_BlockOnly:
            case Segment_test_Mode::V2_BlockOnly:
                segment->write(dmContext(), std::move(block));
                break;
            case Segment_test_Mode::V2_FileOnly:
            {
                auto delegate = dmContext().path_pool.getStableDiskDelegator();
                auto file_provider = dmContext().db_context.getFileProvider();
                auto [range, file_ids] = genDMFile(dmContext(), block);
                auto file_id = file_ids[0];
                auto file_parent_path = delegate.getDTFilePath(file_id);
                auto file = DMFile::restore(file_provider, file_id, file_id, file_parent_path, DMFile::ReadMetaMode::all());
                auto column_file = std::make_shared<ColumnFileBig>(dmContext(), file, range);
                WriteBatches wbs(*storage_pool);
                wbs.data.putExternal(file_id, 0);
                wbs.writeLogAndData();

                segment->ingestColumnFiles(dmContext(), range, {column_file}, false);
                break;
            }
            default:
                throw Exception("Unsupported");
            }

            segment->flushCache(dmContext());
        }
    };

    auto read_rows = [&](const SegmentPtr & segment) {
        size_t rows = 0;
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(false, 1)});
        in->readPrefix();
        while (Block block = in->read())
        {
            rows += block.rows();
        }
        in->readSuffix();
        return rows;
    };

    write_100_rows(segment);

    // Test split
    SegmentPtr other_segment;
    {
        WriteBatches wbs(dmContext().storage_pool);
        auto segment_snap = segment->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfSegmentSplit);
        ASSERT_FALSE(!segment_snap);

        write_100_rows(segment);

        auto split_info = segment->prepareSplit(dmContext(), tableColumns(), segment_snap, wbs);

        wbs.writeLogAndData();
        split_info->my_stable->enableDMFilesGC();
        split_info->other_stable->enableDMFilesGC();

        auto lock = segment->mustGetUpdateLock();
        std::tie(segment, other_segment) = segment->applySplit(dmContext(), segment_snap, wbs, split_info.value());

        wbs.writeAll();
    }

    {
        SegmentPtr new_segment_1 = Segment::restoreSegment(dmContext(), segment->segmentId());
        SegmentPtr new_segment_2 = Segment::restoreSegment(dmContext(), other_segment->segmentId());
        auto rows1 = read_rows(new_segment_1);
        auto rows2 = read_rows(new_segment_2);
        ASSERT_EQ(rows1 + rows2, (size_t)200);
    }

    // Test merge
    {
        WriteBatches wbs(dmContext().storage_pool);

        auto left_snap = segment->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
        auto right_snap = other_segment->createSnapshot(dmContext(), true, CurrentMetrics::DT_SnapshotOfSegmentMerge);
        ASSERT_FALSE(!left_snap || !right_snap);

        write_100_rows(other_segment);
        segment->flushCache(dmContext());

        auto merged_stable = Segment::prepareMerge(dmContext(), tableColumns(), segment, left_snap, other_segment, right_snap, wbs);

        wbs.writeLogAndData();
        merged_stable->enableDMFilesGC();

        auto left_lock = segment->mustGetUpdateLock();
        auto right_lock = other_segment->mustGetUpdateLock();

        segment = Segment::applyMerge(dmContext(), segment, left_snap, other_segment, right_snap, wbs, merged_stable);

        wbs.writeAll();
    }

    {
        SegmentPtr new_segment = Segment::restoreSegment(dmContext(), segment->segmentId());
        auto rows = read_rows(new_segment);
        ASSERT_EQ(rows, (size_t)300);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(Segment_test_Mode, //
                        Segment_test_2,
                        testing::Values(Segment_test_Mode::V1_BlockOnly, Segment_test_Mode::V2_BlockOnly, Segment_test_Mode::V2_FileOnly),
                        testModeToString);

enum class SegmentWriteType
{
    ToDisk,
    ToCache
};
class Segment_DDL_test
    : public Segment_test
    , public testing::WithParamInterface<std::tuple<SegmentWriteType, bool>>
{
};
String paramToString(const ::testing::TestParamInfo<Segment_DDL_test::ParamType> & info)
{
    const auto [write_type, flush_before_ddl] = info.param;

    String name = (write_type == SegmentWriteType::ToDisk) ? "ToDisk_" : "ToCache";
    name += (flush_before_ddl ? "_FlushCache" : "_NotFlushCache");
    return name;
}

/// Mock a col from i8 -> i32
TEST_P(Segment_DDL_test, AlterInt8ToInt32)
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
        segment = reload(columns_before_ddl, std::move(db_settings));

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
        auto in = segment->getInputStream(dmContext(), *columns_to_read, {RowKeyRange::newAll(false, 1)});

        // check that we can read correct values
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(column_name_i8_to_i32));
            const ColumnWithTypeAndName & col = block.getByName(column_name_i8_to_i32);
            ASSERT_DATATYPE_EQ(col.type, column_i32_after_ddl.type);
            ASSERT_EQ(col.name, column_i32_after_ddl.name);
            ASSERT_EQ(col.column_id, column_i32_after_ddl.id);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                auto value = col.column->getInt(i);
                const auto expected = static_cast<int64_t>((i % 2 == 0 ? -1 : 1) * i);
                ASSERT_EQ(value, expected) << "at row: " << i;
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
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
        auto in = segment->getInputStream(dmContext(), *columns_to_read, {RowKeyRange::newAll(false, 1)});

        // check that we can read correct values
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(column_name_i8_to_i32));
            const ColumnWithTypeAndName & col = block.getByName(column_name_i8_to_i32);
            ASSERT_DATATYPE_EQ(col.type, column_i32_after_ddl.type);
            ASSERT_EQ(col.name, column_i32_after_ddl.name);
            ASSERT_EQ(col.column_id, column_i32_after_ddl.id);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                auto value = col.column->getInt(i);
                auto expected = 0;
                if (i < num_rows_write / 2)
                    expected = static_cast<int64_t>((i % 2 == 0 ? -1 : 1) * i);
                else
                {
                    auto r = i - num_rows_write / 2;
                    expected = static_cast<int64_t>((r % 2 == 0 ? -1 : 1) * r);
                }
                // std::cerr << " row: " << i << "  "<< value << std::endl;
                ASSERT_EQ(value, expected) << "at row: " << i;
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, (size_t)(num_rows_write * 2));
    }

    // Flush cache and apply delta-merge, then read again
    // This will create a new stable with new schema, check the data.
    {
        segment->flushCache(dmContext());
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // check the stable data with new schema
        auto in = segment->getInputStream(dmContext(), *columns_to_read, {RowKeyRange::newAll(false, 1)});

        // check that we can read correct values
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(column_name_i8_to_i32));
            const ColumnWithTypeAndName & col = block.getByName(column_name_i8_to_i32);
            ASSERT_DATATYPE_EQ(col.type, column_i32_after_ddl.type);
            ASSERT_EQ(col.name, column_i32_after_ddl.name);
            ASSERT_EQ(col.column_id, column_i32_after_ddl.id);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                auto value = col.column->getInt(i);
                auto expected = 0;
                if (i < num_rows_write / 2)
                    expected = static_cast<int64_t>((i % 2 == 0 ? -1 : 1) * i);
                else
                {
                    auto r = i - num_rows_write / 2;
                    expected = static_cast<int64_t>((r % 2 == 0 ? -1 : 1) * r);
                }
                // std::cerr << " row: " << i << "  "<< value << std::endl;
                ASSERT_EQ(value, expected) << "at row: " << i;
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, (size_t)(num_rows_write * 2));
    }
}
CATCH

TEST_P(Segment_DDL_test, AddColumn)
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
        segment = reload(columns_before_ddl, std::move(db_settings));
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
        auto in = segment->getInputStream(dmContext(), *columns_after_ddl, {RowKeyRange::newAll(false, 1)});

        // check that we can read correct values
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            const ColumnWithTypeAndName & col = block.getByName(new_column_define.name);
            ASSERT_TRUE(col.type->equals(*new_column_define.type));
            ASSERT_EQ(col.name, new_column_define.name);
            ASSERT_EQ(col.column_id, new_column_define.id);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                auto value = col.column->getInt(i);
                ASSERT_EQ(value, new_column_default_value_int) << "at row:" << i;
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
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
        auto in = segment->getInputStream(dmContext(), *columns_after_ddl, {RowKeyRange::newAll(false, 1)});

        // check that we can read correct values
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(new_column_name));
            const ColumnWithTypeAndName & col = block.getByName(new_column_name);
            ASSERT_DATATYPE_EQ(col.type, new_column_define.type);
            ASSERT_EQ(col.name, new_column_define.name);
            ASSERT_EQ(col.column_id, new_column_define.id);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                int8_t value = col.column->getInt(i);
                int8_t expected = 0;
                if (i < num_rows_write / 2)
                    expected = new_column_default_value_int;
                else
                {
                    auto r = i - num_rows_write / 2;
                    expected = static_cast<int8_t>((r % 2 == 0 ? -1 : 1) * r);
                }
                // std::cerr << " row: " << i << "  "<< value << std::endl;
                ASSERT_EQ(value, expected) << "at row: " << i;
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, (size_t)(num_rows_write * 2));
    }

    // Flush cache and apply delta-merge, then read again
    // This will create a new stable with new schema, check the data.
    {
        segment->flushCache(dmContext());
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // read written data after delta-merge
        auto in = segment->getInputStream(dmContext(), *columns_after_ddl, {RowKeyRange::newAll(false, 1)});

        // check that we can read correct values
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            ASSERT_TRUE(block.has(new_column_name));
            const ColumnWithTypeAndName & col = block.getByName(new_column_name);
            ASSERT_DATATYPE_EQ(col.type, new_column_define.type);
            ASSERT_EQ(col.name, new_column_define.name);
            ASSERT_EQ(col.column_id, new_column_define.id);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                int8_t value = col.column->getInt(i);
                int8_t expected = 0;
                if (i < num_rows_write / 2)
                    expected = new_column_default_value_int;
                else
                {
                    auto r = i - num_rows_write / 2;
                    expected = static_cast<int8_t>((r % 2 == 0 ? -1 : 1) * r);
                }
                // std::cerr << " row: " << i << "  "<< value << std::endl;
                ASSERT_EQ(value, expected) << "at row: " << i;
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, (size_t)(num_rows_write * 2));
    }
}
CATCH

TEST_F(Segment_test, CalculateDTFileProperty)
try
{
    Settings settings = dmContext().db_context.getSettings();
    settings.dt_segment_stable_pack_rows = 10;

    segment = reload(DMTestEnv::getDefaultColumns(), std::move(settings));

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

TEST_F(Segment_test, CalculateDTFilePropertyWithPropertyFileDeleted)
try
{
    Settings settings = dmContext().db_context.getSettings();
    settings.dt_segment_stable_pack_rows = 10;

    segment = reload(DMTestEnv::getDefaultColumns(), std::move(settings));

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
        auto & stable = segment->getStable();
        auto & dmfiles = stable->getDMFiles();
        ASSERT_GT(dmfiles[0]->getPacks(), (size_t)1);
        auto & dmfile = dmfiles[0];
        auto file_path = dmfile->path();
        // check property file exists and then delete it
        ASSERT_EQ(Poco::File(file_path + "/property").exists(), true);
        Poco::File(file_path + "/property").remove();
        ASSERT_EQ(Poco::File(file_path + "/property").exists(), false);
        // clear PackProperties to force it to calculate from scratch
        dmfile->getPackProperties().clear_property();
        ASSERT_EQ(dmfile->getPackProperties().property_size(), 0);
        // caculate StableProperty
        ASSERT_EQ(stable->isStablePropertyCached(), false);
        auto start = RowKeyValue::fromHandle(0);
        auto end = RowKeyValue::fromHandle(num_rows_write_every_round);
        RowKeyRange range(start, end, false, 1);
        // calculate the StableProperty for packs in the key range [0, num_rows_write_every_round)
        stable->calculateStableProperty(dmContext(), range, false);
        ASSERT_EQ(stable->isStablePropertyCached(), true);
        auto & property = stable->getStableProperty();
        ASSERT_EQ(property.gc_hint_version, std::numeric_limits<UInt64>::max());
        ASSERT_EQ(property.num_versions, num_rows_write_every_round);
        ASSERT_EQ(property.num_puts, num_rows_write_every_round);
        ASSERT_EQ(property.num_rows, num_rows_write_every_round);
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(SegmentWriteType,
                        Segment_DDL_test,
                        ::testing::Combine( //
                            ::testing::Values(SegmentWriteType::ToDisk, SegmentWriteType::ToCache),
                            ::testing::Bool()),
                        paramToString);

} // namespace tests
} // namespace DM
} // namespace DB
