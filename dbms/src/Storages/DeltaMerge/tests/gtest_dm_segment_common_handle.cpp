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

#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/StoragePool.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/PathPool.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>

#include <ctime>
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
namespace DB::DM::tests
{

class SegmentCommonHandleTest : public DB::base::TiFlashStorageTestBasic
{
public:
    SegmentCommonHandleTest() = default;

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
    SegmentPtr reload(ColumnDefinesPtr cols = {}, DB::Settings && db_settings = DB::Settings())
    {
        TiFlashStorageTestBasic::reload(std::move(db_settings));
        path_pool = std::make_shared<StoragePathPool>(db_context->getPathPool().withTable("test", "t", false));
        storage_pool = std::make_shared<StoragePool>(*db_context, NullspaceID, /*table_id*/ 100, *path_pool, "test.t1");
        storage_pool->restore();
        if (!cols)
            cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        setColumns(cols);

        auto segment_id = storage_pool->newMetaPageId();
        return Segment::newSegment(Logger::get(), *dm_context_, table_columns_, RowKeyRange::newAll(is_common_handle, rowkey_column_size), segment_id, 0);
    }

    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefinesPtr & columns)
    {
        *table_columns_ = *columns;

        dm_context_ = std::make_unique<DMContext>(*db_context,
                                                  path_pool,
                                                  storage_pool,
                                                  /*min_version_*/ 0,
                                                  NullspaceID,
                                                  /*physical_table_id*/ 100,
                                                  is_common_handle,
                                                  rowkey_column_size,
                                                  db_context->getSettingsRef());
    }

    const ColumnDefinesPtr & tableColumns() const { return table_columns_; }

    DMContext & dmContext() { return *dm_context_; }

private:
    /// all these var lives as ref in dm_context
    std::shared_ptr<StoragePathPool> path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    ColumnDefinesPtr table_columns_;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context_;

protected:
    // the segment we are going to test
    SegmentPtr segment;
    bool is_common_handle = true;
    const size_t rowkey_column_size = 2;
};

TEST_F(SegmentCommonHandleTest, WriteRead)
try
{
    const size_t num_rows_write = 100;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0,
                                                         num_rows_write,
                                                         false,
                                                         2,
                                                         EXTRA_HANDLE_COLUMN_NAME,
                                                         EXTRA_HANDLE_COLUMN_ID,
                                                         EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                         is_common_handle,
                                                         rowkey_column_size);
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
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
            ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext(), tableColumns());
        }

        {
            // read written data (only in stable)
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
            ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
        }
    }

    const size_t num_rows_write_2 = 55;

    {
        // write more rows to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write,
                                                         num_rows_write + num_rows_write_2,
                                                         false,
                                                         2,
                                                         EXTRA_HANDLE_COLUMN_NAME,
                                                         EXTRA_HANDLE_COLUMN_ID,
                                                         EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                         is_common_handle,
                                                         rowkey_column_size);
        segment->write(dmContext(), std::move(block));
    }

    { // Round 2
        {
            // read written data (both in delta and stable)
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
            ASSERT_INPUTSTREAM_NROWS(in, num_rows_write + num_rows_write_2);
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext(), tableColumns());
        }

        {
            // read written data (only in stable)
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
            ASSERT_INPUTSTREAM_NROWS(in, num_rows_write + num_rows_write_2);
        }
    }
}
CATCH

TEST_F(SegmentCommonHandleTest, WriteReadMultiRange)
try
{
    const size_t num_rows_write = 100;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0,
                                                         num_rows_write,
                                                         false,
                                                         2,
                                                         EXTRA_HANDLE_COLUMN_NAME,
                                                         EXTRA_HANDLE_COLUMN_ID,
                                                         EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                         is_common_handle,
                                                         rowkey_column_size);
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

    RowKeyRanges read_ranges{
        (RowKeyRange::fromHandleRange(HandleRange(0, 10), true)),
        (RowKeyRange::fromHandleRange(HandleRange(20, 30), true)),
        (RowKeyRange::fromHandleRange(HandleRange(110, 130), true)),
    };
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
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write,
                                                         num_rows_write + num_rows_write_2,
                                                         false,
                                                         2,
                                                         EXTRA_HANDLE_COLUMN_NAME,
                                                         EXTRA_HANDLE_COLUMN_ID,
                                                         EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                         is_common_handle,
                                                         rowkey_column_size);
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

class SegmentDeletion_Common_Handle_test : public SegmentCommonHandleTest
    , public testing::WithParamInterface<std::tuple<bool, bool>>
{
};

TEST_P(SegmentDeletion_Common_Handle_test, DeleteDataInDelta)
try
{
    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0,
                                                         num_rows_write,
                                                         false,
                                                         2,
                                                         EXTRA_HANDLE_COLUMN_NAME,
                                                         EXTRA_HANDLE_COLUMN_ID,
                                                         EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                         is_common_handle,
                                                         rowkey_column_size);
        segment->write(dmContext(), std::move(block));
    }

    auto [read_before_delete, merge_delta_after_delete] = GetParam();
    if (read_before_delete)
    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
    }

    {
        // test delete range [1,99) for data in delta
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(1, 99, rowkey_column_size)});
        // TODO test delete range partial overlap with segment
        // TODO test delete range not included by segment
    }

    if (merge_delta_after_delete)
    {
        // flush segment for apply delete range
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // read after delete range
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        const size_t nrows_after_delete = 2;
        // mock common handle
        auto common_handle_coldata = [this]() {
            auto tmp = std::vector<Int64>{0, 99};
            Strings res;
            std::transform(tmp.begin(), tmp.end(), std::back_inserter(res), [this](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), nrows_after_delete);
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }
}
CATCH

TEST_P(SegmentDeletion_Common_Handle_test, DeleteDataInStable)
try
{
    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0,
                                                         num_rows_write,
                                                         false,
                                                         2,
                                                         EXTRA_HANDLE_COLUMN_NAME,
                                                         EXTRA_HANDLE_COLUMN_ID,
                                                         EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                         is_common_handle,
                                                         rowkey_column_size);
        segment->write(dmContext(), std::move(block));
    }

    auto [read_before_delete, merge_delta_after_delete] = GetParam();
    if (read_before_delete)
    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
    }

    {
        // flush segment
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // test delete range [1,99) for data in stable
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(1, 99, rowkey_column_size)});
        // TODO test delete range partial overlap with segment
        // TODO test delete range not included by segment

        // flush segment
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    if (merge_delta_after_delete)
    {
        // flush segment for apply delete range
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // read after delete range
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        const size_t nrows_after_delete = 2;
        // mock common handle
        auto common_handle_coldata = [this]() {
            auto tmp = std::vector<Int64>{0, 99};
            Strings res;
            std::transform(tmp.begin(), tmp.end(), std::back_inserter(res), [this](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), nrows_after_delete);
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }
}
CATCH

TEST_P(SegmentDeletion_Common_Handle_test, DeleteDataInStableAndDelta)
try
{
    const size_t num_rows_write = 100;
    {
        // write [0, 50) to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0,
                                                         num_rows_write / 2,
                                                         false,
                                                         2,
                                                         EXTRA_HANDLE_COLUMN_NAME,
                                                         EXTRA_HANDLE_COLUMN_ID,
                                                         EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                         is_common_handle,
                                                         rowkey_column_size);
        segment->write(dmContext(), std::move(block));
        // flush [0, 50) to segment's stable
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    auto [read_before_delete, merge_delta_after_delete] = GetParam();

    {
        // write [50, 100) to segment's delta
        Block block = DMTestEnv::prepareSimpleWriteBlock(num_rows_write / 2,
                                                         num_rows_write,
                                                         false,
                                                         2,
                                                         EXTRA_HANDLE_COLUMN_NAME,
                                                         EXTRA_HANDLE_COLUMN_ID,
                                                         EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                         is_common_handle,
                                                         rowkey_column_size);
        segment->write(dmContext(), std::move(block));
    }

    if (read_before_delete)
    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
    }

    {
        // test delete range [1,99) for data in stable and delta
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(1, 99, rowkey_column_size)});
        // TODO test delete range partial overlap with segment
        // TODO test delete range not included by segment
    }

    if (merge_delta_after_delete)
    {
        // flush segment for apply delete range
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // read after delete range
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        const size_t nrows_after_delete = 2;
        // mock common handle
        auto common_handle_coldata = [this]() {
            auto tmp = std::vector<Int64>{0, 99};
            Strings res;
            std::transform(tmp.begin(), tmp.end(), std::back_inserter(res), [this](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), nrows_after_delete);
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(WhetherReadOrMergeDeltaBeforeDeleteRange,
                        SegmentDeletion_Common_Handle_test,
                        testing::Combine(testing::Bool(), testing::Bool()));

TEST_F(SegmentCommonHandleTest, DeleteRead)
try
{
    const size_t num_rows_write = 64;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0,
                                                         num_rows_write,
                                                         false,
                                                         2,
                                                         EXTRA_HANDLE_COLUMN_NAME,
                                                         EXTRA_HANDLE_COLUMN_ID,
                                                         EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                         is_common_handle,
                                                         rowkey_column_size);
        segment->write(dmContext(), std::move(block));
    }

    {
        // do delta-merge move data to stable
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    auto check_segment_squash_delete_range = [this](SegmentPtr & segment, const RowKeyRange & expect_range) {
        // set `is_update=false` to get full squash delete range
        auto snap = segment->createSnapshot(dmContext(), /*for_update*/ false, CurrentMetrics::DT_SnapshotOfRead);
        auto squash_range = snap->delta->getSquashDeleteRange(is_common_handle, rowkey_column_size);
        ASSERT_ROWKEY_RANGE_EQ(squash_range, expect_range);
    };

    {
        // Test delete range [70, 100)
        auto del_row_range = DMTestEnv::getRowKeyRangeForClusteredIndex(70, 100, rowkey_column_size);
        SCOPED_TRACE("check after range: " + del_row_range.toDebugString()); // Add trace msg when ASSERT failed
        // mem-table
        segment->write(dmContext(), {del_row_range});
        check_segment_squash_delete_range(segment, del_row_range);
        // persisted-file
        segment->flushCache(dmContext());
        check_segment_squash_delete_range(segment, del_row_range);
    }

    {
        // Read after deletion
        // The deleted range has no overlap with current data, so there should be no change
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        // mock common handle
        auto common_handle_coldata = [this]() {
            auto tmp = createNumbers<Int64>(0, num_rows_write);
            Strings res;
            std::transform(tmp.begin(), tmp.end(), std::back_inserter(res), [this](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), num_rows_write);
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }

    {
        // Test delete range [63, 70)
        auto del_row_range = DMTestEnv::getRowKeyRangeForClusteredIndex(63, 70, rowkey_column_size);
        auto merged_del_range = DMTestEnv::getRowKeyRangeForClusteredIndex(63, 100, rowkey_column_size);
        SCOPED_TRACE("check after range: " + del_row_range.toDebugString()); // Add trace msg when ASSERT failed
        // mem-table
        segment->write(dmContext(), {del_row_range});
        check_segment_squash_delete_range(segment, merged_del_range);
        // persisted-file
        segment->flushCache(dmContext());
        check_segment_squash_delete_range(segment, merged_del_range);
    }

    {
        // Read after deletion
        // The deleted range has overlap range [63, 64) with current data, so the record with Handle 63 should be deleted
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        // mock common handle
        auto common_handle_coldata = [this]() {
            std::vector<Int64> int_coldata = createNumbers<Int64>(0, 63);
            Strings res;
            std::transform(int_coldata.begin(), int_coldata.end(), std::back_inserter(res), [this](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_EQ(common_handle_coldata.size(), num_rows_write - 1);
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }

    {
        // Test delete range [1, 32)
        auto del_row_range = DMTestEnv::getRowKeyRangeForClusteredIndex(1, 32, rowkey_column_size);
        SCOPED_TRACE("check after range: " + del_row_range.toDebugString()); // Add trace msg when ASSERT failed
        segment->write(dmContext(), {del_row_range});
        auto merged_del_range = DMTestEnv::getRowKeyRangeForClusteredIndex(
            1,
            100,
            rowkey_column_size); // suqash_delete_range will consider [1, 100) maybe deleted
        check_segment_squash_delete_range(segment, merged_del_range);
    }

    {
        // Read after deletion
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        // mock common handle
        auto common_handle_coldata = [this]() {
            // the result should be [0, 32,33,34,...62]
            std::vector<Int64> int_coldata{0};
            auto tmp = createNumbers<Int64>(32, 63);
            int_coldata.insert(int_coldata.end(), tmp.begin(), tmp.end());
            Strings res;
            std::transform(int_coldata.begin(), int_coldata.end(), std::back_inserter(res), [this](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }

    {
        // Test delete range [1, 32)
        // delete should be idempotent
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(1, 32, rowkey_column_size)});
        // flush segment
        segment->flushCache(dmContext());
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        // mock common handle
        auto common_handle_coldata = [this]() {
            std::vector<Int64> int_coldata{0};
            auto tmp = createNumbers<Int64>(32, 63);
            int_coldata.insert(int_coldata.end(), tmp.begin(), tmp.end());
            Strings res;
            std::transform(int_coldata.begin(), int_coldata.end(), std::back_inserter(res), [this](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }

    {
        // Test delete range [0, 2)
        // There is an overlap range [0, 1)
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(0, 2, rowkey_column_size)});
        // flush segment
        segment->flushCache(dmContext());
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        // mock common handle
        auto common_handle_coldata = [this]() {
            std::vector<Int64> int_coldata = createNumbers<Int64>(32, 63);
            Strings res;
            std::transform(int_coldata.begin(), int_coldata.end(), std::back_inserter(res), [this](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
            return res;
        }();
        ASSERT_INPUTSTREAM_COLS_UR(
            in,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<String>(common_handle_coldata),
            }));
    }
}
CATCH

TEST_F(SegmentCommonHandleTest, Split)
try
{
    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0,
                                                         num_rows_write,
                                                         false,
                                                         2,
                                                         EXTRA_HANDLE_COLUMN_NAME,
                                                         EXTRA_HANDLE_COLUMN_ID,
                                                         EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                         is_common_handle,
                                                         rowkey_column_size);
        segment->write(dmContext(), std::move(block));
    }

    {
        // read written data
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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

    size_t num_rows_seg1 = getInputStreamNRows(segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)}));
    size_t num_rows_seg2 = getInputStreamNRows(segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)}));
    ASSERT_EQ(num_rows_seg1 + num_rows_seg2, num_rows_write);

    // merge segments
    segment = Segment::merge(dmContext(), tableColumns(), {segment, new_segment});
    {
        // check merged segment range
        const auto & merged_range = segment->getRowKeyRange();
        EXPECT_EQ(*merged_range.start.value, *s1_range.start.value);
        EXPECT_EQ(*merged_range.end.value, *s2_range.end.value);
        // TODO check segment epoch is increase
    }
    {
        auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        ASSERT_INPUTSTREAM_NROWS(in, num_rows_write);
    }
}
CATCH

TEST_F(SegmentCommonHandleTest, Restore)
try
{
    // compare will compares the given segments.
    // If they are equal, result will be true, otherwise it will be false.
    auto compare = [&](const SegmentPtr & seg1, const SegmentPtr & seg2, bool & result) {
        result = false;
        auto in1 = seg1->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        auto in2 = seg2->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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
                        ASSERT_EQ(c1->operator[](i).get<String>(), c2->operator[](i).get<String>());
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
        Block block = DMTestEnv::prepareSimpleWriteBlock(0,
                                                         num_rows_write,
                                                         false,
                                                         2,
                                                         EXTRA_HANDLE_COLUMN_NAME,
                                                         EXTRA_HANDLE_COLUMN_ID,
                                                         EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                         is_common_handle,
                                                         rowkey_column_size);
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
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(0, 32, rowkey_column_size)});
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

TEST_F(SegmentCommonHandleTest, MassiveSplit)
try
{
    Settings settings = dmContext().db_context.getSettings();
    settings.dt_segment_limit_rows = 11;
    settings.dt_segment_delta_limit_rows = 7;

    segment = reload(DMTestEnv::getDefaultColumns(DMTestEnv::PkType::CommonHandle), std::move(settings));

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
                false,
                2,
                EXTRA_HANDLE_COLUMN_NAME,
                EXTRA_HANDLE_COLUMN_ID,
                EXTRA_HANDLE_COLUMN_STRING_TYPE,
                is_common_handle,
                rowkey_column_size);
            segment->write(dmContext(), std::move(block));
            num_batches_written += 1;
        }

        {
            // Delete some records so that the following condition can be satisfied:
            // if pk % 5 < 2, then the record would be deleted
            // if pk % 5 >= 2, then the record would be reserved
            segment->write(
                dmContext(),
                DMTestEnv::getRowKeyRangeForClusteredIndex(
                    static_cast<Int64>((num_batches_written - 1) * num_rows_per_write),
                    static_cast<Int64>((num_batches_written - 1) * num_rows_per_write + 2),
                    rowkey_column_size));
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext(), tableColumns());
        }

        for (size_t i = (num_batches_written - 1) * num_rows_per_write + 2; i < num_batches_written * num_rows_per_write; i++)
        {
            temp.push_back(static_cast<Int64>(i));
        }

        {
            // Read after writing
            auto in = segment->getInputStreamModeNormal(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
            ASSERT_EQ(temp.size(), num_batches_written * (num_rows_per_write - 2));
            // mock common handle
            auto common_handle_coldata = [this, &temp]() {
                Strings res;
                std::transform(temp.begin(), temp.end(), std::back_inserter(res), [this](Int64 v) { return genMockCommonHandle(v, rowkey_column_size); });
                return res;
            }();
            ASSERT_INPUTSTREAM_COLS_UR(
                in,
                Strings({DMTestEnv::pk_name}),
                createColumns({
                    createColumn<String>(common_handle_coldata),
                }));
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

} // namespace DB::DM::tests
