#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ctime>
#include <memory>

#include "dm_basic_include.h"

namespace DB
{
namespace DM
{
namespace tests
{

class Segment_Common_Handle_test : public ::testing::Test
{
public:
    Segment_Common_Handle_test() : name("tmp"), storage_pool() {}

private:
    void dropDataOnDisk()
    {
        // drop former-gen table's data in disk
        if (Poco::File file(DB::tests::TiFlashTestEnv::getTemporaryPath()); file.exists())
            file.remove(true);
    }

public:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        db_context     = std::make_unique<Context>(DMTestEnv::getContext(DB::Settings()));
        table_columns_ = std::make_shared<ColumnDefines>();
        dropDataOnDisk();

        segment = reload();
        ASSERT_EQ(segment->segmentId(), DELTA_MERGE_FIRST_SEGMENT_ID);
    }

protected:
    SegmentPtr reload(ColumnDefinesPtr cols = {}, DB::Settings && db_settings = DB::Settings())
    {
        *db_context  = DMTestEnv::getContext(db_settings);
        path_pool    = std::make_unique<StoragePathPool>(db_context->getPathPool().withTable("test", "t", false));
        storage_pool = std::make_unique<StoragePool>("test.t1", *path_pool, *db_context, db_context->getSettingsRef());
        storage_pool->restore();
        if (!cols)
            cols = DMTestEnv::getDefaultColumns(is_common_handle ? DMTestEnv::PkType::CommonHandle : DMTestEnv::PkType::HiddenTiDBRowID);
        setColumns(cols);

        auto segment_id = storage_pool->newMetaPageId();
        return Segment::newSegment(*dm_context_, table_columns_, RowKeyRange::newAll(is_common_handle, rowkey_column_size), segment_id, 0);
    }

    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefinesPtr & columns)
    {
        *table_columns_ = *columns;

        dm_context_ = std::make_unique<DMContext>(*db_context,
                                                  *path_pool,
                                                  *storage_pool,
                                                  0,
                                                  /*min_version_*/ 0,
                                                  settings.not_compress_columns,
                                                  is_common_handle,
                                                  rowkey_column_size,
                                                  db_context->getSettingsRef());
    }

    const ColumnDefinesPtr & tableColumns() const { return table_columns_; }

    DMContext & dmContext() { return *dm_context_; }

private:
    std::unique_ptr<Context> db_context;
    // the table name
    String name;
    /// all these var lives as ref in dm_context
    std::unique_ptr<StoragePathPool> path_pool;
    std::unique_ptr<StoragePool>     storage_pool;
    ColumnDefinesPtr                 table_columns_;
    DM::DeltaMergeStore::Settings    settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context_;

protected:
    // the segment we are going to test
    SegmentPtr segment;
    bool       is_common_handle   = true;
    size_t     rowkey_column_size = 2;
};

TEST_F(Segment_Common_Handle_test, WriteRead)
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
            auto   in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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
            auto   in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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
            auto   in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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
            auto   in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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

class SegmentDeletion_Common_Handle_test : public Segment_Common_Handle_test, //
                                           public testing::WithParamInterface<std::tuple<bool, bool>>
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
        auto   in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), 2UL);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](0).get<String>(), 0, rowkey_column_size);
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](1).get<String>(), 99, rowkey_column_size);
                }
            }
        }
        in->readSuffix();
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
        auto   in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), 2UL);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](0).get<String>(), 0, rowkey_column_size);
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](1).get<String>(), 99, rowkey_column_size);
                }
            }
        }
        in->readSuffix();
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
        auto   in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), 2UL);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](0).get<String>(), 0, rowkey_column_size);
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](1).get<String>(), 99, rowkey_column_size);
                }
            }
        }
        in->readSuffix();
    }
}
CATCH

INSTANTIATE_TEST_CASE_P(WhetherReadOrMergeDeltaBeforeDeleteRange,
                        SegmentDeletion_Common_Handle_test,
                        testing::Combine(testing::Bool(), testing::Bool()));

TEST_F(Segment_Common_Handle_test, DeleteRead)
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
        // flush segment
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Test delete range [70, 100)
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(70, 100, rowkey_column_size)});
        // flush segment
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        // The deleted range has no overlap with current data, so there should be no change
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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
                        DMTestEnv::verifyClusteredIndexValue(c->operator[](i).get<String>(), i, rowkey_column_size);
                    }
                }
            }
        }
        in->readSuffix();
    }

    {
        // Test delete range [63, 70)
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(63, 70, rowkey_column_size)});
        // flush segment
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        // The deleted range has overlap range [63, 64) with current data, so the record with Handle 63 should be deleted
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 1);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](0).get<String>(), 0, rowkey_column_size);
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](62).get<String>(), 62, rowkey_column_size);
                }
                EXPECT_EQ(c->size(), 63UL);
            }
        }
        in->readSuffix();
    }

    {
        // Test delete range [1, 32)
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(1, 32, rowkey_column_size)});
        // flush segment
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 32);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](0).get<String>(), 0, rowkey_column_size);
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](1).get<String>(), 32, rowkey_column_size);
                }
            }
        }
        in->readSuffix();
    }

    {
        // Test delete range [1, 32)
        // delete should be idempotent
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(1, 32, rowkey_column_size)});
        // flush segment
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 32);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](0).get<String>(), 0, rowkey_column_size);
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](1).get<String>(), 32, rowkey_column_size);
                }
            }
        }
        in->readSuffix();
    }

    {
        // Test delete range [0, 2)
        // There is an overlap range [0, 1)
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(0, 2, rowkey_column_size)});
        // flush segment
        segment = segment->mergeDelta(dmContext(), tableColumns());
    }

    {
        // Read after deletion
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 33);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == DMTestEnv::pk_name)
                {
                    DMTestEnv::verifyClusteredIndexValue(c->operator[](0).get<String>(), 32, rowkey_column_size);
                }
            }
        }
        in->readSuffix();
    }
}
CATCH

TEST_F(Segment_Common_Handle_test, Split)
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
        auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});

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
            auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_seg1 += block.rows();
            }
            in->readSuffix();
        }
        {
            auto in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_seg2 += block.rows();
            }
            in->readSuffix();
        }
        ASSERT_EQ(num_rows_seg1 + num_rows_seg2, num_rows_write);
    }

    // merge segments
    // TODO: enable merge test!
    if (false)
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
            auto   in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_read += block.rows();
            }
            in->readSuffix();
            EXPECT_EQ(num_rows_read, num_rows_write);
        }
    }
}
CATCH

TEST_F(Segment_Common_Handle_test, Restore)
try
{
    // compare will compares the given segments.
    // If they are equal, result will be true, otherwise it will be false.
    auto compare = [&](const SegmentPtr & seg1, const SegmentPtr & seg2, bool & result) {
        result   = false;
        auto in1 = seg1->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
        auto in2 = seg2->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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

    SegmentPtr new_segment = Segment::restoreSegment(dmContext(), segment->segmentId());

    {
        // test compare
        bool result;
        compare(segment, new_segment, result);
        ASSERT_TRUE(result);
    }

    {
        // Do some update and restore again
        segment->write(dmContext(), {DMTestEnv::getRowKeyRangeForClusteredIndex(0, 32, rowkey_column_size)});
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

TEST_F(Segment_Common_Handle_test, MassiveSplit)
try
{
    Settings settings                    = dmContext().db_context.getSettings();
    settings.dt_segment_limit_rows       = 11;
    settings.dt_segment_delta_limit_rows = 7;

    segment = reload(DMTestEnv::getDefaultColumns(DMTestEnv::PkType::CommonHandle), std::move(settings));

    size_t       num_batches_written = 0;
    const size_t num_rows_per_write  = 5;

    const time_t start_time = std::time(nullptr);

    auto temp = std::vector<Int64>();
    for (;;)
    {
        {
            // Write to segment
            Block block = DMTestEnv::prepareSimpleWriteBlock( //
                num_batches_written * num_rows_per_write,     //
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
            segment->write(dmContext(),
                           DMTestEnv::getRowKeyRangeForClusteredIndex(Int64((num_batches_written - 1) * num_rows_per_write),
                                                                      Int64((num_batches_written - 1) * num_rows_per_write + 2),
                                                                      rowkey_column_size));
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
            auto   in = segment->getInputStream(dmContext(), *tableColumns(), {RowKeyRange::newAll(is_common_handle, rowkey_column_size)});
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
                            DMTestEnv::verifyClusteredIndexValue(c->operator[](Int64(i)).get<String>(), expect, rowkey_column_size);
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

} // namespace tests
} // namespace DM
} // namespace DB
