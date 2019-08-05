#include <memory>

#include "dm_basic_include.h"

#include <Poco/File.h>

#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>

namespace DB
{
namespace DM
{
namespace tests
{

class Segment_test : public ::testing::Test
{
public:
    Segment_test() : name("t"), path("./" + name), storage_pool() {}

private:
    void dropDataInDisk()
    {
        // drop former-gen table's data in disk
        Poco::File file(path);
        if (file.exists())
            file.remove(true);
    }

protected:
    void SetUp() override
    {
        dropDataInDisk();
        storage_pool        = std::make_unique<StoragePool>(path);
        Context context     = DMTestEnv::getContext();
        table_handle_define = ColumnDefine{
            .name = "pk",
            .type = std::make_shared<DataTypeInt64>(),
            .id   = 1,
        };
        table_columns.clear();
        table_columns.emplace_back(table_handle_define);
        table_columns.emplace_back(VERSION_COLUMN_DEFINE);
        table_columns.emplace_back(TAG_COLUMN_DEFINE);

        dm_context = std::make_unique<DMContext>(
            DMContext{.db_context          = context,
                      .storage_pool        = *storage_pool,
                      .table_name          = name,
                      .table_columns       = table_columns,
                      .table_handle_define = table_handle_define,
                      .min_version         = 0,

                      .not_compress            = settings.not_compress_columns,
                      .delta_limit_rows        = context.getSettingsRef().dm_segment_delta_limit_rows,
                      .delta_limit_bytes       = context.getSettingsRef().dm_segment_delta_limit_bytes,
                      .delta_cache_limit_rows  = context.getSettingsRef().dm_segment_delta_cache_limit_rows,
                      .delta_cache_limit_bytes = context.getSettingsRef().dm_segment_delta_cache_limit_bytes});

        auto segment_id = storage_pool->newMetaPageId();
        ASSERT_EQ(segment_id, DELTA_MERGE_FIRST_SEGMENT_ID);
        segment = Segment::newSegment(*dm_context, HandleRange::newAll(), segment_id, 0);
    }

protected:
    // the table name
    String name;
    // the path to the dir of table
    String path;
    /// all these var lives as ref in dm_context
    std::unique_ptr<StoragePool>  storage_pool;
    ColumnDefine                  table_handle_define;
    ColumnDefines                 table_columns;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context;

    // the segment we are going to test
    SegmentPtr segment;
};

TEST_F(Segment_test, WriteRead)
{
    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(*dm_context, std::move(block));
    }

    {
        // read written data
        auto   in            = segment->getInputStream(/* dm_context= */ *dm_context,
                                          /*columns_to_read= */ table_columns,
                                          {HandleRange::newAll()},
                                          /* max_version= */ std::numeric_limits<UInt64>::max(),
                                          /* expected_block_size= */ 1024);
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
        // test delete range [1,99)
        HandleRange remove(1, 99);
        segment->write(*dm_context, {remove});
        // TODO test delete range partial overlap with segment
        // TODO test delete range not included by segment
    }

    {
        // read after delete range
        auto in = segment->getInputStream(/* dm_context= */ *dm_context,
                                          /* columns_to_read= */ table_columns,
                                          {HandleRange::newAll()},
                                          /* max_version= */ std::numeric_limits<UInt64>::max(),
                                          /* expected_block_size= */ 1024);
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), 2UL);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == "pk")
                {
                    EXPECT_EQ(c->getInt(0), 0);
                    EXPECT_EQ(c->getInt(1), 99);
                }
            }
        }
        in->readSuffix();
    }
}

TEST_F(Segment_test, Split)
{
    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(*dm_context, std::move(block));
    }

    {
        // read written data
        auto   in            = segment->getInputStream(/* dm_context= */ *dm_context,
                                          /*columns_to_read= */ table_columns,
                                          {HandleRange::newAll()},
                                          /* max_version= */ std::numeric_limits<UInt64>::max(),
                                          /* expected_block_size= */ 1024);
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }

    const auto old_range = segment->getRange();

    SegmentPtr new_segment;
    // test split segment
    std::tie(segment, new_segment) = segment->split(*dm_context);

    // check segment range
    const auto s1_range = segment->getRange();
    EXPECT_EQ(s1_range.start, old_range.start);
    const auto s2_range = new_segment->getRange();
    EXPECT_EQ(s2_range.start, s1_range.end);
    EXPECT_EQ(s2_range.end, old_range.end);
    // TODO check segment epoch is increase

    size_t num_rows_seg1 = 0;
    size_t num_rows_seg2 = 0;
    {
        {
            auto in = segment->getInputStream(/* dm_context= */ *dm_context,
                                              /* columns_to_read= */ table_columns,
                                              {HandleRange::newAll()},
                                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                                              /* expected_block_size= */ 1024);
            in->readPrefix();
            while (Block block = in->read())
            {
                num_rows_seg1 += block.rows();
            }
            in->readSuffix();
        }
        {
            auto in = new_segment->getInputStream(/* dm_context= */ *dm_context,
                                                  /*columns_to_read= */ table_columns,
                                                  {HandleRange::newAll()},
                                                  /* max_version= */ std::numeric_limits<UInt64>::max(),
                                                  /* expected_block_size= */ 1024);
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
    {
        segment = Segment::merge(*dm_context, segment, new_segment);
        {
            // check merged segment range
            const auto & merged_range = segment->getRange();
            EXPECT_EQ(merged_range.start, s1_range.start);
            EXPECT_EQ(merged_range.end, s2_range.end);
            // TODO check segment epoch is increase
        }
        {
            size_t num_rows_read = 0;
            auto   in            = segment->getInputStream(/* dm_context= */ *dm_context,
                                              /* columns_to_read= */ table_columns,
                                              {HandleRange::newAll()},
                                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                                              /* expected_block_size= */ 1024);
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

TEST_F(Segment_test, Restore) {}

} // namespace tests
} // namespace DM
} // namespace DB
