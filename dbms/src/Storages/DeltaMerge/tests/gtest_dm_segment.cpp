#include <memory>

#include "dm_basic_include.h"

#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>

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
    static void SetUpTestCase()
    {
        Poco::AutoPtr<Poco::ConsoleChannel>   channel = new Poco::ConsoleChannel(std::cerr);
        Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
        formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
        Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
        Logger::root().setChannel(formatting_channel);
        Logger::root().setLevel("trace");
    }

    void SetUp() override
    {
        db_context = std::make_unique<Context>(DMTestEnv::getContext(DB::Settings()));
        dropDataInDisk();
        segment = reload();
        ASSERT_EQ(segment->segmentId(), DELTA_MERGE_FIRST_SEGMENT_ID);
    }

    SegmentPtr reload(ColumnDefines && pre_define_columns = {}, DB::Settings && db_settings = DB::Settings())
    {
        storage_pool       = std::make_unique<StoragePool>(path);
        *db_context        = DMTestEnv::getContext(db_settings);
        ColumnDefines cols = pre_define_columns.empty() ? DMTestEnv::getDefaultColumns() : pre_define_columns;
        setColumns(cols);

        auto segment_id = storage_pool->newMetaPageId();
        return Segment::newSegment(*dm_context_, HandleRange::newAll(), segment_id, 0);
    }

    // setColumns should update dm_context at the same time
    void setColumns(const ColumnDefines & columns)
    {
        table_columns_ = columns;

        dm_context_ = std::make_unique<DMContext>(
            DMContext{.db_context          = *db_context,
                      .storage_pool        = *storage_pool,
                      .table_columns       = table_columns_,
                      .table_handle_define = table_columns_.at(0),
                      .min_version         = 0,

                      .not_compress            = settings.not_compress_columns,
                      .delta_limit_rows        = db_context->getSettingsRef().dm_segment_delta_limit_rows,
                      .delta_limit_bytes       = db_context->getSettingsRef().dm_segment_delta_limit_bytes,
                      .delta_cache_limit_rows  = db_context->getSettingsRef().dm_segment_delta_cache_limit_rows,
                      .delta_cache_limit_bytes = db_context->getSettingsRef().dm_segment_delta_cache_limit_bytes});
    }

    const ColumnDefines & tableColumns() const { return table_columns_; }

    DMContext & dmContext() { return *dm_context_; }

private:
    std::unique_ptr<Context> db_context;
    // the table name
    String name;
    // the path to the dir of table
    String path;
    /// all these var lives as ref in dm_context
    std::unique_ptr<StoragePool>  storage_pool;
    ColumnDefines                 table_columns_;
    DM::DeltaMergeStore::Settings settings;
    /// dm_context
    std::unique_ptr<DMContext> dm_context_;

protected:
    // the segment we are going to test
    SegmentPtr segment;
};

TEST_F(Segment_test, WriteRead)
{
    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(dmContext(), std::move(block));
    }

    {
        // read written data
        auto   in            = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ {dmContext().storage_pool},
                                          /* columns_to_read= */ tableColumns(),
                                          /* read_ranges= */ {HandleRange::newAll()},
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
        segment->write(dmContext(), {remove});
        // TODO test delete range partial overlap with segment
        // TODO test delete range not included by segment
    }

    {
        // read after delete range
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ {dmContext().storage_pool},
                                          /* columns_to_read= */ tableColumns(),
                                          /* read_ranges= */ {HandleRange::newAll()},
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
        segment->write(dmContext(), std::move(block));
    }

    {
        // read written data
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ {dmContext().storage_pool},
                                          /* columns_to_read= */ tableColumns(),
                                          /* read_ranges= */ {HandleRange::newAll()},
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
    std::tie(segment, new_segment) = segment->split(dmContext());

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
            auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                              /* segment_snap= */ segment->getReadSnapshot(),
                                              /* storage_snap= */ {dmContext().storage_pool},
                                              /* columns_to_read= */ tableColumns(),
                                              /* read_ranges= */ {HandleRange::newAll()},
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
            auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                              /* segment_snap= */ segment->getReadSnapshot(),
                                              /* storage_snap= */ {dmContext().storage_pool},
                                              /* columns_to_read= */ tableColumns(),
                                              /* read_ranges= */ {HandleRange::newAll()},
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
        segment = Segment::merge(dmContext(), segment, new_segment);
        {
            // check merged segment range
            const auto & merged_range = segment->getRange();
            EXPECT_EQ(merged_range.start, s1_range.start);
            EXPECT_EQ(merged_range.end, s2_range.end);
            // TODO check segment epoch is increase
        }
        {
            size_t num_rows_read = 0;
            auto   in            = segment->getInputStream(/* dm_context= */ dmContext(),
                                              /* segment_snap= */ segment->getReadSnapshot(),
                                              /* storage_snap= */ {dmContext().storage_pool},
                                              /* columns_to_read= */ tableColumns(),
                                              /* read_ranges= */ {HandleRange::newAll()},
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

/// Mock a col from i8 -> i32
TEST_F(Segment_test, DDLAlterInt8ToInt32)
{
    const String       column_name_i8_to_i32 = "i8_to_i32";
    const ColumnID     column_id_i8_to_i32   = 4;
    const ColumnDefine column_i8_before_ddl(column_id_i8_to_i32, column_name_i8_to_i32, DataTypeFactory::instance().get("Int8"));
    const ColumnDefine column_i32_after_ddl(column_id_i8_to_i32, column_name_i8_to_i32, DataTypeFactory::instance().get("Int32"));

    {
        ColumnDefines columns_before_ddl = DMTestEnv::getDefaultColumns();
        columns_before_ddl.emplace_back(column_i8_before_ddl);
        // Not cache any rows
        DB::Settings db_settings;
        db_settings.dm_segment_delta_cache_limit_rows = 0;

        segment = reload(std::move(columns_before_ddl), std::move(db_settings));
    }

    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);

        // add int8_col and later read it as int32
        // (mock ddl change int8 -> int32)
        const size_t          num_rows = block.rows();
        ColumnWithTypeAndName int8_col(column_i8_before_ddl.type, column_i8_before_ddl.name);
        {
            IColumn::MutablePtr m_col       = int8_col.type->createColumn();
            auto &              column_data = typeid_cast<ColumnVector<Int8> &>(*m_col).getData();
            column_data.resize(num_rows);
            for (size_t i = 0; i < num_rows; ++i)
            {
                column_data[i] = static_cast<int8_t>(-1 * (i % 2 ? 1 : -1) * i);
            }
            int8_col.column = std::move(m_col);
        }
        block.insert(int8_col);

        segment->write(dmContext(), std::move(block));
    }

    {
        ColumnDefines columns_to_read{
            column_i32_after_ddl,
        };

        // read written data
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ {dmContext().storage_pool},
                                          /* columns_to_read= */ tableColumns(),
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* max_version= */ std::numeric_limits<UInt64>::max(),
                                          /* expected_block_size= */ 1024);

        // check that we can read correct values
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            const ColumnWithTypeAndName & col = block.getByName(column_name_i8_to_i32);
            ASSERT_TRUE(col.type->equals(*column_i32_after_ddl.type));
            ASSERT_EQ(col.name, column_i32_after_ddl.name);
            ASSERT_EQ(col.column_id, column_i32_after_ddl.id);
            for (size_t i = 0; i < block.rows(); ++i)
            {
                auto       value    = col.column->getInt(i);
                const auto expected = static_cast<int64_t>(-1 * (i % 2 ? 1 : -1) * i);
                ASSERT_EQ(value, expected);
            }
        }
        in->readSuffix();
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}

TEST_F(Segment_test, DDLAddColumnWithDefaultValue)
{
    const String   new_column_name = "i8";
    const ColumnID new_column_id   = 4;
    ColumnDefine   new_column_define(new_column_id, new_column_name, DataTypeFactory::instance().get("Int8"));
    const Int8     new_column_default_value_int = 16;
    new_column_define.default_value             = DB::toString(new_column_default_value_int);

    {
        ColumnDefines columns_before_ddl = DMTestEnv::getDefaultColumns();
        // Not cache any rows
        DB::Settings db_settings;
        db_settings.dm_segment_delta_cache_limit_rows = 0;

        segment = reload(std::move(columns_before_ddl), std::move(db_settings));
    }

    const size_t num_rows_write = 100;
    {
        // write to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(dmContext(), std::move(block));
    }

    {
        // DDL add new column with default value
        ColumnDefines columns_after_ddl = DMTestEnv::getDefaultColumns();
        columns_after_ddl.emplace_back(new_column_define);
        setColumns(columns_after_ddl);
    }

    {
        ColumnDefines columns_to_read{
            new_column_define,
        };

        // read written data
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ {dmContext().storage_pool},
                                          /* columns_to_read= */ tableColumns(),
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* max_version= */ std::numeric_limits<UInt64>::max(),
                                          /* expected_block_size= */ 1024);

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
}

} // namespace tests
} // namespace DM
} // namespace DB
