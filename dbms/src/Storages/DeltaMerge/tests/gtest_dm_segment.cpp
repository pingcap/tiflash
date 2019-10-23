#include <ctime>
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

public:
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

protected:
    SegmentPtr reload(ColumnDefines && pre_define_columns = {}, DB::Settings && db_settings = DB::Settings())
    {
        storage_pool       = std::make_unique<StoragePool>("test.t1", path);
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
            DMContext{.db_context    = *db_context,
                      .storage_pool  = *storage_pool,
                      .store_columns = table_columns_,
                      .handle_column = table_columns_.at(0),
                      .min_version   = 0,

                      .not_compress = settings.not_compress_columns,

                      .segment_limit_rows = db_context->getSettingsRef().dm_segment_limit_rows,

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
        // estimate segment
        auto estimatedRows = segment->getEstimatedRows();
        ASSERT_GT(estimatedRows, num_rows_write / 2);
        ASSERT_LT(estimatedRows, num_rows_write * 2);

        auto estimatedBytes = segment->getEstimatedBytes();
        ASSERT_GT(estimatedBytes, num_rows_write * 5 / 2);
        ASSERT_LT(estimatedBytes, num_rows_write * 5 * 2);
    }

    {
        // check segment
        segment->check(dmContext(), "test");
    }

    { // Round 1
        {
            // read written data (only in delta)
            auto   in            = segment->getInputStream(/* dm_context= */ dmContext(),
                                              /* columns_to_read= */ tableColumns(),
                                              /* segment_snap= */ segment->getReadSnapshot(),
                                              /* storage_snap= */ {dmContext().storage_pool},
                                              /* read_ranges= */ {HandleRange::newAll()},
                                              /* filter= */ EMPTY_FILTER,
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
            // flush segment
            segment = segment->mergeDelta(dmContext());
        }

        {
            // read written data (only in stable)
            auto   in            = segment->getInputStream(/* dm_context= */ dmContext(),
                                              /* columns_to_read= */ tableColumns(),
                                              /* segment_snap= */ segment->getReadSnapshot(),
                                              /* storage_snap= */ StorageSnapshot{dmContext().storage_pool},
                                              /* read_ranges= */ {HandleRange::newAll()},
                                              /* filter= */ EMPTY_FILTER,
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
            auto   in            = segment->getInputStream(/* dm_context= */ dmContext(),
                                              /* columns_to_read= */ tableColumns(),
                                              /* segment_snap= */ segment->getReadSnapshot(),
                                              /* storage_snap= */ StorageSnapshot{dmContext().storage_pool},
                                              /* read_ranges= */ {HandleRange::newAll()},
                                              /* filter= */ EMPTY_FILTER,
                                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                                              /* expected_block_size= */ 1024);
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
            segment = segment->mergeDelta(dmContext());
        }

        {
            // read written data (only in stable)
            auto   in            = segment->getInputStream(/* dm_context= */ dmContext(),
                                              /* columns_to_read= */ tableColumns(),
                                              /* segment_snap= */ segment->getReadSnapshot(),
                                              /* storage_snap= */ StorageSnapshot{dmContext().storage_pool},
                                              /* read_ranges= */ {HandleRange::newAll()},
                                              /* filter= */ EMPTY_FILTER,
                                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                                              /* expected_block_size= */ 1024);
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

class SegmentDeletion_test : public Segment_test, //
                             public testing::WithParamInterface<std::tuple<bool, bool>>
{
};

TEST_P(SegmentDeletion_test, DeleteDataInDelta)
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
        auto   in            = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ StorageSnapshot{dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter */ EMPTY_FILTER,
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
        // test delete range [1,99) for data in delta
        HandleRange remove(1, 99);
        segment->write(dmContext(), {remove});
        // TODO test delete range partial overlap with segment
        // TODO test delete range not included by segment
    }

    if (merge_delta_after_delete)
    {
        // flush segment for apply delete range
        segment = segment->mergeDelta(dmContext());
    }

    {
        // read after delete range
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ StorageSnapshot{dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter= */ EMPTY_FILTER,
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

TEST_P(SegmentDeletion_test, DeleteDataInStable)
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
        auto   in            = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ StorageSnapshot{dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter */ EMPTY_FILTER,
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
        // flush segment
        segment = segment->mergeDelta(dmContext());
    }

    {
        // test delete range [1,99) for data in stable
        HandleRange remove(1, 99);
        segment->write(dmContext(), {remove});
        // TODO test delete range partial overlap with segment
        // TODO test delete range not included by segment

        // flush segment
        segment = segment->mergeDelta(dmContext());
    }

    if (merge_delta_after_delete)
    {
        // flush segment for apply delete range
        segment = segment->mergeDelta(dmContext());
    }

    {
        // read after delete range
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ StorageSnapshot{dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter= */ EMPTY_FILTER,
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

TEST_P(SegmentDeletion_test, DeleteDataInStableAndDelta)
{
    const size_t num_rows_write = 100;
    {
        // write [0, 50) to segment
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write / 2, false);
        segment->write(dmContext(), std::move(block));
        // flush [0, 50) to segment's stable
        segment = segment->mergeDelta(dmContext());
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
        auto   in            = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ StorageSnapshot{dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter */ EMPTY_FILTER,
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
        // test delete range [1,99) for data in stable and delta
        HandleRange remove(1, 99);
        segment->write(dmContext(), {remove});
        // TODO test delete range partial overlap with segment
        // TODO test delete range not included by segment
    }

    if (merge_delta_after_delete)
    {
        // flush segment for apply delete range
        segment = segment->mergeDelta(dmContext());
    }

    {
        // read after delete range
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ StorageSnapshot{dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter= */ EMPTY_FILTER,
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

INSTANTIATE_TEST_CASE_P(WhetherReadOrMergeDeltaBeforeDeleteRange, SegmentDeletion_test, testing::Combine(testing::Bool(), testing::Bool()));
TEST_F(Segment_test, DeleteRead)
{
    const size_t num_rows_write = 64;
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
        segment->write(dmContext(), std::move(block));
    }

    {
        // flush segment
        segment = segment->mergeDelta(dmContext());
    }

    {
        // Test delete range [70, 100)
        HandleRange del{70, 100};
        segment->write(dmContext(), {del});
        // flush segment
        segment = segment->mergeDelta(dmContext());
    }

    {
        // Read after deletion
        // The deleted range has no overlap with current data, so there should be no change
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snaps= */ {dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter */ {},
                                          /* max_version= */ std::numeric_limits<UInt64>::max(),
                                          /* expected_block_size= */ 1024);
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write);
            for (auto & iter : block)
            {
                auto c = iter.column;
                for (Int64 i = 0; i < Int64(c->size()); i++)
                {
                    if (iter.name == "pk")
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
        segment->write(dmContext(), {del});
        // flush segment
        segment = segment->mergeDelta(dmContext());
    }

    {
        // Read after deletion
        // The deleted range has overlap range [63, 64) with current data, so the record with Handle 63 should be deleted
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snaps= */ {dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter */ {},
                                          /* max_version= */ std::numeric_limits<UInt64>::max(),
                                          /* expected_block_size= */ 1024);
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 1);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == "pk")
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
        segment->write(dmContext(), {del});
        // flush segment
        segment = segment->mergeDelta(dmContext());
    }

    {
        // Read after deletion
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snaps= */ {dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter */ {},
                                          /* max_version= */ std::numeric_limits<UInt64>::max(),
                                          /* expected_block_size= */ 1024);
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 32);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == "pk")
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
        segment->write(dmContext(), {del});
        // flush segment
        segment = segment->mergeDelta(dmContext());
    }

    {
        // Read after deletion
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snaps= */ {dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter */ {},
                                          /* max_version= */ std::numeric_limits<UInt64>::max(),
                                          /* expected_block_size= */ 1024);
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 32);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == "pk")
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
        segment->write(dmContext(), {del});
        // flush segment
        segment = segment->mergeDelta(dmContext());
    }

    {
        // Read after deletion
        auto in = segment->getInputStream(/* dm_context= */ dmContext(),
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snaps= */ {dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter */ {},
                                          /* max_version= */ std::numeric_limits<UInt64>::max(),
                                          /* expected_block_size= */ 1024);
        in->readPrefix();
        while (Block block = in->read())
        {
            ASSERT_EQ(block.rows(), num_rows_write - 33);
            for (auto & iter : block)
            {
                auto c = iter.column;
                if (iter.name == "pk")
                {
                    EXPECT_EQ(c->getInt(0), 32);
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
                                          /* columns_to_read= */ tableColumns(),
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ {dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter */ {},
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
    {
        std::tie(segment, new_segment) = segment->split(dmContext());
    }
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
                                              /* columns_to_read= */ tableColumns(),
                                              /* segment_snap= */ segment->getReadSnapshot(),
                                              /* storage_snap= */ {dmContext().storage_pool},
                                              /* read_ranges= */ {HandleRange::newAll()},
                                              /* filter */ {},
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
                                              /* columns_to_read= */ tableColumns(),
                                              /* segment_snap= */ segment->getReadSnapshot(),
                                              /* storage_snap= */ {dmContext().storage_pool},
                                              /* read_ranges= */ {HandleRange::newAll()},
                                              /* filter */ {},
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
                                              /* columns_to_read= */ tableColumns(),
                                              /* segment_snap= */ segment->getReadSnapshot(),
                                              /* storage_snap= */ {dmContext().storage_pool},
                                              /* read_ranges= */ {HandleRange::newAll()},
                                              /* filter= */ {},
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

TEST_F(Segment_test, Restore)
{
    // compare will compares the given segments.
    // If they are equal, result will be true, otherwise it will be false.
    auto compare = [&](const SegmentPtr & seg1, const SegmentPtr & seg2, bool & result) {
        result   = false;
        auto in1 = seg1->getInputStream(/* dm_context= */ dmContext(),
                                        /* columns_to_read= */ tableColumns(),
                                        /* segment_snap= */ seg1->getReadSnapshot(),
                                        /* storage_snaps= */ {dmContext().storage_pool},
                                        /* read_ranges= */ {HandleRange::newAll()},
                                        /* filter */ {},
                                        /* max_version= */ std::numeric_limits<UInt64>::max(),
                                        /* expected_block_size= */ 1024);

        auto in2 = seg2->getInputStream(/* dm_context= */ dmContext(),
                                        /* columns_to_read= */ tableColumns(),
                                        /* segment_snap= */ seg2->getReadSnapshot(),
                                        /* storage_snaps= */ {dmContext().storage_pool},
                                        /* read_ranges= */ {HandleRange::newAll()},
                                        /* filter */ {},
                                        /* max_version= */ std::numeric_limits<UInt64>::max(),
                                        /* expected_block_size= */ 1024);
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
                    if (iter1->name == "pk")
                    {
                        ASSERT_EQ(iter2->name, "pk");
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
        segment = segment->mergeDelta(dmContext());
    }

    SegmentPtr new_segment = segment->restoreSegment(dmContext(), segment->segmentId());

    {
        // test compare
        bool result;
        compare(segment, new_segment, result);
        ASSERT_TRUE(result);
    }

    {
        // Do some update and restore again
        HandleRange del(0, 32);
        segment->write(dmContext(), {del});
        new_segment = segment->restoreSegment(dmContext(), segment->segmentId());
    }

    {
        // test compare
        bool result;
        compare(new_segment, new_segment, result);
        ASSERT_TRUE(result);
    }
}

TEST_F(Segment_test, MassiveSplit)
try
{
    Settings settings                    = dmContext().db_context.getSettings();
    settings.dm_segment_limit_rows       = 11;
    settings.dm_segment_delta_limit_rows = 7;

    segment = reload(DMTestEnv::getDefaultColumns(), std::move(settings));

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
            segment->write(dmContext(), {del});
        }

        {
            // flush segment
            segment = segment->mergeDelta(dmContext());
        }

        for (size_t i = (num_batches_written - 1) * num_rows_per_write + 2; i < num_batches_written * num_rows_per_write; i++)
        {
            temp.push_back(Int64(i));
        }

        {
            // Read after writing
            auto   in            = segment->getInputStream(/* dm_context= */ dmContext(),
                                              /* columns_to_read= */ tableColumns(),
                                              /* segment_snap= */ segment->getReadSnapshot(),
                                              /* storage_snaps= */ {dmContext().storage_pool},
                                              /* read_ranges= */ {HandleRange::newAll()},
                                              /* filter */ {},
                                              /* max_version= */ std::numeric_limits<UInt64>::max(),
                                              /* expected_block_size= */ 1024);
            size_t num_rows_read = 0;
            in->readPrefix();
            while (Block block = in->read())
            {
                for (auto & iter : block)
                {
                    auto c = iter.column;
                    for (size_t i = 0; i < c->size(); i++)
                    {
                        if (iter.name == "pk")
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
catch (const Exception & e)
{
    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;
    if (std::string::npos == embedded_stack_trace_pos)
        std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString() << std::endl;

    throw;
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

        BlockInputStreamPtr in;
        try
        {
            // read written data
            in = segment->getInputStream(/* dm_context= */ dmContext(),
                                         /* columns_to_read= */ columns_to_read,
                                         /* segment_snap= */ segment->getReadSnapshot(),
                                         /* storage_snap= */ {dmContext().storage_pool},
                                         /* read_ranges= */ {HandleRange::newAll()},
                                         /* filter */ {},
                                         /* max_version= */ std::numeric_limits<UInt64>::max(),
                                         /* expected_block_size= */ 1024);
        }
        catch (const Exception & e)
        {
            const auto text = e.displayText();
            std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;
            std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString();

            throw;
        }

        // check that we can read correct values
        size_t num_rows_read = 0;
        in->readPrefix();
        while (Block block = in->read())
        {
            num_rows_read += block.rows();
            const ColumnWithTypeAndName & col = block.getByName(column_name_i8_to_i32);
            ASSERT_TRUE(col.type->equals(*column_i32_after_ddl.type))
                << "col.type: " + col.type->getName() + " expect type: " + column_i32_after_ddl.type->getName();
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
    new_column_define.default_value             = toField(new_column_default_value_int);

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
                                          /* columns_to_read= */ columns_to_read,
                                          /* segment_snap= */ segment->getReadSnapshot(),
                                          /* storage_snap= */ {dmContext().storage_pool},
                                          /* read_ranges= */ {HandleRange::newAll()},
                                          /* filter */ {},
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
