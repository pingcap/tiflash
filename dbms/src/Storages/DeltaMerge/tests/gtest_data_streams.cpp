#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/DeltaMerge/PKSquashingBlockInputStream.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>

namespace DB
{
namespace DM
{
namespace tests
{
TEST(PKSquash_test, WithExtraSort)
{
    BlocksList blocks;

    size_t rows_per_block = 10;
    size_t num_rows_write = 0;
    {
        // pk asc, ts desc
        blocks.push_back(DMTestEnv::prepareBlockWithTso(4, 10000 + rows_per_block * 2, 10000 + rows_per_block * 3, true));
        num_rows_write += blocks.back().rows();
        blocks.push_back(DMTestEnv::prepareBlockWithTso(4, 10000 + rows_per_block, 10000 + rows_per_block * 2, true));
        num_rows_write += blocks.back().rows();
        blocks.push_back(DMTestEnv::prepareBlockWithTso(4, 10000, 10000 + rows_per_block, true));
        num_rows_write += blocks.back().rows();

        {
            Block mix_pks_block = DMTestEnv::prepareBlockWithTso(5, 10000, 10000 + rows_per_block, true);
            Block b2 = DMTestEnv::prepareBlockWithTso(6, 10000 + rows_per_block, 10000 + rows_per_block * 2, true);
            concat(mix_pks_block, b2);
            blocks.push_back(mix_pks_block);
            num_rows_write += blocks.back().rows();
        }
        {
            Block mix_pks_block = DMTestEnv::prepareBlockWithTso(6, 10000, 10000 + rows_per_block, true);
            Block b2 = DMTestEnv::prepareBlockWithTso(7, 10000 + rows_per_block, 10000 + rows_per_block * 2, true);
            concat(mix_pks_block, b2);
            blocks.push_back(mix_pks_block);
            num_rows_write += blocks.back().rows();
        }
        blocks.push_back(DMTestEnv::prepareBlockWithTso(7, 10000, 10000 + rows_per_block, true));
        num_rows_write += blocks.back().rows();
    }

    // Sorted by pk, tso asc
    SortDescription sort //
        = SortDescription{//
                          SortColumnDescription{EXTRA_HANDLE_COLUMN_NAME, 1, 0},
                          SortColumnDescription{VERSION_COLUMN_NAME, 1, 0}};

    {
        auto in = std::make_shared<PKSquashingBlockInputStream</*need_extra_sort*/ true>>(
            std::make_shared<BlocksListBlockInputStream>(blocks.begin(), blocks.end()),
            TiDBPkColumnID,
            false);
        size_t num_blocks_read = 0;
        size_t num_rows_read = 0;
        in->readPrefix();
        Block block;
        while (true)
        {
            block = in->read();
            if (!block)
                break;

            num_blocks_read += 1;
            if (num_blocks_read == 1)
            {
                // for pk == 4
                EXPECT_EQ(block.rows(), rows_per_block * 3);
            }
            else if (num_blocks_read == 2)
            {
                // for pk in (5, 6)
                EXPECT_EQ(block.rows(), rows_per_block * 3);
            }
            else
            {
                // for pk == 7
                EXPECT_EQ(block.rows(), rows_per_block * 2);
            }
            num_rows_read += block.rows();
            // Should be sorted
            ASSERT_TRUE(isAlreadySorted(block, sort));
        }
        in->readSuffix();
        ASSERT_EQ(num_blocks_read, 3);
        ASSERT_EQ(num_rows_read, num_rows_write);
    }
}

} // namespace tests
} // namespace DM
} // namespace DB
