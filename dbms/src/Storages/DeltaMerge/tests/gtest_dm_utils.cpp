#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <TestUtils/TiFlashTestBasic.h>

#include "dm_basic_include.h"

namespace DB
{
namespace DM
{
namespace tests
{
TEST(RowKeyFilter_test, FilterSortedBlock)
{
    const size_t num_rows_write = 100;
    RowKeyRanges ranges;
    // create ranges unsorted and overlapped with each other
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(20, 50)));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(0, 30)));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(70, 90)));
    Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
    auto filtered_block = RowKeyFilter::filterSorted(ranges, std::move(block), 0);
    ASSERT_EQ(filtered_block.rows(), 70);
}

TEST(RowKeyFilter_test, FilterUnsortedBlock)
{
    const size_t num_rows_write = 100;
    RowKeyRanges ranges;
    // create ranges unsorted and overlapped with each other
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(20, 50)));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(0, 30)));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(70, 90)));
    Block block = DMTestEnv::prepareSimpleWriteBlock(0, num_rows_write, false);
    auto filtered_block = RowKeyFilter::filterUnsorted(ranges, std::move(block), 0);
    ASSERT_EQ(filtered_block.rows(), 70);
}

TEST(RowKeyFilter_test, FilterSortedBlockCommonHandle)
{
    const size_t num_rows_write = 100;
    RowKeyRanges ranges;
    // create ranges unsorted and overlapped with each other
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(20, 50), true));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(0, 30), true));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(70, 90), true));
    Block block = DMTestEnv::prepareSimpleWriteBlock(0,
                                                     num_rows_write,
                                                     false,
                                                     2,
                                                     EXTRA_HANDLE_COLUMN_NAME,
                                                     EXTRA_HANDLE_COLUMN_ID,
                                                     EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                     true,
                                                     1);
    auto filtered_block = RowKeyFilter::filterSorted(ranges, std::move(block), 0);
    ASSERT_EQ(filtered_block.rows(), 70);
}

TEST(RowKeyFilter_test, FilterUnsortedBlockCommonHandle)
{
    const size_t num_rows_write = 100;
    RowKeyRanges ranges;
    // create ranges unsorted and overlapped with each other
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(20, 50), true));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(0, 30), true));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(70, 90), true));
    Block block = DMTestEnv::prepareSimpleWriteBlock(0,
                                                     num_rows_write,
                                                     false,
                                                     2,
                                                     EXTRA_HANDLE_COLUMN_NAME,
                                                     EXTRA_HANDLE_COLUMN_ID,
                                                     EXTRA_HANDLE_COLUMN_STRING_TYPE,
                                                     true,
                                                     1);
    auto filtered_block = RowKeyFilter::filterUnsorted(ranges, std::move(block), 0);
    ASSERT_EQ(filtered_block.rows(), 70);
}

} // namespace tests
} // namespace DM
} // namespace DB
