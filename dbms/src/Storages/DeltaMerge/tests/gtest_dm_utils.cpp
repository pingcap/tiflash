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

#include <Storages/DeltaMerge/RowKeyFilter.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace DM
{
namespace tests
{
TEST(RowKeyFilterTest, FilterSortedBlock)
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

TEST(RowKeyFilterTest, FilterUnsortedBlock)
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

TEST(RowKeyFilterTest, FilterSortedBlockCommonHandle)
{
    const size_t num_rows_write = 100;
    RowKeyRanges ranges;
    // create ranges unsorted and overlapped with each other
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(20, 50), true));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(0, 30), true));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(70, 90), true));
    Block block = DMTestEnv::prepareSimpleWriteBlock(
        0,
        num_rows_write,
        false,
        2,
        MutSup::extra_handle_column_name,
        MutSup::extra_handle_id,
        MutSup::getExtraHandleColumnStringType(),
        true,
        1);
    auto filtered_block = RowKeyFilter::filterSorted(ranges, std::move(block), 0);
    ASSERT_EQ(filtered_block.rows(), 70);
}

TEST(RowKeyFilterTest, FilterUnsortedBlockCommonHandle)
{
    const size_t num_rows_write = 100;
    RowKeyRanges ranges;
    // create ranges unsorted and overlapped with each other
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(20, 50), true));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(0, 30), true));
    ranges.emplace_back(RowKeyRange::fromHandleRange(HandleRange(70, 90), true));
    Block block = DMTestEnv::prepareSimpleWriteBlock(
        0,
        num_rows_write,
        false,
        2,
        MutSup::extra_handle_column_name,
        MutSup::extra_handle_id,
        MutSup::getExtraHandleColumnStringType(),
        true,
        1);
    auto filtered_block = RowKeyFilter::filterUnsorted(ranges, std::move(block), 0);
    ASSERT_EQ(filtered_block.rows(), 70);
}

} // namespace tests
} // namespace DM
} // namespace DB
