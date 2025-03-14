// Copyright 2025 PingCAP, Inc.
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

#include <Storages/DeltaMerge/File/ColumnCache.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{

TEST(ColumnCacheTest, BasicOperations)
try
{
    auto cache = ColumnCache();

    // Test put and get
    ColumnID col_id = 1;
    auto data = genSequence<Int64>("[0, 5)");
    auto col = ::DB::tests::createColumn<Int64>(data, "", col_id).column;
    cache.tryPutColumn(0, 1, col, 0, 5);

    ASSERT_TRUE(cache.getReadStrategy(0, 1, col_id)[0].second == ColumnCache::Strategy::Memory);

    {
        // Get the column that exactly matches the cache
        auto get_col = cache.getColumn(0, 1, 5, col_id);
        ASSERT_EQ(get_col->size(), 5);
    }
    {
        // Get a part of the cache
        auto get_col = cache.getColumn(0, 1, 3, col_id);
        ASSERT_EQ(get_col->size(), 3);
    }

    // Test delete
    cache.delColumn(1, 1);
    ASSERT_TRUE(cache.getReadStrategy(0, 1, col_id)[0].second == ColumnCache::Strategy::Disk);
}
CATCH

TEST(ColumnCacheTest, RangeStrategy)
try
{
    auto cache = ColumnCache();

    ColumnID col_id = 1;
    // Prepare test data
    {
        auto data = genSequence<Int64>("[0, 10)");
        auto col = ::DB::tests::createColumn<Int64>(data, "", col_id).column;
        // pack0, pack1 share the same ColumnPtr with different rows_offset
        cache.tryPutColumn(0, col_id, col, 0, 5);
        cache.tryPutColumn(1, col_id, col, 5, 5);
    }
    {
        auto data = genSequence<Int64>("[10, 20)");
        auto col = ::DB::tests::createColumn<Int64>(data, "", col_id).column;
        // pack2, pack3 share the same ColumnPtr with different rows_offset
        cache.tryPutColumn(2, col_id, col, 0, 5);
        cache.tryPutColumn(3, col_id, col, 5, 5);
    }

    // Test continuous range
    {
        // read pack0, pack1
        auto strategies = cache.getReadStrategy(0, 2, col_id);
        ASSERT_EQ(strategies.size(), 1);
        ASSERT_EQ(strategies[0].second, ColumnCache::Strategy::Memory);
        auto get_col = cache.getColumn(0, 2, 10, col_id);
        auto data = genSequence<Int64>("[0, 10)");
        const auto & actual_data = toColumnVectorData<Int64>(get_col);
        ASSERT_TRUE(sequenceEqual(data, actual_data));
    }
    {
        // read pack1, pack2
        auto strategies = cache.getReadStrategy(1, 2, col_id);
        ASSERT_EQ(strategies.size(), 1);
        ASSERT_EQ(strategies[0].second, ColumnCache::Strategy::Memory);
        auto get_col = cache.getColumn(1, 3, 10, col_id);
        auto data = genSequence<Int64>("[5, 15)");
        const auto & actual_data = toColumnVectorData<Int64>(get_col);
        ASSERT_TRUE(sequenceEqual(data, actual_data));
    }

    // Test mixed range
    cache.delColumn(col_id, 1);
    auto strategies = cache.getReadStrategy(0, 2, col_id);
    ASSERT_EQ(strategies.size(), 2);
    ASSERT_EQ(strategies[0].second, ColumnCache::Strategy::Disk);
    ASSERT_EQ(strategies[1].second, ColumnCache::Strategy::Memory);
}
CATCH

TEST(ColumnCacheTest, CleanReadStrategy)
try
{
    std::vector<size_t> clean_packs = {0, 2, 4};
    auto strategies = ColumnCache::getCleanReadStrategy(0, 5, clean_packs);

    ASSERT_EQ(strategies.size(), 5);
    ASSERT_EQ(strategies[0].second, ColumnCache::Strategy::Memory);
    ASSERT_EQ(strategies[1].second, ColumnCache::Strategy::Disk);
    ASSERT_EQ(strategies[2].second, ColumnCache::Strategy::Memory);
    ASSERT_EQ(strategies[3].second, ColumnCache::Strategy::Disk);
    ASSERT_EQ(strategies[4].second, ColumnCache::Strategy::Memory);
}
CATCH

} // namespace DB::DM::tests
