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
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/DeltaMerge/ReadThread/ColumnSharingCache.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{

constexpr int TEST_PACK_ROWS = 1024;
DataTypePtr TEST_DATA_TYPE = std::make_shared<DataTypeString>();

ColumnPtr createColumn(int packs)
{
    int rows = packs * TEST_PACK_ROWS;

    auto mut_col = TEST_DATA_TYPE->createColumn();
    for (int i = 0; i < rows; i++)
    {
        Field f = std::to_string(packs * 100000 + i);
        mut_col->insert(f);
    }
    return std::move(mut_col);
}

void compareColumn(ColumnPtr & col1, ColumnPtr & col2, int rows)
{
    for (int i = 0; i < rows; i++)
    {
        ASSERT_EQ((*col1)[i].toString(), (*col2)[i].toString());
    }
}

TEST(ColumnSharingCacheTest, AddAndGet)
{
    ColumnSharingCache cache;

    auto col = createColumn(8);
    cache.add(1, 8, col);

    ColumnPtr col1;
    auto st = cache.get(1, 8, 8 * TEST_PACK_ROWS, col1, TEST_DATA_TYPE);
    ASSERT_EQ(st, ColumnCacheStatus::GET_HIT);
    ASSERT_EQ(col1->size(), 8 * TEST_PACK_ROWS);
    compareColumn(col1, col, col1->size());

    ColumnPtr col2;
    st = cache.get(1, 7, 7 * TEST_PACK_ROWS, col2, TEST_DATA_TYPE);
    ASSERT_EQ(st, ColumnCacheStatus::GET_COPY);
    ASSERT_EQ(col2->size(), 7 * TEST_PACK_ROWS);
    compareColumn(col2, col, col2->size());

    ColumnPtr col3;
    st = cache.get(1, 9, 9 * TEST_PACK_ROWS, col3, TEST_DATA_TYPE);
    ASSERT_EQ(st, ColumnCacheStatus::GET_PART);
    ASSERT_EQ(col3, nullptr);

    ColumnPtr col4;
    st = cache.get(2, 8, 8 * TEST_PACK_ROWS, col4, TEST_DATA_TYPE);
    ASSERT_EQ(st, ColumnCacheStatus::GET_MISS);
    ASSERT_EQ(col4, nullptr);

    auto col5 = createColumn(7);
    cache.add(1, 7, col5);
    ColumnPtr col6;
    st = cache.get(1, 8, 8 * TEST_PACK_ROWS, col6, TEST_DATA_TYPE);
    ASSERT_EQ(st, ColumnCacheStatus::GET_HIT);
    ASSERT_EQ(col6->size(), 8 * TEST_PACK_ROWS);
    compareColumn(col6, col, col6->size());

    auto col7 = createColumn(9);
    cache.add(1, 9, col7);
    ColumnPtr col8;
    st = cache.get(1, 8, 8 * TEST_PACK_ROWS, col8, TEST_DATA_TYPE);
    ASSERT_EQ(st, ColumnCacheStatus::GET_COPY);
    ASSERT_EQ(col8->size(), 8 * TEST_PACK_ROWS);
    compareColumn(col8, col7, col8->size());
}

TEST(ColumnSharingCacheTest, Del)
{
    ColumnSharingCache cache;

    auto col1 = createColumn(8);
    cache.add(1, 8, col1);

    auto col2 = createColumn(8);
    cache.add(9, 8, col2);

    auto col3 = createColumn(8);
    cache.add(17, 8, col3);

    cache.del(10);

    ColumnPtr col4;
    auto st = cache.get(9, 8, 8 * TEST_PACK_ROWS, col4, TEST_DATA_TYPE);
    ASSERT_EQ(st, ColumnCacheStatus::GET_HIT);
    ASSERT_EQ(col4->size(), 8 * TEST_PACK_ROWS);
    compareColumn(col4, col2, col4->size());

    ColumnPtr col5;
    st = cache.get(17, 6, 6 * TEST_PACK_ROWS, col5, TEST_DATA_TYPE);
    ASSERT_EQ(st, ColumnCacheStatus::GET_COPY);
    ASSERT_EQ(col5->size(), 6 * TEST_PACK_ROWS);
    compareColumn(col5, col2, col5->size());
}

} // namespace DB::DM::tests