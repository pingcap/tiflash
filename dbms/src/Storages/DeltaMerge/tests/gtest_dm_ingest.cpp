// Copyright 2022 PingCAP, Ltd.
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

#include <Storages/DeltaMerge/tests/gtest_dm_simple_pk_test_basic.h>

namespace DB
{
namespace DM
{
namespace tests
{

class StoreIngestTest : public SimplePKTestBasic
{
};

TEST_F(StoreIngestTest, Basic)
try
{
    ASSERT_EQ(0, getRowsN());
    auto block1 = fillBlock({.range = {0, 100}});
    auto block2 = fillBlock({.range = {100, 142}});
    ingestFiles({.range = {0, 500}, .blocks = {block1, block2}, .clear = false});
    ASSERT_TRUE(isFilled(0, 142));
    ASSERT_EQ(142, getRowsN());

    ingestFiles({.range = {0, 500}, .blocks = {block1}, .clear = true});
    ASSERT_TRUE(isFilled(0, 100));
    ASSERT_EQ(100, getRowsN());
}
CATCH

TEST_F(StoreIngestTest, RangeSmallerThanData)
try
{
    ASSERT_EQ(0, getRowsN());
    auto block1 = fillBlock({.range = {0, 100}});
    ASSERT_THROW({
        ingestFiles({.range = {20, 40}, .blocks = {block1}, .clear = false});
    },  DB::Exception);
}
CATCH

TEST_F(StoreIngestTest, RangeLargerThanData)
try
{
    ASSERT_EQ(0, getRowsN());
    auto block1 = fillBlock({.range = {0, 100}});
    ingestFiles({.range = {-100, 110}, .blocks = {block1}, .clear = false});
    ASSERT_TRUE(isFilled(0, 100));
    ASSERT_EQ(100, getRowsN());

    fill(-500, 500);
    ingestFiles({.range = {-100, 110}, .blocks = {block1}, .clear = true});
    ASSERT_TRUE(isFilled(-500, -100));
    ASSERT_TRUE(isFilled(0, 100));
    ASSERT_TRUE(isFilled(110, 500));
    ASSERT_EQ(890, getRowsN());
}
CATCH

TEST_F(StoreIngestTest, OverlappedFiles)
try
{
    auto block1 = fillBlock({.range = {0, 100}});
    auto block2 = fillBlock({.range = {99, 105}});

    ASSERT_THROW({
        ingestFiles({.range = {0, 500}, .blocks = {block1, block2}});
    },
                 DB::Exception);

    ASSERT_THROW({
        ingestFiles({.range = {0, 500}, .blocks = {block2, block1}});
    },
                 DB::Exception);
}
CATCH

TEST_F(StoreIngestTest, UnorderedFiles)
try
{
    auto block1 = fillBlock({.range = {0, 100}});
    auto block2 = fillBlock({.range = {100, 142}});
    ASSERT_THROW({
        ingestFiles({.range = {0, 500}, .blocks = {block2, block1}});
    }, DB::Exception);
}
CATCH


TEST_F(StoreIngestTest, EmptyFileLists)
try
{
    /// If users create an empty table with TiFlash replica, we will apply Region
    /// snapshot without any rows, which make it ingest with an empty DTFile list.
    /// Test whether we can clean the original data if `clear_data_in_range` is true.

    fill(0, 128);
    ASSERT_EQ(128, getRowsN());
    ASSERT_EQ(128, getRowsN(0, 128));

    // Test that if we ingest a empty file list, the data in range will be removed.
    ingestFiles({ .range = {32, 256}, .blocks = {}, .clear = true });

    // After ingesting, the data in [32, 128) should be overwrite by the data in ingested files.
    ASSERT_EQ(32, getRowsN());
    ASSERT_TRUE(isFilled(0, 32));
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
