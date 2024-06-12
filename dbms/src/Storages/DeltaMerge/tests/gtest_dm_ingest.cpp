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

#include <Common/FailPoint.h>
#include <Common/UniThreadPool.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/GCOptions.h>
#include <Storages/DeltaMerge/tests/gtest_dm_simple_pk_test_basic.h>
#include <TestUtils/InputStreamTestUtils.h>

#include <future>
#include <random>

namespace DB
{

namespace FailPoints
{
extern const char force_ingest_via_delta[];
extern const char force_ingest_via_replace[];
} // namespace FailPoints

namespace DM
{
namespace tests
{

class StoreIngestTest
    : public SimplePKTestBasic
    , public testing::WithParamInterface<bool /* ingest_by_split */>
{
public:
    StoreIngestTest()
        : ingest_by_split(GetParam())
    {
        if (ingest_by_split)
            FailPointHelper::enableFailPoint(FailPoints::force_ingest_via_replace);
        else
            FailPointHelper::enableFailPoint(FailPoints::force_ingest_via_delta);
    }

    ~StoreIngestTest()
    {
        if (ingest_by_split)
            FailPointHelper::disableFailPoint(FailPoints::force_ingest_via_replace);
        else
            FailPointHelper::disableFailPoint(FailPoints::force_ingest_via_delta);
    }

    const bool ingest_by_split;
};

INSTANTIATE_TEST_CASE_P(Group, StoreIngestTest, ::testing::Bool());

TEST_P(StoreIngestTest, Basic)
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

TEST_P(StoreIngestTest, RangeSmallerThanData)
try
{
    ASSERT_EQ(0, getRowsN());
    auto block1 = fillBlock({.range = {0, 100}});
    ASSERT_THROW({ ingestFiles({.range = {20, 40}, .blocks = {block1}, .clear = false}); }, DB::Exception);
}
CATCH

TEST_P(StoreIngestTest, RangeLargerThanData)
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

TEST_P(StoreIngestTest, OverlappedFiles)
try
{
    auto block1 = fillBlock({.range = {0, 100}});
    auto block2 = fillBlock({.range = {99, 105}});

    ASSERT_THROW({ ingestFiles({.range = {0, 500}, .blocks = {block1, block2}}); }, DB::Exception);

    ASSERT_THROW({ ingestFiles({.range = {0, 500}, .blocks = {block2, block1}}); }, DB::Exception);
}
CATCH

TEST_P(StoreIngestTest, UnorderedFiles)
try
{
    auto block1 = fillBlock({.range = {0, 100}});
    auto block2 = fillBlock({.range = {100, 142}});
    ASSERT_THROW({ ingestFiles({.range = {0, 500}, .blocks = {block2, block1}}); }, DB::Exception);
}
CATCH

TEST_P(StoreIngestTest, EmptyFileLists)
try
{
    /// If users create an empty table with TiFlash replica, we will apply Region
    /// snapshot without any rows, which make it ingest with an empty DTFile list.
    /// Test whether we can clean the original data if `clear_data_in_range` is true.

    fill(0, 128);
    ASSERT_EQ(128, getRowsN());
    ASSERT_EQ(128, getRowsN(0, 128));

    // Test that if we ingest a empty file list, the data in range will be removed.
    ingestFiles({.range = {32, 256}, .blocks = {}, .clear = true});

    // After ingesting, the data in [32, 128) should be overwrite by the data in ingested files.
    ASSERT_EQ(32, getRowsN());
    ASSERT_TRUE(isFilled(0, 32));
}
CATCH

TEST_P(StoreIngestTest, ConcurrentIngestAndWrite)
try
{
    auto log = Logger::get("ConcurrentIngestAndWrite");

    std::mt19937 random;
    {
        auto const seed = std::random_device{}();
        random = std::mt19937{seed};
    }

    constexpr int upper_bound = 10000;
    constexpr int n_operations = 300;

    auto filled_bitmap = std::vector<bool>();
    filled_bitmap.resize(upper_bound);
    std::fill(filled_bitmap.begin(), filled_bitmap.end(), 0);

    size_t filled_n = 0;
    size_t filled_n_raw = 0;

    struct Operation
    {
        Block block;
        int start_key;
        int end_key;
        bool use_write;
    };

    auto ops = std::vector<Operation>();
    ops.reserve(n_operations);

    // Prepare blocks. Blocks may overlap.
    {
        auto dist_start_key = std::uniform_int_distribution{0, upper_bound - 5};
        auto dist_keys = std::uniform_int_distribution{1, 20};
        auto dist_use_write = std::uniform_int_distribution{0, 99};

        // Create N=operations operation sequence.
        // Each operation is either write or ingest several rows within the [0, upper_bound).
        for (int i = 0; i < n_operations; ++i)
        {
            auto use_write = dist_use_write(random) < 50; // 50% write, 50% ingest.
            auto start_key = dist_start_key(random);
            auto end_key = start_key + dist_keys(random);
            if (end_key > upper_bound)
                end_key = upper_bound;
            RUNTIME_CHECK(start_key < end_key);

            filled_n_raw += (end_key - start_key);

            auto block = fillBlock({.range = {start_key, end_key}});
            ops.emplace_back(Operation{
                .block = block,
                .start_key = start_key,
                .end_key = end_key,
                .use_write = use_write,
            });

            // Also mark whether row is set in the filled_bitmap. This is used to
            // verify correctness later.
            for (int row = start_key; row < end_key; row++)
                filled_bitmap[row] = true;
        }

        for (const auto filled : filled_bitmap)
            if (filled)
                filled_n++;
    }

    auto pool = std::make_shared<ThreadPool>(4);
    for (const auto & op : ops)
    {
        pool->scheduleOrThrowOnError([=, this, &log] {
            try
            {
                LOG_INFO(log, "{} to [{}, {})", op.use_write ? "write" : "ingest", op.start_key, op.end_key);

                ingestFiles({.range = {op.start_key, op.end_key}, .blocks = {op.block}, .clear = false});
            }
            CATCH
        });
    }

    pool->wait();

    auto statistics_segments_n = store->segments.size();

    ASSERT_EQ(filled_n, getRowsN());
    ASSERT_EQ(filled_n_raw, getRawRowsN());
    {
        // Check PK column values.
        auto expected_pk_column = std::vector<Int64>();
        for (int i = 0; i < upper_bound; ++i)
            if (filled_bitmap[i])
                expected_pk_column.emplace_back(i);
        auto stream = store->read(
            *db_context,
            db_context->getSettingsRef(),
            store->getTableColumns(),
            {RowKeyRange::newAll(is_common_handle, 1)},
            /* num_streams= */ 1,
            /* start_ts= */ std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            "",
            /* keep_order= */ true)[0];
        ASSERT_INPUTSTREAM_COLS_UR(
            stream,
            Strings({DMTestEnv::pk_name}),
            createColumns({
                createColumn<Int64>(expected_pk_column),
            }));
    }

    store->onSyncGc(10000, GCOptions::newAllForTest());
    ASSERT_EQ(filled_n, getRowsN());
    ASSERT_EQ(filled_n_raw, getRawRowsN());

    LOG_INFO(
        log,
        "Test finished, {} segments after all operations, {} segments after gc",
        statistics_segments_n,
        store->segments.size());
    LOG_INFO(log, "{} rows are filled in [0, {}), without MVCC = {} rows", filled_n, upper_bound, filled_n_raw);
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
