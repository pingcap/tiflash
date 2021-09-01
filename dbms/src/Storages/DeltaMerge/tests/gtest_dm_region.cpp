#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Segment.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>
#include <Storages/tests/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <ctime>
#include <memory>

namespace DB
{
namespace DM
{
namespace tests
{
class DMRegion_test : public DB::base::TiFlashStorageTestBasic
{
public:
    DMRegion_test()
        : log(&Poco::Logger::get("DMRegion_test"))
    {
        settings.set("dt_segment_limit_rows", (UInt64)10000);
        settings.set("dt_segment_delta_limit_rows", (UInt64)1000);
        settings.set("dt_segment_delta_cache_limit_rows", (UInt64)500);
        settings.set("dt_segment_delta_small_pack_rows", (UInt64)100);
        settings.set("dt_segment_stable_pack_rows", (UInt64)100);

        table_columns.push_back(getExtraHandleColumnDefine(false));
        table_columns.push_back(getVersionColumnDefine());
        table_columns.push_back(getTagColumnDefine());
    }

protected:
    static void SetUpTestCase() {}

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        reload(std::move(settings));
        store = std::make_shared<DeltaMergeStore>(
            *db_context,
            false,
            "test_database",
            "test_table",
            table_columns,
            getExtraHandleColumnDefine(false),
            false,
            1);
    }

protected:
    // a ptr to context, we can reload context with different settings if need.
    ColumnDefines table_columns;
    DB::Settings settings;
    DeltaMergeStorePtr store;

    Poco::Logger * log;
};

/// TODO: we temperary disable those tests. Because they take too long time to run, and the "check_approx" could fail because it is not accurate.
/// Those test cases need to improve later.

TEST_F(DMRegion_test, DISABLED_GetRowsAndBytes)
try
{
    srand(time(NULL));
    auto random_range = [&](size_t max_rows) {
        size_t rand_start = rand() % max_rows;
        size_t rand_end = rand() % max_rows;
        rand_start = std::min(rand_start, rand_end);
        rand_end = std::max(rand_start, rand_end);

        return HandleRange((Int64)rand_start, (Int64)rand_end);
    };

    auto check_exact = [&](const RowKeyRange & range, size_t expected_rows, size_t expected_bytes) {
        auto [exact_rows, exact_bytes] = store->getRowsAndBytesInRange(*db_context, range, /*is_exact*/ true);
        ASSERT_EQ(exact_rows, expected_rows);
        ASSERT_EQ(exact_bytes, expected_bytes);
    };

    auto check_approx = [&](const RowKeyRange & range) {
        auto [exact_rows, exact_bytes] = store->getRowsAndBytesInRange(*db_context, range, /*is_exact*/ true);

        // We cannot correctly calculate too small range in approximate mode.
        if (exact_rows <= settings.dt_segment_stable_pack_rows.value * 3)
            return;
        auto [approx_rows, approx_bytes] = store->getRowsAndBytesInRange(*db_context, range, /*is_exact*/ false);
        ASSERT_LE(std::abs((Int64)(approx_rows - exact_rows)), exact_rows * 0.2);
        ASSERT_LE(std::abs((Int64)(approx_bytes - exact_bytes)), exact_bytes * 0.2);
    };

    size_t bytes_per_rows = 8 + 8 + 1;

    LOG_DEBUG(log, "Exact check");

    size_t insert_rows = 100000;
    size_t cur_rows = 0;
    while (cur_rows < insert_rows)
    {
        size_t step = rand() % 1000;
        LOG_DEBUG(log, "step " << step);
        auto block = DMTestEnv::prepareBlockWithIncreasingPKAndTs(step, cur_rows, cur_rows);
        store->write(*db_context, settings, std::move(block));
        cur_rows += step;

        check_exact(RowKeyRange::fromHandleRange({0, (Int64)(cur_rows)}), cur_rows, cur_rows * bytes_per_rows);

        auto rand_range = random_range(cur_rows);
        check_exact(RowKeyRange::fromHandleRange(rand_range),
                    rand_range.end - rand_range.start,
                    (rand_range.end - rand_range.start) * bytes_per_rows);
    }

    LOG_DEBUG(log, "Approximate check");

    check_approx(RowKeyRange::fromHandleRange({0, (Int64)(cur_rows)}));

    for (int i = 0; i < 100; ++i)
    {
        check_approx(RowKeyRange::fromHandleRange(random_range(cur_rows)));
    }

    LOG_DEBUG(log, "Approximate with delete range");

    for (int i = 0; i < 100; ++i)
    {
        size_t rand_start = rand() % cur_rows;
        size_t rand_end = rand() % cur_rows;
        rand_start = std::min(rand_start, rand_end);
        rand_end = std::max(rand_start, rand_end);

        store->deleteRange(*db_context, settings, RowKeyRange::fromHandleRange({(Int64)rand_start, (Int64)rand_end}));

        check_approx(RowKeyRange::fromHandleRange(random_range(cur_rows)));
    }
}
CATCH

TEST_F(DMRegion_test, DISABLED_GetSplitPoint)
try
{
    srand(time(NULL));
    auto random_range = [&](size_t max_rows) {
        size_t rand_start = rand() % max_rows;
        size_t rand_end = rand() % max_rows;
        rand_start = std::min(rand_start, rand_end);
        rand_end = std::max(rand_start, rand_end);

        return RowKeyRange::fromHandleRange({(Int64)rand_start, (Int64)rand_end});
    };

    auto check_split_point = [&](const RowKeyRange & range) {
        auto res = store->getRegionSplitPoint(*db_context, range, /*max_region_size*/ 0, /*split_size, useless*/ 0);
        if (res.split_points.empty())
            return;
        auto split_point = res.split_points[0];
        auto [exact_rows_1, exact_bytes_1]
            = store->getRowsAndBytesInRange(*db_context, RowKeyRange(range.start, split_point, false, 1), /*is_exact*/ true);
        auto [exact_rows_2, exact_bytes_2]
            = store->getRowsAndBytesInRange(*db_context, RowKeyRange(split_point, range.end, false, 1), /*is_exact*/ true);

        if (std::abs((Int64)(exact_rows_1 - exact_rows_2)) <= exact_rows_1 * 0.1)
        {
            ASSERT_LE(std::abs((Int64)(exact_rows_1 - exact_rows_2)), exact_rows_1 * 0.1);
            ASSERT_LE(std::abs((Int64)(exact_bytes_1 - exact_bytes_2)), exact_bytes_1 * 0.1);
        }
    };

    LOG_DEBUG(log, "Check split point");

    size_t insert_rows = 100000;
    size_t cur_rows = 0;
    while (cur_rows < insert_rows)
    {
        size_t step = rand() % 1000;
        LOG_DEBUG(log, "step " << step);
        auto block = DMTestEnv::prepareBlockWithIncreasingPKAndTs(step, cur_rows, cur_rows);
        store->write(*db_context, settings, std::move(block));
        cur_rows += step;

        check_split_point(random_range(cur_rows));
    }

    LOG_DEBUG(log, "Check split point with delete range");

    for (int i = 0; i < 100; ++i)
    {
        size_t rand_start = rand() % cur_rows;
        size_t rand_end = rand() % cur_rows;
        rand_start = std::min(rand_start, rand_end);
        rand_end = std::max(rand_start, rand_end);

        store->deleteRange(*db_context, settings, RowKeyRange::fromHandleRange({(Int64)rand_start, (Int64)rand_end}));

        check_split_point(random_range(cur_rows));
    }
}
CATCH

} // namespace tests
} // namespace DM
} // namespace DB
