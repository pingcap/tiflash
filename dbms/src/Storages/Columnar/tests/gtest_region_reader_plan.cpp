// Copyright 2026 PingCAP, Inc.
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

#include <Common/config.h> // for ENABLE_NEXT_GEN_COLUMNAR
#if ENABLE_NEXT_GEN_COLUMNAR

#include <Common/FailPoint.h>
#include <Debug/TiFlashTestEnv.h>
#include <Storages/Columnar/ColumnarReader.h>
#include <Storages/KVStore/TiKVHelpers/TiKVRecordFormat.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <algorithm>

namespace pingcap::coprocessor
{
bool operator==(const pingcap::coprocessor::KeyRange & lhs, const pingcap::coprocessor::KeyRange & rhs)
{
    return lhs.start_key == rhs.start_key && lhs.end_key == rhs.end_key;
}
bool operator!=(const pingcap::coprocessor::KeyRange & lhs, const pingcap::coprocessor::KeyRange & rhs)
{
    return !(lhs == rhs);
}
} // namespace pingcap::coprocessor

namespace DB
{
namespace FailPoints
{
extern const char force_return_columnar_region_bucket_keys[];
} // namespace FailPoints

namespace tests
{
namespace
{


String normalizedKey(TableID table_id, HandleID handle_id)
{
    const auto key = RecordKVFormat::genKey(table_id, handle_id);
    return String(key.data(), key.dataSize());
}

String encodedKey(TableID table_id, HandleID handle_id)
{
    const auto key = RecordKVFormat::genKey(table_id, handle_id);
    const auto encoded = RecordKVFormat::encodeAsTiKVKey(key);
    return String(encoded.data(), encoded.dataSize());
}

ColumnarPhysicalTableRanges singleTableRange(TableID table_id, HandleID start, HandleID end)
{
    return ColumnarPhysicalTableRanges{std::make_tuple(
        table_id,
        pingcap::coprocessor::KeyRanges{pingcap::coprocessor::KeyRange{
            normalizedKey(table_id, start),
            normalizedKey(table_id, end),
        }})};
}

std::unordered_map<RegionID, ColumnarPhysicalTableRanges> makeRemoteRegionsByRegion(
    RegionID region_id,
    ColumnarPhysicalTableRanges physical_table_ranges)
{
    return {{region_id, std::move(physical_table_ranges)}};
}

std::unordered_map<RegionID, pingcap::kv::RegionVerID> makeRegionVerIDs(RegionID region_id)
{
    return {{region_id, pingcap::kv::RegionVerID(region_id, /*conf_ver=*/1, /*ver=*/2)}};
}

void sortFlattenedPlansByRangeStart(std::vector<ColumnarReaderPlan> & plans)
{
    std::sort(plans.begin(), plans.end(), [](const auto & lhs, const auto & rhs) {
        const auto & lhs_range = std::get<1>(lhs.physical_table_ranges.front()).front();
        const auto & rhs_range = std::get<1>(rhs.physical_table_ranges.front()).front();
        return lhs_range.start_key < rhs_range.start_key;
    });
}

class RegionBucketKeysFailPointGuard
{
public:
    explicit RegionBucketKeysFailPointGuard(std::unordered_map<RegionID, std::vector<String>> bucket_keys_by_region)
    {
        FailPointHelper::enableFailPoint(
            FailPoints::force_return_columnar_region_bucket_keys,
            std::make_any<std::unordered_map<RegionID, std::vector<String>>>(std::move(bucket_keys_by_region)));
    }

    ~RegionBucketKeysFailPointGuard()
    {
        FailPointHelper::disableFailPoint(FailPoints::force_return_columnar_region_bucket_keys);
    }
};

} // namespace

class ColumnarRegionReaderPlanTest : public ::testing::Test
{
protected:
    ContextPtr context = TiFlashTestEnv::getContext();
};

TEST_F(ColumnarRegionReaderPlanTest, IsBucketBoundaryInsideRange)
{
    const pingcap::coprocessor::KeyRange range{"b", "f"};

    EXPECT_FALSE(isBucketBoundaryInsideRange("", range));
    EXPECT_FALSE(isBucketBoundaryInsideRange("b", range));
    EXPECT_FALSE(isBucketBoundaryInsideRange("f", range));
    EXPECT_TRUE(isBucketBoundaryInsideRange("c", range));
    EXPECT_TRUE(isBucketBoundaryInsideRange("e", range));
}

TEST_F(ColumnarRegionReaderPlanTest, SplitRangesByBucketKeys)
{
    constexpr TableID table_id = 100;

    {
        const auto physical_table_ranges = singleTableRange(table_id, 10, 100);
        // Bucket keys is the Region boundary, no split should happen.
        const auto [has_bucket_split, units] = splitRangesByBucketKeys(
            physical_table_ranges,
            {
                encodedKey(table_id, 10),
                encodedKey(table_id, 100),
            });

        EXPECT_FALSE(has_bucket_split);
        EXPECT_TRUE(units.empty());
    }

    {
        const auto physical_table_ranges = singleTableRange(table_id, 10, 100);
        // Bucket keys split the range into 3 parts: [10,30), [30,60), [60,100).
        const auto [has_bucket_split, units] = splitRangesByBucketKeys(
            physical_table_ranges,
            {
                encodedKey(table_id, 10),
                encodedKey(table_id, 30),
                encodedKey(table_id, 60),
                encodedKey(table_id, 100),
            });

        ASSERT_TRUE(has_bucket_split);
        ASSERT_EQ(units.size(), 3U);
        EXPECT_EQ(units[0].first, table_id);
        EXPECT_EQ(units[0].second.start_key, normalizedKey(table_id, 10));
        EXPECT_EQ(units[0].second.end_key, normalizedKey(table_id, 30));
        EXPECT_EQ(units[1].second.start_key, normalizedKey(table_id, 30));
        EXPECT_EQ(units[1].second.end_key, normalizedKey(table_id, 60));
        EXPECT_EQ(units[2].second.start_key, normalizedKey(table_id, 60));
        EXPECT_EQ(units[2].second.end_key, normalizedKey(table_id, 100));
    }

    {
        const auto physical_table_ranges = singleTableRange(table_id, 20, 80);
        // Bucket keys split the range into 2 parts: [20,30), [30,80).
        const auto [has_bucket_split, units] = splitRangesByBucketKeys(
            physical_table_ranges,
            {
                encodedKey(table_id, 10),
                encodedKey(table_id, 30),
                encodedKey(table_id, 90),
                encodedKey(table_id, 100),
            });

        ASSERT_TRUE(has_bucket_split);
        ASSERT_EQ(units.size(), 2U);
        EXPECT_EQ(units[0].second.start_key, normalizedKey(table_id, 20));
        EXPECT_EQ(units[0].second.end_key, normalizedKey(table_id, 30));
        EXPECT_EQ(units[1].second.start_key, normalizedKey(table_id, 30));
        EXPECT_EQ(units[1].second.end_key, normalizedKey(table_id, 80));
    }

    {
        const auto physical_table_ranges = singleTableRange(table_id, 10, 100);
        // Bucket keys split the range into 3 parts: [10,30), [30,90), [90,100).
        const auto [has_bucket_split, units] = splitRangesByBucketKeys(
            physical_table_ranges,
            {
                encodedKey(table_id, 10),
                encodedKey(table_id, 30),
                encodedKey(table_id, 90),
                encodedKey(table_id, 100),
            });

        ASSERT_TRUE(has_bucket_split);
        ASSERT_EQ(units.size(), 3);
        EXPECT_EQ(units[0].second.start_key, normalizedKey(table_id, 10));
        EXPECT_EQ(units[0].second.end_key, normalizedKey(table_id, 30));
        EXPECT_EQ(units[1].second.start_key, normalizedKey(table_id, 30));
        EXPECT_EQ(units[1].second.end_key, normalizedKey(table_id, 90));
        EXPECT_EQ(units[2].second.start_key, normalizedKey(table_id, 90));
        EXPECT_EQ(units[2].second.end_key, normalizedKey(table_id, 100));
    }
}

TEST_F(ColumnarRegionReaderPlanTest, BuildRegionReaderPlansWithoutBucketParallel)
{
    constexpr RegionID region_id = 1000;
    constexpr TableID table_id = 100;
    auto all_remote_regions_by_region = makeRemoteRegionsByRegion(region_id, singleTableRange(table_id, 10, 100));
    auto region_ver_ids = makeRegionVerIDs(region_id);

    // `enable_bucket_parallel` is false, bucket split should not be applied
    const auto output = buildColumnarRegionReaderPlans(*context, all_remote_regions_by_region, region_ver_ids, false);

    ASSERT_EQ(output.planned_reader_num, 1U);
    ASSERT_EQ(output.total_split_bucket_num, 0U);
    ASSERT_EQ(output.region_reader_plans.size(), 1U);
    EXPECT_TRUE(output.region_reader_plans[0].bucket_units.empty());
    EXPECT_EQ(output.region_reader_plans[0].region_id, region_id);
}

TEST_F(ColumnarRegionReaderPlanTest, BuildRegionReaderPlansWithBucketParallel)
{
    constexpr RegionID region_id = 1000;
    constexpr TableID table_id = 100;
    auto all_remote_regions_by_region = makeRemoteRegionsByRegion(region_id, singleTableRange(table_id, 10, 100));
    auto region_ver_ids = makeRegionVerIDs(region_id);
    RegionBucketKeysFailPointGuard failpoint_guard({
        {region_id,
         {
             encodedKey(table_id, 10),
             encodedKey(table_id, 30),
             encodedKey(table_id, 60),
             encodedKey(table_id, 100),
         }},
    });

    // read range [10,100) can be split into 3 bucket units: [10,30), [30,60), [60,100).
    const auto output = buildColumnarRegionReaderPlans(*context, all_remote_regions_by_region, region_ver_ids, true);

    ASSERT_EQ(output.planned_reader_num, 3U);
    ASSERT_EQ(output.total_split_bucket_num, 3U);
    ASSERT_EQ(output.region_reader_plans.size(), 1U);
    ASSERT_EQ(output.region_reader_plans[0].bucket_units.size(), 3U);
}

TEST_F(ColumnarRegionReaderPlanTest, BuildRegionReaderPlansBucketSplitNotAppliedWhenOnlyOneUnit)
{
    constexpr RegionID region_id = 1000;
    constexpr TableID table_id = 100;
    auto all_remote_regions_by_region = makeRemoteRegionsByRegion(region_id, singleTableRange(table_id, 10, 100));
    auto region_ver_ids = makeRegionVerIDs(region_id);
    RegionBucketKeysFailPointGuard failpoint_guard({
        {region_id, {encodedKey(table_id, 10), encodedKey(table_id, 100)}},
    });

    const auto output = buildColumnarRegionReaderPlans(*context, all_remote_regions_by_region, region_ver_ids, true);

    ASSERT_EQ(output.planned_reader_num, 1U);
    ASSERT_EQ(output.total_split_bucket_num, 0U);
    EXPECT_TRUE(output.region_reader_plans[0].bucket_units.empty());
}

TEST_F(ColumnarRegionReaderPlanTest, BuildRegionReaderPlansMultipleRegions)
{
    constexpr TableID table_id = 100;
    std::unordered_map<RegionID, ColumnarPhysicalTableRanges> all_remote_regions_by_region{
        {1000, singleTableRange(table_id, 10, 100)},
        {2000, singleTableRange(table_id, 200, 300)},
    };
    std::unordered_map<RegionID, pingcap::kv::RegionVerID> region_ver_ids{
        {1000, pingcap::kv::RegionVerID(1000, 1, 2)},
        {2000, pingcap::kv::RegionVerID(2000, 1, 2)},
    };
    RegionBucketKeysFailPointGuard failpoint_guard({
        {1000,
         {
             encodedKey(table_id, 10),
             encodedKey(table_id, 30),
             encodedKey(table_id, 60),
             encodedKey(table_id, 100),
         }},
        {2000, {encodedKey(table_id, 200), encodedKey(table_id, 300)}},
    });

    // Region 1000 can be split into 3 bucket units: [10,30), [30,60), [60,100).
    // Region 2000 cannot be split since the bucket keys are the same as region boundaries.
    const auto output = buildColumnarRegionReaderPlans(*context, all_remote_regions_by_region, region_ver_ids, true);

    ASSERT_EQ(output.planned_reader_num, 4U);
    ASSERT_EQ(output.total_split_bucket_num, 3U);

    size_t bucket_split_region_count = 0;
    for (const auto & plan : output.region_reader_plans)
    {
        if (plan.region_id == 1000)
        {
            ASSERT_EQ(plan.bucket_units.size(), 3U);
            ++bucket_split_region_count;
        }
        else if (plan.region_id == 2000)
        {
            EXPECT_TRUE(plan.bucket_units.empty());
            ++bucket_split_region_count;
        }
    }
    EXPECT_EQ(bucket_split_region_count, 2U);
}

TEST_F(ColumnarRegionReaderPlanTest, FlattenRegionReaderPlansWithoutBucketSplit)
{
    constexpr RegionID region_id = 1000;
    constexpr TableID table_id = 100;
    const auto physical_table_ranges = singleTableRange(table_id, 10, 100);
    std::vector<ColumnarRegionReaderPlan> region_reader_plans{
        ColumnarRegionReaderPlan{
            .region_id = region_id,
            .region_ver_id = pingcap::kv::RegionVerID(region_id, 1, 2),
            .physical_table_ranges = physical_table_ranges,
        },
    };

    const auto flattened = flattenColumnarRegionReaderPlans(region_reader_plans);
    ASSERT_EQ(flattened.size(), 1U);
    EXPECT_EQ(flattened[0].region_id, region_id);
    EXPECT_EQ(flattened[0].region_ver, 2U);
    EXPECT_EQ(flattened[0].region_conf_ver, 1U);
    EXPECT_EQ(flattened[0].physical_table_ranges, physical_table_ranges);
}

TEST_F(ColumnarRegionReaderPlanTest, FlattenRegionReaderPlansWithBucketSplit)
{
    constexpr RegionID region_id = 1000;
    constexpr TableID table_id = 100;
    const auto physical_table_ranges = singleTableRange(table_id, 10, 100);
    const auto [has_bucket_split, units] = splitRangesByBucketKeys(
        physical_table_ranges,
        {
            encodedKey(table_id, 10),
            encodedKey(table_id, 30),
            encodedKey(table_id, 60),
            encodedKey(table_id, 100),
        });
    ASSERT_TRUE(has_bucket_split);
    std::vector<ColumnarRegionReaderPlan> region_reader_plans{
        ColumnarRegionReaderPlan{
            .region_id = region_id,
            .region_ver_id = pingcap::kv::RegionVerID(region_id, 1, 2),
            .physical_table_ranges = physical_table_ranges,
            .bucket_units = units,
        },
    };

    auto flattened = flattenColumnarRegionReaderPlans(region_reader_plans);
    ASSERT_EQ(flattened.size(), 3U);
    sortFlattenedPlansByRangeStart(flattened);
    EXPECT_EQ(
        flattened[0].physical_table_ranges.front(),
        std::make_tuple(
            table_id,
            pingcap::coprocessor::KeyRanges{
                units[0].second,
            }));
    EXPECT_EQ(
        flattened[1].physical_table_ranges.front(),
        std::make_tuple(
            table_id,
            pingcap::coprocessor::KeyRanges{
                units[1].second,
            }));
    EXPECT_EQ(
        flattened[2].physical_table_ranges.front(),
        std::make_tuple(
            table_id,
            pingcap::coprocessor::KeyRanges{
                units[2].second,
            }));
}

} // namespace tests
} // namespace DB

#endif
