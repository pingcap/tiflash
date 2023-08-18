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

#include <Flash/Coprocessor/RequestUtils.h>
#include <gtest/gtest.h>

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <kvproto/coprocessor.pb.h>
#include <kvproto/mpp.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/RegionCache.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

namespace DB::tests
{
bool isSameKeyRange(const coprocessor::KeyRange & actual, const pingcap::coprocessor::KeyRange & expect)
{
    return (actual.start() == expect.start_key && actual.end() == expect.end_key);
}
#define ASSERT_RANGE_EQ(pb_keyrange, expect) ASSERT_TRUE(isSameKeyRange((pb_keyrange), (expect)))

bool isSameRegionId(const coprocessor::RegionInfo & actual, const pingcap::kv::RegionVerID & expect)
{
    return actual.region_id() == expect.id && actual.region_epoch().conf_ver() == expect.conf_ver
        && actual.region_epoch().version() == expect.ver;
}
#define ASSERT_REGION_ID_EQ(pb_region, expect) ASSERT_TRUE(isSameRegionId((pb_region), (expect)))

TEST(RequestEncodeTest, MPPNonPartitionRegions)
{
    namespace pc = pingcap::coprocessor;
    namespace pkv = pingcap::kv;
    pc::BatchCopTask batch_cop_task;
    std::vector<pc::RegionInfo> regions{
        pc::RegionInfo{.region_id = {111, 1, 4}, .ranges = {{"a", "b"}, {"b", "c"}}},
        pc::RegionInfo{.region_id = {222, 2, 5}, .ranges = {{"dd", "e"}, {"f", "g"}}},
        pc::RegionInfo{.region_id = {333, 3, 6}, .ranges = {{"x", "y"}}},
    };
    batch_cop_task.region_infos = regions;

    ::mpp::DispatchTaskRequest req;
    RequestUtils::setUpRegionInfos(batch_cop_task, &req);

    ASSERT_EQ(req.regions_size(), 3);
    ASSERT_EQ(req.table_regions_size(), 0);

    auto reg0 = req.regions(0);
    ASSERT_REGION_ID_EQ(reg0, pkv::RegionVerID(111, 1, 4));
    ASSERT_EQ(reg0.ranges_size(), 2);
    ASSERT_RANGE_EQ(reg0.ranges(0), pc::KeyRange("a", "b"));
    ASSERT_RANGE_EQ(reg0.ranges(1), pc::KeyRange("b", "c"));

    auto reg1 = req.regions(1);
    ASSERT_REGION_ID_EQ(reg1, pkv::RegionVerID(222, 2, 5));
    ASSERT_EQ(reg1.ranges_size(), 2);
    ASSERT_RANGE_EQ(reg1.ranges(0), pc::KeyRange("dd", "e"));
    ASSERT_RANGE_EQ(reg1.ranges(1), pc::KeyRange("f", "g"));

    auto reg2 = req.regions(2);
    ASSERT_REGION_ID_EQ(reg2, pkv::RegionVerID(333, 3, 6));
    ASSERT_EQ(reg2.ranges_size(), 1);
    ASSERT_RANGE_EQ(reg2.ranges(0), pc::KeyRange("x", "y"));
}

TEST(RequestEncodeTest, MPPPartitionRegions)
{
    namespace pc = pingcap::coprocessor;
    namespace pkv = pingcap::kv;
    pc::BatchCopTask batch_cop_task;
    std::vector<pc::TableRegions> regions{
        // clang-format off
        pc::TableRegions{
            .physical_table_id = 70,
            .region_infos = {
                pc::RegionInfo{.region_id = {111, 1, 4}, .ranges = {{"a", "b"}, {"b", "c"}}},
                pc::RegionInfo{.region_id = {222, 2, 5}, .ranges = {{"dd", "e"}, {"f", "g"}}},
                pc::RegionInfo{.region_id = {333, 3, 6}, .ranges = {{"x", "y"}}},
            }},
        pc::TableRegions{
            .physical_table_id = 71,
            .region_infos = {
                pc::RegionInfo{.region_id = {444, 1, 4}, .ranges = {{"z", "z0"}, {"z2", "z3"}}},
                pc::RegionInfo{.region_id = {555, 3, 6}, .ranges = {{"zy", "zz"}}},
            }},
        // clang-format on
    };
    batch_cop_task.table_regions = regions;

    ::mpp::DispatchTaskRequest req;
    RequestUtils::setUpRegionInfos(batch_cop_task, &req);

    ASSERT_EQ(req.regions_size(), 0);
    ASSERT_EQ(req.table_regions_size(), 2);

    {
        const auto & table0 = req.table_regions(0);
        ASSERT_EQ(table0.regions_size(), 3);

        const auto & reg0 = table0.regions(0);
        ASSERT_REGION_ID_EQ(reg0, pkv::RegionVerID(111, 1, 4));
        ASSERT_EQ(reg0.ranges_size(), 2);
        ASSERT_RANGE_EQ(reg0.ranges(0), pc::KeyRange("a", "b"));
        ASSERT_RANGE_EQ(reg0.ranges(1), pc::KeyRange("b", "c"));

        const auto & reg1 = table0.regions(1);
        ASSERT_REGION_ID_EQ(reg1, pkv::RegionVerID(222, 2, 5));
        ASSERT_EQ(reg1.ranges_size(), 2);
        ASSERT_RANGE_EQ(reg1.ranges(0), pc::KeyRange("dd", "e"));
        ASSERT_RANGE_EQ(reg1.ranges(1), pc::KeyRange("f", "g"));

        const auto & reg2 = table0.regions(2);
        ASSERT_REGION_ID_EQ(reg2, pkv::RegionVerID(333, 3, 6));
        ASSERT_EQ(reg2.ranges_size(), 1);
        ASSERT_RANGE_EQ(reg2.ranges(0), pc::KeyRange("x", "y"));
    }

    {
        const auto & table1 = req.table_regions(1);
        ASSERT_EQ(table1.regions_size(), 2);

        const auto & reg0 = table1.regions(0);
        ASSERT_REGION_ID_EQ(reg0, pkv::RegionVerID(444, 1, 4));
        ASSERT_EQ(reg0.ranges_size(), 2);
        ASSERT_RANGE_EQ(reg0.ranges(0), pc::KeyRange("z", "z0"));
        ASSERT_RANGE_EQ(reg0.ranges(1), pc::KeyRange("z2", "z3"));

        const auto & reg1 = table1.regions(1);
        ASSERT_REGION_ID_EQ(reg1, pkv::RegionVerID(555, 3, 6));
        ASSERT_EQ(reg1.ranges_size(), 1);
        ASSERT_RANGE_EQ(reg1.ranges(0), pc::KeyRange("zy", "zz"));
    }
}
} // namespace DB::tests
