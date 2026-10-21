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

#include <Flash/Coprocessor/ShardInfo.h>
#include <Storages/Tantivy/TiCIAdmission.h>
#include <gtest/gtest.h>

namespace DB::tests
{

TEST(TiCIAdmission, ParsesFreshnessFromShardRequest)
{
    coprocessor::ShardInfo request;
    request.set_shard_id(1);
    request.set_shard_epoch(2);
    request.set_required_resolved_ts(100);
    request.set_freshness_mode(coprocessor::BOUNDED_STALE);
    request.set_max_wait_ms(50);
    const ShardInfo shard(request);
    EXPECT_EQ(shard.required_resolved_ts, 100);
    EXPECT_EQ(shard.freshness_mode, static_cast<UInt8>(coprocessor::BOUNDED_STALE));
    EXPECT_EQ(shard.max_wait_ms, 50);
}

TEST(TiCIAdmission, NeverRoutesFreshnessFailureToLocalOrRemoteRead)
{
    ShardFreshnessRequirement requirement{.shard_id = 1, .expected_epoch = 2, .required_resolved_ts = 100};
    ShardAdmissionResult result{.shard_id = 1, .expected_epoch = 2, .reader_applied_resolved_ts = 100};
    for (const auto mode : {ShardFreshnessMode::StrictWatermark, ShardFreshnessMode::BoundedStale})
    {
        requirement.mode = mode;
        result.code = ShardAdmissionCode::Ready;
        EXPECT_EQ(routeTiCIShard(requirement, result), TiCIAdmissionRoute::Local);
        result.reader_applied_resolved_ts = 99;
        EXPECT_EQ(routeTiCIShard(requirement, result), TiCIAdmissionRoute::Retry);
        result.reader_applied_resolved_ts = 100;
        for (const auto code :
             {ShardAdmissionCode::Missing,
              ShardAdmissionCode::EpochMismatch,
              ShardAdmissionCode::ReaderBehind,
              ShardAdmissionCode::ControlPlaneUnavailable})
        {
            result.code = code;
            EXPECT_EQ(routeTiCIShard(requirement, result), TiCIAdmissionRoute::Retry);
        }
    }
}

TEST(TiCIAdmission, LegacyMissMayUseRemoteRead)
{
    const auto requirement = makeTiCIShardRequirement(1, 2, 100, ShardFreshnessMode::Legacy);
    EXPECT_EQ(requirement.expected_epoch, 0);
    EXPECT_EQ(requirement.required_resolved_ts, 0);
    const auto strict = makeTiCIShardRequirement(1, 2, 100, ShardFreshnessMode::StrictWatermark);
    EXPECT_EQ(strict.expected_epoch, 2);
    EXPECT_EQ(strict.required_resolved_ts, 100);
    ShardAdmissionResult result{.code = ShardAdmissionCode::Ready, .shard_id = 1, .expected_epoch = 0};
    EXPECT_EQ(routeTiCIShard(requirement, result), TiCIAdmissionRoute::Local);
    result.code = ShardAdmissionCode::Missing;
    EXPECT_EQ(routeTiCIShard(requirement, result), TiCIAdmissionRoute::Remote);
}

} // namespace DB::tests
