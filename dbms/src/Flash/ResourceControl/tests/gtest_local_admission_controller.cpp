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

#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <gtest/gtest.h>

namespace DB::tests
{
TEST(LocalAdmissionControllerTest, StartupRefillDoesNotUseGlobalBurstLimitAsHighWatermark)
{
    constexpr double fill_rate = 1000;
    constexpr int64_t global_burst_limit = 10000;
    constexpr double consumed_tokens = 500;
    const auto start_time = SteadyClock::now() - 2 * ResourceGroup::REFILL_TOKEN_INTERVAL;

    resource_manager::ResourceGroup group_pb;
    group_pb.set_name("startup");
    group_pb.set_mode(resource_manager::GroupMode::RUMode);
    group_pb.set_priority(ResourceGroup::UserMediumPriority);
    auto * settings = group_pb.mutable_r_u_settings()->mutable_r_u()->mutable_settings();
    settings->set_fill_rate(fill_rate);
    settings->set_burst_limit(global_burst_limit);

    ResourceGroup group(group_pb, start_time);
    group.smooth_ru_consumption_speed = 0;
    group.consumeResource(consumed_tokens, 0);

    EXPECT_FALSE(group.shouldRefillToken(start_time + ResourceGroup::REFILL_TOKEN_INTERVAL / 2));
    EXPECT_TRUE(group.shouldRefillToken(start_time + ResourceGroup::REFILL_TOKEN_INTERVAL));

    const auto request_info = group.buildRequestInfoIfNecessary(start_time + ResourceGroup::REFILL_TOKEN_INTERVAL);
    ASSERT_TRUE(request_info.has_value());
    EXPECT_DOUBLE_EQ(request_info->acquire_tokens, fill_rate * (1 - ResourceGroup::REFILL_TOKEN_THRESHOLD_RATE));
    EXPECT_DOUBLE_EQ(request_info->ru_consumption_delta, consumed_tokens);

    const auto response_time = start_time + ResourceGroup::REFILL_TOKEN_INTERVAL;
    group.updateNormalMode(request_info->acquire_tokens, global_burst_limit, response_time);
    EXPECT_FALSE(group.lowToken());
    EXPECT_TRUE(group.shouldRefillToken(response_time + ResourceGroup::REFILL_TOKEN_INTERVAL));

    const auto next_request_info
        = group.buildRequestInfoIfNecessary(response_time + ResourceGroup::REFILL_TOKEN_INTERVAL);
    ASSERT_TRUE(next_request_info.has_value());
    EXPECT_DOUBLE_EQ(
        next_request_info->acquire_tokens,
        global_burst_limit * (1 - ResourceGroup::REFILL_TOKEN_THRESHOLD_RATE));
}

TEST(LocalAdmissionControllerTest, RefillTokensIncrementallyAboveLowWatermark)
{
    constexpr double capacity = 10000;
    constexpr double consumed_tokens = 3000;

    ResourceGroup group(
        "normal_refill",
        ResourceGroup::UserMediumPriority,
        capacity,
        /*burstable_=*/false);
    group.smooth_ru_consumption_speed = 0;
    group.consumeResource(consumed_tokens, 0);

    ASSERT_TRUE(group.shouldRefillToken(SteadyClock::now()));
    auto request_info = group.buildRequestInfoIfNecessary(SteadyClock::now());
    ASSERT_TRUE(request_info.has_value());
    EXPECT_DOUBLE_EQ(request_info->acquire_tokens, capacity * (1 - ResourceGroup::REFILL_TOKEN_THRESHOLD_RATE));
    EXPECT_DOUBLE_EQ(request_info->ru_consumption_delta, consumed_tokens);
}

TEST(LocalAdmissionControllerTest, RefillTokensUsesPredictedConsumptionWhenHigher)
{
    constexpr double capacity = 10000;
    constexpr double consumed_tokens = 3000;

    ResourceGroup group(
        "high_speed",
        ResourceGroup::UserMediumPriority,
        capacity,
        /*burstable_=*/false);
    group.smooth_ru_consumption_speed = 4000;
    group.consumeResource(consumed_tokens, 0);

    auto request_info = group.buildRequestInfoIfNecessary(SteadyClock::now());
    ASSERT_TRUE(request_info.has_value());
    EXPECT_DOUBLE_EQ(request_info->acquire_tokens, consumed_tokens);
}

TEST(LocalAdmissionControllerTest, LowTokenRefillBypassesIncrementalLimit)
{
    constexpr double capacity = 10000;
    constexpr double consumed_tokens = 7500;

    ResourceGroup group(
        "emergency_refill",
        ResourceGroup::UserMediumPriority,
        capacity,
        /*burstable_=*/false);
    group.smooth_ru_consumption_speed = 0;
    group.consumeResource(consumed_tokens, 0);

    auto request_info = group.buildRequestInfoIfNecessary(SteadyClock::now());
    ASSERT_TRUE(request_info.has_value());
    EXPECT_DOUBLE_EQ(request_info->acquire_tokens, consumed_tokens);
}
} // namespace DB::tests
