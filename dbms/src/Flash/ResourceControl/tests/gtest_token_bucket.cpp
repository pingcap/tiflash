// Copyright 2023 PingCAP, Ltd.
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

class TestTokenBucket : public ::testing::Test
{
};

TEST_F(TestTokenBucket, TestTrickleMode)
{
    const uint64_t ru_per_sec = 1;
    const bool burstable = false;
    ResourceGroup rg("rg1", ResourceGroup::MediumPriorityValue, ru_per_sec, burstable);

    // 1. consume many tokens and token should be negative.
    const uint64_t consume_ru = 100;
    const uint64_t consume_cpu = consume_ru * 3;
    rg.consumeResource(consume_ru, consume_cpu);

    const double add_tokens = 10;
    const double new_capacity = 1;
    const double trickle_ms = 5000;
    rg.reConfigTokenBucketInTrickleMode(add_tokens, new_capacity, trickle_ms);

    // EXPECT_EQ(rg.bucket->
}
} // namespace DB::tests
