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

#include <Flash/Executor/toRU.h>
#include <gtest/gtest.h>

namespace DB::tests
{
class TestToRU : public ::testing::Test
{
};

TEST_F(TestToRU, base)
{
    ASSERT_EQ(0, cpuTimeToRU(0));

    auto base_ru = cpuTimeToRU(1);
    ASSERT_TRUE(base_ru > 0);

    for (size_t i = 1; i < 10; ++i)
    {
        auto ru = cpuTimeToRU(i);
        ASSERT_TRUE(ru >= base_ru);
        base_ru = ru;
    }

    constexpr auto ten_ms = 10'000'000;
    for (size_t i = 1; i < 20; ++i)
    {
        auto ru = cpuTimeToRU(i * ten_ms);
        ASSERT_TRUE(ru > base_ru);
        base_ru = ru;
    }
}
} // namespace DB::tests
