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

#include <Flash/Executor/ARU.h>
#include <gtest/gtest.h>

namespace DB::tests
{
class TestARU : public ::testing::Test
{
};

TEST_F(TestARU, base)
{
    auto to_aru = [](double mock_second) {
        // 1 vcore * mock_second
        double mock_cpu_time_s = mock_second * 1000'000'000;
        UInt64 mock_cpu_time_ns = 1 * mock_cpu_time_s;
        return toARU(mock_cpu_time_ns);
    };
    auto base_aru = to_aru(0.1);
    ASSERT_TRUE(base_aru > 0);
    ASSERT_EQ(base_aru, to_aru(0.2));
    ASSERT_EQ(base_aru, to_aru(0.3));
    ASSERT_EQ(base_aru, to_aru(0.4));
    ASSERT_EQ(base_aru, to_aru(0.5));
    ASSERT_EQ(base_aru, to_aru(0.6));
    ASSERT_EQ(base_aru, to_aru(0.7));
    ASSERT_EQ(base_aru, to_aru(0.8));
    ASSERT_EQ(base_aru, to_aru(0.9));
    ASSERT_EQ(base_aru, to_aru(1));
    ASSERT_TRUE(base_aru < to_aru(1.1));
    ASSERT_EQ(to_aru(1.9), to_aru(1.1));
}
} // namespace DB::tests
