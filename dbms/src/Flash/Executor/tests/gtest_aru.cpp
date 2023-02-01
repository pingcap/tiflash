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
    constexpr auto one_second = 1000'000'000;
    auto base_aru = toARU(1);
    ASSERT_TRUE(base_aru > 0);
    ASSERT_EQ(base_aru, toARU(0.1 * one_second));
    ASSERT_EQ(base_aru, toARU(0.2 * one_second));
    ASSERT_EQ(base_aru, toARU(0.3 * one_second));
    ASSERT_EQ(base_aru, toARU(0.4 * one_second));
    ASSERT_EQ(base_aru, toARU(0.5 * one_second));
    ASSERT_EQ(base_aru, toARU(0.6 * one_second));
    ASSERT_EQ(base_aru, toARU(0.7 * one_second));
    ASSERT_EQ(base_aru, toARU(0.8 * one_second));
    ASSERT_EQ(base_aru, toARU(0.9 * one_second));
    ASSERT_EQ(base_aru, toARU(1 * one_second));

    ASSERT_TRUE(base_aru < toARU(1.1 * one_second));
    ASSERT_EQ(toARU(1.9 * one_second), toARU(1.1 * one_second));
}
} // namespace DB::tests
