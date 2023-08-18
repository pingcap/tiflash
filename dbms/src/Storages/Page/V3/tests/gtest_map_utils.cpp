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

#include <Storages/Page/V3/MapUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/format.h>

#include <map>
namespace DB::PS::V3::tests
{
struct Empty
{
};

::testing::AssertionResult MapIterCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const std::map<int, Empty>::const_iterator lhs,
    const std::pair<int, Empty> rhs)
{
    if (lhs->first == rhs.first)
        return ::testing::AssertionSuccess();
    return ::testing::internal::EqFailure(
        lhs_expr,
        rhs_expr,
        fmt::format("{{{}, Empty}}", lhs->first),
        fmt::format("{{{}, Empty}}", rhs.first),
        false);
}
#define ASSERT_ITER_EQ(iter, val) ASSERT_PRED_FORMAT2(MapIterCompare, iter, val)

TEST(STDMapUtil, FindLessEqual)
{
    std::map<int, Empty> m0{};
    ASSERT_EQ(MapUtils::findLessEQ(m0, 1), m0.end());

    std::map<int, Empty> m1{{1, {}}, {2, {}}, {3, {}}, {6, {}}};
    ASSERT_EQ(MapUtils::findLessEQ(m1, 0), m1.end());
    ASSERT_ITER_EQ(MapUtils::findLessEQ(m1, 1), std::make_pair(1, Empty{}));
    ASSERT_ITER_EQ(MapUtils::findLessEQ(m1, 2), std::make_pair(2, Empty{}));
    for (int x = 3; x < 20; ++x)
    {
        if (x < 6)
            ASSERT_ITER_EQ(MapUtils::findLessEQ(m1, x), std::make_pair(3, Empty{}));
        else
            ASSERT_ITER_EQ(MapUtils::findLessEQ(m1, x), std::make_pair(6, Empty{}));
    }
}

TEST(STDMapUtil, FindLess)
{
    std::map<int, Empty> m0{};
    ASSERT_EQ(MapUtils::findLess(m0, 1), m0.end());

    std::map<int, Empty> m1{{1, {}}, {2, {}}, {3, {}}, {6, {}}};
    ASSERT_EQ(MapUtils::findLess(m1, 0), m1.end());
    ASSERT_EQ(MapUtils::findLess(m1, 1), m1.end());
    ASSERT_ITER_EQ(MapUtils::findLess(m1, 2), std::make_pair(1, Empty{}));
    ASSERT_ITER_EQ(MapUtils::findLess(m1, 3), std::make_pair(2, Empty{}));
    ASSERT_ITER_EQ(MapUtils::findLess(m1, 4), std::make_pair(3, Empty{}));
    for (int x = 5; x < 20; ++x)
    {
        if (x <= 6)
            ASSERT_ITER_EQ(MapUtils::findLess(m1, x), std::make_pair(3, Empty{}));
        else
            ASSERT_ITER_EQ(MapUtils::findLess(m1, x), std::make_pair(6, Empty{}));
    }
}

} // namespace DB::PS::V3::tests
