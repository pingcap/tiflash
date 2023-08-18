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

#include <Columns/ColumnNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/ExecutorTestUtils.h>

#include <Flash/Coprocessor/JoinInterpreterHelper.cpp>
#include <ext/enumerate.h>
#include <tuple>

namespace DB
{
namespace tests
{
class JoinKindAndBuildIndexTestRunner : public testing::Test
{
};

TEST(JoinKindAndBuildIndexTestRunner, TestNullAwareJoins)
{
    auto result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiSemiJoin, 1, true, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::NullAware_Anti && result.second == 1);

    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterSemiJoin, 1, true, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::NullAware_LeftOuterSemi && result.second == 1);

    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 1, true, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::NullAware_LeftOuterAnti && result.second == 1);

    /// NullAware join, expect join keys > 0
    try
    {
        JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiSemiJoin, 1, true, 0);
        ASSERT_TRUE(false);
    }
    catch (Exception & e)
    {
    }

    /// NullAware join, expect right table as build table
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiSemiJoin, 0, true, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::NullAware_Anti && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterSemiJoin, 0, true, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::NullAware_LeftOuterSemi && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 0, true, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::NullAware_LeftOuterAnti && result.second == 0);
}

TEST(JoinKindAndBuildIndexTestRunner, TestCrossJoins)
{
    /// Cross Inner Join, both sides supported
    auto result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeInnerJoin, 0, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeInnerJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross && result.second == 1);

    /// Cross LeftOuterJoin, uses right table as build side only
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuter && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterJoin, 0, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuter && result.second == 1);

    /// Cross RightOuterJoin, uses left table as build side only
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeRightOuterJoin, 0, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuter && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeRightOuterJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuter && result.second == 0);

    /// Cross Semi/Anti, uses right table as build side only
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeSemiJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeSemiJoin, 0, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiSemiJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_Anti && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiSemiJoin, 0, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_Anti && result.second == 0);

    /// Cross LeftOuter Semi/Anti, uses right table as build side only
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterSemiJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuterSemi && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterSemiJoin, 0, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuterSemi && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 1, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuterAnti && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 0, false, 0);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Cross_LeftOuterAnti && result.second == 0);
}

TEST(JoinKindAndBuildIndexTestRunner, TestEqualJoins)
{
    /// Inner Join
    auto result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeInnerJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Inner && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeInnerJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Inner && result.second == 1);

    /// LeftOuterJoin
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::LeftOuter && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::RightOuter && result.second == 0);

    /// RightOuterJoin
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeRightOuterJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::LeftOuter && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeRightOuterJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::RightOuter && result.second == 1);

    /// Semi/Anti
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeSemiJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Inner && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeSemiJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::RightSemi && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiSemiJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::Anti && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiSemiJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::RightAnti && result.second == 0);

    /// LeftOuter Semi/Anti
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterSemiJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::LeftOuterSemi && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeLeftOuterSemiJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::LeftOuterSemi && result.second == 0);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 1, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::LeftOuterAnti && result.second == 1);
    result = JoinInterpreterHelper::getJoinKindAndBuildSideIndex(tipb::JoinType::TypeAntiLeftOuterSemiJoin, 0, false, 1);
    ASSERT_TRUE(result.first == ASTTableJoin::Kind::LeftOuterAnti && result.second == 0);
}

} // namespace tests
} // namespace DB
