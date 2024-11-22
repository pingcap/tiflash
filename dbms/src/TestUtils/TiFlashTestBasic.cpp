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

#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>
#include <gtest/internal/gtest-internal.h>

namespace DB::tests
{
::testing::AssertionResult DataTypeCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const DataTypePtr & lhs,
    const DataTypePtr & rhs)
{
    if (lhs->equals(*rhs))
        return ::testing::AssertionSuccess();
    else
        return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs->getName(), rhs->getName(), false);
}

::testing::AssertionResult fieldCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const Field & lhs,
    const Field & rhs)
{
    if (lhs == rhs)
        return ::testing::AssertionSuccess();
    return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs.toString(), rhs.toString(), false);
}

::testing::AssertionResult StringViewCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    std::string_view lhs,
    std::string_view rhs)
{
    if (strncmp(lhs.data(), rhs.data(), lhs.size()) == 0)
        return ::testing::AssertionSuccess();
    return ::testing::internal::EqFailure(lhs_expr, rhs_expr, String(lhs), String(rhs), false);
}
} // namespace DB::tests
