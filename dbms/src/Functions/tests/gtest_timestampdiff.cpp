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

#include <optional>

#include "DataTypes/DataTypeMyDuration.h"
#include "DataTypes/DataTypeNullable.h"
#include "TestUtils/FunctionTestUtils.h"
#include "common/types.h"
#include "gtest/gtest.h"

namespace DB::tests
{
namespace
{
class TestTimestampDiff : public DB::tests::FunctionTest
{
};

const std::string func_name = "tidbTimestampDiff";

#define ASSERT_TIMESTAMP_DIFF(unit, t1, t2, result) \
    ASSERT_COLUMN_EQ(result, executeFunction(func_name, {createConstColumn<String>((t1).column->size(), unit), t1, t2}))


TEST_F(TestTimestampDiff, nullOnly)
try
{
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(1, std::nullopt),
        executeFunction(
            func_name,
            {createConstColumn<String>(1, "year"),
             createOnlyNullColumnConst(1),
             createDateTimeColumn({{{2020, 1, 1, 0, 0, 0, 0}}}, 6)}));
    ASSERT_COLUMN_EQ(
        createConstColumn<Nullable<Int64>>(1, std::nullopt),
        executeFunction(
            func_name,
            {createConstColumn<String>(1, "year"),
             createDateTimeColumn({{{2020, 1, 1, 0, 0, 0, 0}}}, 6),
             createOnlyNullColumnConst(1)}));
}
CATCH

TEST_F(TestTimestampDiff, constVector)
try
{
    ASSERT_TIMESTAMP_DIFF(
        "year",
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 0, 0, 0, 0}}, 6),
        createDateTimeColumn({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "year",
        createDateTimeColumn({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 0, 0, 0, 0}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "quarter",
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 0, 0, 0, 0}}, 6),
        createDateTimeColumn({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "quarter",
        createDateTimeColumn({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 0, 0, 0, 0}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "month",
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 0, 0, 0, 0}}, 6),
        createDateTimeColumn({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "month",
        createDateTimeColumn({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 0, 0, 0, 0}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "week",
        createDateTimeColumnConst<false>(2, {{2021, 2, 28, 0, 0, 0, 0}}, 6),
        createDateTimeColumn({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "week",
        createDateTimeColumn({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
        createDateTimeColumnConst<false>(2, {{2021, 2, 28, 0, 0, 0, 0}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "day",
        createDateTimeColumnConst<false>(2, {{2021, 2, 28, 0, 0, 0, 0}}, 6),
        createDateTimeColumn({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "day",
        createDateTimeColumn({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
        createDateTimeColumnConst<false>(2, {{2021, 2, 28, 0, 0, 0, 0}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "hour",
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 23, 0, 0, 0}}, 6),
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "hour",
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 23, 0, 0, 0}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "minute",
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 23, 59, 0, 0}}, 6),
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "minute",
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 23, 59, 0, 0}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "second",
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 23, 59, 59, 0}}, 6),
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "second",
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 23, 59, 59, 0}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "microsecond",
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 23, 59, 59, 999999}}, 6),
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "microsecond",
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
        createDateTimeColumnConst<false>(2, {{2021, 1, 1, 23, 59, 59, 999999}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
}
CATCH

TEST_F(TestTimestampDiff, vectorVector)
try
{
    ASSERT_TIMESTAMP_DIFF(
        "year",
        createDateTimeColumn({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "year",
        createDateTimeColumn({{{2022, 1, 1, 0, 0, 0, 0}}, {{2020, 1, 1, 0, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "quarter",
        createDateTimeColumn({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "quarter",
        createDateTimeColumn({{{2021, 4, 1, 0, 0, 0, 0}}, {{2020, 10, 1, 0, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "month",
        createDateTimeColumn({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "month",
        createDateTimeColumn({{{2021, 2, 1, 0, 0, 0, 0}}, {{2020, 12, 1, 0, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 1, 1, 0, 0, 0, 0}}, {{2021, 1, 1, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "week",
        createDateTimeColumn({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "week",
        createDateTimeColumn({{{2021, 3, 7, 0, 0, 0, 0}}, {{2021, 2, 21, 0, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "day",
        createDateTimeColumn({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "day",
        createDateTimeColumn({{{2021, 3, 1, 0, 0, 0, 0}}, {{2021, 2, 27, 0, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 2, 28, 0, 0, 0, 0}}, {{2021, 2, 28, 0, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "hour",
        createDateTimeColumn({{{2021, 1, 1, 23, 0, 0, 0}}, {{2021, 1, 1, 23, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "hour",
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 22, 0, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 1, 1, 23, 0, 0, 0}}, {{2021, 1, 1, 23, 0, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "minute",
        createDateTimeColumn({{{2021, 1, 1, 23, 59, 0, 0}}, {{2021, 1, 1, 23, 59, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "minute",
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 58, 0, 0}}}, 6),
        createDateTimeColumn({{{2021, 1, 1, 23, 59, 0, 0}}, {{2021, 1, 1, 23, 59, 0, 0}}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "second",
        createDateTimeColumn({{{2021, 1, 1, 23, 59, 59, 0}}, {{2021, 1, 1, 23, 59, 59, 0}}}, 6),
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "second",
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 58, 0}}}, 6),
        createDateTimeColumn({{{2021, 1, 1, 23, 59, 59, 0}}, {{2021, 1, 1, 23, 59, 59, 0}}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
    ASSERT_TIMESTAMP_DIFF(
        "microsecond",
        createDateTimeColumn({{{2021, 1, 1, 23, 59, 59, 999999}}, {{2021, 1, 1, 23, 59, 59, 999999}}}, 6),
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
        createColumn<Nullable<Int64>>({1, -1}));
    ASSERT_TIMESTAMP_DIFF(
        "microsecond",
        createDateTimeColumn({{{2021, 1, 2, 0, 0, 0, 0}}, {{2021, 1, 1, 23, 59, 59, 999998}}}, 6),
        createDateTimeColumn({{{2021, 1, 1, 23, 59, 59, 999999}}, {{2021, 1, 1, 23, 59, 59, 999999}}}, 6),
        createColumn<Nullable<Int64>>({-1, 1}));
}
CATCH

} // namespace
} // namespace DB::tests
