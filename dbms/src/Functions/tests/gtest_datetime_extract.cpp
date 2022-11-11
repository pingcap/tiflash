// Copyright 2022 PingCAP, Ltd.
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

#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Functions/FunctionsDateTime.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
// TODO: rewrite using executeFunction()
class TestDateTimeExtract : public DB::tests::FunctionTest
{
};

TEST_F(TestDateTimeExtract, ExtractFromString)
try
{
    std::vector<String> units{
        "day_microsecond",
        "day_second",
        "day_minute",
        "day_hour",
    };
    String datetime_value{"2021/1/29 12:34:56.123456"};
    std::vector<Int64> results{29123456123456, 29123456, 291234, 2912};

    for (size_t i = 0; i < units.size(); ++i)
    {
        const auto & unit = units[i];
        const auto & result = results[i];
        // nullable/non-null string
        ASSERT_COLUMN_EQ(toNullableVec<Int64>({result}), executeFunction("extractMyDateTimeFromString", createConstColumn<String>(1, {unit}), toNullableVec<String>({datetime_value})));
        ASSERT_COLUMN_EQ(toVec<Int64>({result}), executeFunction("extractMyDateTimeFromString", createConstColumn<String>(1, {unit}), toVec<String>({datetime_value})));
        // const string
        ASSERT_COLUMN_EQ(createConstColumn<Int64>(1, result), executeFunction("extractMyDateTimeFromString", createConstColumn<String>(1, {unit}), createConstColumn<String>(1, {datetime_value})));
    }

    datetime_value = "12:34:56.123456";
    results = {123456123456, 123456, 1234, 12};

    for (size_t i = 0; i < units.size(); ++i)
    {
        const auto & unit = units[i];
        const auto & result = results[i];
        // nullable/non-null string
        ASSERT_COLUMN_EQ(toNullableVec<Int64>({result}), executeFunction("extractMyDateTimeFromString", createConstColumn<String>(1, {unit}), toNullableVec<String>({datetime_value})));
        ASSERT_COLUMN_EQ(toVec<Int64>({result}), executeFunction("extractMyDateTimeFromString", createConstColumn<String>(1, {unit}), toVec<String>({datetime_value})));
        // const string
        ASSERT_COLUMN_EQ(createConstColumn<Int64>(1, result), executeFunction("extractMyDateTimeFromString", createConstColumn<String>(1, {unit}), createConstColumn<String>(1, {datetime_value})));
    }
}
CATCH

TEST_F(TestDateTimeExtract, ExtractFromMyDateTime)
try
{
    std::vector<String> units{
        "year",
        "quarter",
        "month",
        "week",
        "day",
        "day_microsecond",
        "day_second",
        "day_minute",
        "day_hour",
        "year_month",
    };
    MyDateTime datetime_value(2021, 1, 29, 12, 34, 56, 123456);
    std::vector<Int64> results{2021, 1, 1, 4, 29, 29123456123456, 29123456, 291234, 2912, 202101};

    for (size_t i = 0; i < units.size(); ++i)
    {
        const auto & unit = units[i];
        const auto & result = results[i];
        // nullable/non-null datetime
        ASSERT_COLUMN_EQ(toNullableVec<Int64>({result}), executeFunction("extractMyDateTime", createConstColumn<String>(1, {unit}), createDateTimeColumn({datetime_value}, 6)));
        ASSERT_COLUMN_EQ(toVec<Int64>({result}), executeFunction("extractMyDateTime", createConstColumn<String>(1, {unit}), createDateTimeColumn<false>({datetime_value}, 6)));
        // const datetime
        ASSERT_COLUMN_EQ(createConstColumn<Int64>(1, result), executeFunction("extractMyDateTime", createConstColumn<String>(1, {unit}), createDateTimeColumnConst(1, {datetime_value}, 6)));
        // null
        ASSERT_COLUMN_EQ(toNullableVec<Int64>({std::nullopt}), executeFunction("extractMyDateTime", createConstColumn<String>(1, {unit}), createDateTimeColumn({std::nullopt}, 6)));
    }
}
CATCH

} // namespace tests
} // namespace DB
