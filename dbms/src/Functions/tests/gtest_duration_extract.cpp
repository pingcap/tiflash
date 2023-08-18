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

#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Functions/FunctionsDuration.h>
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
class TestDurationExtract : public DB::tests::FunctionTest
{
};

TEST_F(TestDurationExtract, ExtractFromMyDuration)
try
{
    std::vector<String> units{
        "hour",
        "minute",
        "second",
        "microsecond",
        "second_microsecond",
        "minute_microsecond",
        "minute_second",
        "hour_microsecond",
        "hour_second",
        "hour_minute",
        "day_microsecond",
        "day_second",
        "day_minute",
        "day_hour",
    };
    MyDuration duration_value(1, 838, 34, 56, 123456, 6);
    std::vector<Int64> results{
        838,
        34,
        56,
        123456,
        56123456,
        3456123456,
        3456,
        8383456123456,
        8383456,
        83834,
        8383456123456,
        8383456,
        83834,
        838};

    for (size_t i = 0; i < units.size(); ++i)
    {
        const auto & unit = units[i];
        const auto & result = results[i];
        // nullable/non-null duration
        ASSERT_COLUMN_EQ(
            toNullableVec<Int64>({result}),
            executeFunction(
                "extractMyDuration",
                createConstColumn<String>(1, {unit}),
                createDurationColumn({duration_value}, 6)));
        ASSERT_COLUMN_EQ(
            toVec<Int64>({result}),
            executeFunction(
                "extractMyDuration",
                createConstColumn<String>(1, {unit}),
                createDurationColumn<false>({duration_value}, 6)));
        // const duration
        ASSERT_COLUMN_EQ(
            createConstColumn<Int64>(1, result),
            executeFunction(
                "extractMyDuration",
                createConstColumn<String>(1, {unit}),
                createDurationColumnConst(1, {duration_value}, 6)));
        // null
        ASSERT_COLUMN_EQ(
            toNullableVec<Int64>({std::nullopt}),
            executeFunction(
                "extractMyDuration",
                createConstColumn<String>(1, {unit}),
                createDurationColumn({std::nullopt}, 6)));
    }
}
CATCH

} // namespace tests
} // namespace DB
