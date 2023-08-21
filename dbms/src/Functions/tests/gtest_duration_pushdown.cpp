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

#include <Columns/ColumnVector.h>
#include <Common/MyDuration.h>
#include <DataTypes/DataTypeMyDuration.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/registerFunctions.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <random>
#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class DurationPushDown : public DB::tests::FunctionTest
{
};

TEST_F(DurationPushDown, durationPushDownTest)
try
{
    ColumnWithTypeAndName result_col(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({-1,
                                                               0,
                                                               1,
                                                               {},
                                                               INT64_MAX,
                                                               INT64_MIN,
                                                               (838 * 3600 + 59 * 60 + 59) * 1000000000L,
                                                               -(838 * 3600 + 59 * 60 + 59) * 1000000000L})
            .column,
        makeNullable(std::make_shared<DataTypeMyDuration>(1)),
        "result");
    ASSERT_COLUMN_EQ(
        result_col,
        executeFunction(
            "FunctionConvertDurationFromNanos",
            {createColumn<Nullable<Int64>>(
                 {-1,
                  0,
                  1,
                  {},
                  INT64_MAX,
                  INT64_MIN,
                  (838 * 3600 + 59 * 60 + 59) * 1000000000L,
                  -(838 * 3600 + 59 * 60 + 59) * 1000000000L}),
             createConstColumn<Int64>(8, 1)},
            nullptr,
            true));

    ColumnWithTypeAndName result_col2(
        createConstColumn<DataTypeMyDuration::FieldType>(3, 3).column,
        std::make_shared<DataTypeMyDuration>(2),
        "result2");
    ASSERT_COLUMN_EQ(
        result_col2,
        executeFunction(
            "FunctionConvertDurationFromNanos",
            {createConstColumn<Int64>(3, 3), createConstColumn<Int64>(3, 2)},
            nullptr,
            true));
}
CATCH

TEST_F(DurationPushDown, hourPushDownTest)
try
{
    ColumnWithTypeAndName input(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({(838 * 3600 + 59 * 60 + 59) * 1000000000L + 999999000L,
                                                               -(838 * 3600 + 59 * 60 + 59) * 1000000000L - 123456000L,
                                                               0,
                                                               (1 * 3600 + 2 * 60 + 3) * 1000000000L + 4000L})
            .column,
        makeNullable(std::make_shared<DataTypeMyDuration>(6)),
        "input");
    auto hour_output = createColumn<Nullable<Int64>>({838, 838, 0, 1});
    auto minute_output = createColumn<Nullable<Int64>>({59, 59, 0, 2});
    auto second_output = createColumn<Nullable<Int64>>({59, 59, 0, 3});
    auto microsecond_output = createColumn<Nullable<Int64>>({999999, 123456, 0, 4});
    ASSERT_COLUMN_EQ(hour_output, executeFunction("hour", input));
    ASSERT_COLUMN_EQ(minute_output, executeFunction("minute", input));
    ASSERT_COLUMN_EQ(second_output, executeFunction("second", input));
    ASSERT_COLUMN_EQ(microsecond_output, executeFunction("microSecond", input));

    // Test Overflow
    ColumnWithTypeAndName input2(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>(
            {(838 * 3600 + 59 * 60 + 59) * 1000000000L + 999999000L + 1000L})
            .column,
        makeNullable(std::make_shared<DataTypeMyDuration>(6)),
        "result");
    try
    {
        auto result = executeFunction("hour", input2);
        FAIL() << "Expected overflow";
    }
    catch (DB::Exception & e)
    {
        ASSERT_EQ(e.message(), std::string("nanos must >= -3020399999999000 and <= 3020399999999000"));
    }
    catch (...)
    {
        FAIL() << "Expected overflow";
    };

    ColumnWithTypeAndName input3(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>(
            {-(838 * 3600 + 59 * 60 + 59) * 1000000000L - 999999000L - 1000L})
            .column,
        makeNullable(std::make_shared<DataTypeMyDuration>(6)),
        "result");
    try
    {
        auto result = executeFunction("hour", input3);
        FAIL() << "Expected overflow";
    }
    catch (DB::Exception & e)
    {
        ASSERT_EQ(e.message(), std::string("nanos must >= -3020399999999000 and <= 3020399999999000"));
    }
    catch (...)
    {
        FAIL() << "Expected overflow";
    };

    // Random Test
    constexpr int rowNum = 1000;
    auto dur_column = ColumnVector<Int64>::create();
    auto & dur_data = dur_column->getData();
    auto hour_column = ColumnVector<Int64>::create();
    auto & hour_data = hour_column->getData();
    auto minute_column = ColumnVector<Int64>::create();
    auto & minute_data = minute_column->getData();
    auto second_column = ColumnVector<Int64>::create();
    auto & second_data = second_column->getData();
    auto microSecond_column = ColumnVector<Int64>::create();
    auto & microSecond_data = microSecond_column->getData();
    dur_data.resize(rowNum);
    hour_data.resize(rowNum);
    minute_data.resize(rowNum);
    second_data.resize(rowNum);
    microSecond_data.resize(rowNum);

    std::random_device rd;
    std::default_random_engine gen = std::default_random_engine(rd());
    std::uniform_int_distribution<int> sign_dis(0, 1), hour_dis(0, 838), minute_dis(0, 59), second_dis(0, 59),
        microSecond_dis(0, 999999);
    for (int i = 0; i < rowNum; i++)
    {
        auto sign = (sign_dis(gen) == 0) ? 1 : -1;
        auto hour = hour_dis(gen);
        auto minute = minute_dis(gen);
        auto second = second_dis(gen);
        auto microSecond = microSecond_dis(gen);
        dur_data[i] = sign * ((hour * 3600 + minute * 60 + second) * 1000000000L + microSecond * 1000L);
        hour_data[i] = hour;
        minute_data[i] = minute;
        second_data[i] = second;
        microSecond_data[i] = microSecond;
    }

    ColumnWithTypeAndName input4(std::move(dur_column), std::make_shared<DataTypeMyDuration>(6), "duration");
    ColumnWithTypeAndName hour_out(std::move(hour_column), std::make_shared<DataTypeInt64>(), "hour");
    ColumnWithTypeAndName minute_out(std::move(minute_column), std::make_shared<DataTypeInt64>(), "minute");
    ColumnWithTypeAndName second_out(std::move(second_column), std::make_shared<DataTypeInt64>(), "second");
    ColumnWithTypeAndName microSecond_out(
        std::move(microSecond_column),
        std::make_shared<DataTypeInt64>(),
        "microSecond");
    ASSERT_COLUMN_EQ(hour_out, executeFunction("hour", input4));
    ASSERT_COLUMN_EQ(minute_out, executeFunction("minute", input4));
    ASSERT_COLUMN_EQ(second_out, executeFunction("second", input4));
    ASSERT_COLUMN_EQ(microSecond_out, executeFunction("microSecond", input4));
}
CATCH

TEST_F(DurationPushDown, timeToSecPushDownTest)
try
{
    ColumnWithTypeAndName input(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>({(838 * 3600 + 59 * 60 + 59) * 1000000000L + 999999000L,
                                                               -(838 * 3600 + 59 * 60 + 59) * 1000000000L - 123456000L,
                                                               0,
                                                               (1 * 3600 + 2 * 60 + 3) * 1000000000L + 4000L})
            .column,
        makeNullable(std::make_shared<DataTypeMyDuration>(6)),
        "input");
    auto second_output = createColumn<Nullable<Int64>>({3020399, -3020399, 0, 3723});
    ASSERT_COLUMN_EQ(second_output, executeFunction("tidbTimeToSec", input));

    // Test Overflow
    ColumnWithTypeAndName input2(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>(
            {(838 * 3600 + 59 * 60 + 59) * 1000000000L + 999999000L + 1000L})
            .column,
        makeNullable(std::make_shared<DataTypeMyDuration>(6)),
        "result");
    try
    {
        auto result = executeFunction("tidbTimeToSec", input2);
        FAIL() << "Expected overflow";
    }
    catch (DB::Exception & e)
    {
        ASSERT_EQ(e.message(), std::string("nanos must >= -3020399999999000 and <= 3020399999999000"));
    }
    catch (...)
    {
        FAIL() << "Expected overflow";
    };

    ColumnWithTypeAndName input3(
        createColumn<Nullable<DataTypeMyDuration::FieldType>>(
            {-(838 * 3600 + 59 * 60 + 59) * 1000000000L - 999999000L - 1000L})
            .column,
        makeNullable(std::make_shared<DataTypeMyDuration>(6)),
        "result");
    try
    {
        auto result = executeFunction("tidbTimeToSec", input3);
        FAIL() << "Expected overflow";
    }
    catch (DB::Exception & e)
    {
        ASSERT_EQ(e.message(), std::string("nanos must >= -3020399999999000 and <= 3020399999999000"));
    }
    catch (...)
    {
        FAIL() << "Expected overflow";
    };

    // Random Test
    constexpr int rowNum = 1000;
    auto dur_column = ColumnVector<Int64>::create();
    auto & dur_data = dur_column->getData();
    auto second_column = ColumnVector<Int64>::create();
    auto & second_data = second_column->getData();
    dur_data.resize(rowNum);
    second_data.resize(rowNum);

    std::random_device rd;
    std::default_random_engine gen = std::default_random_engine(rd());
    std::uniform_int_distribution<int> sign_dis(0, 1), hour_dis(0, 838), minute_dis(0, 59), second_dis(0, 59),
        microSecond_dis(0, 999999);
    for (int i = 0; i < rowNum; ++i)
    {
        auto sign = (sign_dis(gen) == 0) ? 1 : -1;
        auto hour = hour_dis(gen);
        auto minute = minute_dis(gen);
        auto second = second_dis(gen);
        auto microSecond = microSecond_dis(gen);
        dur_data[i] = sign * ((hour * 3600 + minute * 60 + second) * 1000000000L + microSecond * 1000L);
        second_data[i] = sign * (hour * 3600 + minute * 60 + second);
    }

    ColumnWithTypeAndName input4(std::move(dur_column), std::make_shared<DataTypeMyDuration>(6), "duration");
    ColumnWithTypeAndName second_out(std::move(second_column), std::make_shared<DataTypeInt64>(), "time_to_sec");
    ASSERT_COLUMN_EQ(second_out, executeFunction("tidbTimeToSec", input4));
}
CATCH
} // namespace tests
} // namespace DB