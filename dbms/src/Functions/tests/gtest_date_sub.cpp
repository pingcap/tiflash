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

#include <Common/MyTime.h>
#include <Functions/FunctionFactory.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <vector>

namespace DB::tests
{
class Datesub : public DB::tests::FunctionTest
{
protected:
    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    static ColumnWithTypeAndName toStringVec(const std::vector<String> & v) { return createColumn<String>(v); }

    static ColumnWithTypeAndName toIntVec(const std::vector<Int64> & v) { return createColumn<Int64>(v); }

    static ColumnWithTypeAndName toFloatVec(const std::vector<Float64> & v) { return createColumn<Float64>(v); }

    static ColumnWithTypeAndName toConst(const int s) { return createConstColumn<int>(1, s); }

    static ColumnWithTypeAndName toConst(const Float64 s) { return createConstColumn<Float64>(1, s); }

    static ColumnWithTypeAndName toConst(const String & s) { return createConstColumn<String>(1, s); }
};

TEST_F(Datesub, dateSubStringIntUnitTest)
{
    ASSERT_COLUMN_EQ(
        toNullableVec(
            {"2020-12-13 12:12:12",
             "2020-12-13 12:12:12",
             "2020-12-13 12:12:12",
             "2020-12-13 12:12:12",
             "2012-12-13 12:12:00",
             "2020-12-13 12:12:09",
             "2020-12-13 12:12:09",
             "2012-12-13 12:09:00",
             "2020-12-13 12:12:00",
             "2020-12-13 12:12:00",
             "2012-12-13 12:00:00",
             "2020-12-13 12:09:00",
             "2020-12-13 12:09:00",
             "2012-12-13",
             "2020-12-13 12:00:00",
             "2012-12-13",
             "2020-12-13 09:00:00",
             "2020-12-13 09:00:00",
             "2012-12-10",
             "2020-12-13",
             "2020-12-13",
             "2020-12-10",
             "2020-12-13 12:12:12.123456",
             "2020-12-13",
             "2020-12-13 00:00:00"}),
        executeFunction(
            "subtractDays",
            toNullableVec(
                {"20201212121212",
                 "2020-12-12 12:12:12",
                 "201212121212",
                 "20-12-12 12-12-12",
                 "2012-12-12 12-12",
                 "20121212129",
                 "20-12-12 12-12-9",
                 "2012-12-12 12-9",
                 "2012121212",
                 "20-12-12 12-12",
                 "2012-12-12 12",
                 "201212129",
                 "20-12-12 12-9",
                 "20121212",
                 "20-12-12 12",
                 "2012-12-12",
                 "2012129",
                 "20-12-12 9",
                 "2012-12-9",
                 "201212",
                 "20-12-12",
                 "20-12-9",
                 "2020-12-12 12:12:12..123456",
                 "20-12-12.",
                 "201212.0"}),
            toConst(-1)));

    ASSERT_COLUMN_EQ(
        toNullableVec(
            {"2020-12-13 12:12:12",
             "2020-12-13 12:12:12",
             "2020-12-13 12:12:12",
             "2020-12-13 12:12:12",
             "2012-12-13 12:12:00",
             "2020-12-13 12:12:09",
             "2020-12-13 12:12:09",
             "2012-12-13 12:09:00",
             "2020-12-13 12:12:00",
             "2020-12-13 12:12:00",
             "2012-12-13 12:00:00",
             "2020-12-13 12:09:00",
             "2020-12-13 12:09:00",
             "2012-12-13",
             "2020-12-13 12:00:00",
             "2012-12-13",
             "2020-12-13 09:00:00",
             "2020-12-13 09:00:00",
             "2012-12-10",
             "2020-12-13",
             "2020-12-13",
             "2020-12-10",
             "2020-12-13 12:12:12.123456",
             "2020-12-13",
             "2020-12-13 00:00:00"}),
        executeFunction(
            "subtractDays",
            toStringVec(
                {"20201212121212",
                 "2020-12-12 12:12:12",
                 "201212121212",
                 "20-12-12 12-12-12",
                 "2012-12-12 12-12",
                 "20121212129",
                 "20-12-12 12-12-9",
                 "2012-12-12 12-9",
                 "2012121212",
                 "20-12-12 12-12",
                 "2012-12-12 12",
                 "201212129",
                 "20-12-12 12-9",
                 "20121212",
                 "20-12-12 12",
                 "2012-12-12",
                 "2012129",
                 "20-12-12 9",
                 "2012-12-9",
                 "201212",
                 "20-12-12",
                 "20-12-9",
                 "2020-12-12 12:12:12..123456",
                 "20-12-12.",
                 "201212.0"}),
            toConst(-1)));

    ASSERT_COLUMN_EQ(
        toNullableVec(
            {"2020-12-13 12:12:12.000100",
             "2020-12-13 12:12:12",
             "2020-12-13 12:12:12.000001",
             "2012-12-13 12:12:01",
             "2020-12-02 00:00:01",
             "2020-12-02 12:12:12"}),
        executeFunction(
            "subtractDays",
            toNullableVec(
                {"20201212121212.0001",
                 "20201212121212.0000001",
                 "20201212121212.000001",
                 "2012-12-12 12:12.01",
                 "20121.000001",
                 "20121.121212"}),
            toConst(-1)));

    ASSERT_COLUMN_EQ(
        toNullableVec({{}, {}, {}, {}, {}, {}, {}, {}}),
        executeFunction(
            "subtractDays",
            toNullableVec(
                {"20130229",
                 "20121312",
                 "20120012",
                 "20121200",
                 "20121232",
                 "20121212241212",
                 "20121212126012",
                 "20121212121260"}),
            toConst(-1)));
    ASSERT_COLUMN_EQ(
        toNullableVec({{}, {}, {}, {}, {}, {}, {}, {}}),
        executeFunction(
            "subtractDays",
            toNullableVec(
                {"2013-02-29",
                 "2012-13-12",
                 "2012-00-12",
                 "2012-12-00",
                 "2012-12-32",
                 "2012-12-12- 24:12:12",
                 "2012-12-12-12:60:12",
                 "2012-12-12 12:12:60"}),
            toConst(-1)));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-13", "2020-12-14", {}, "9992-01-01 12:12:00", "0007-11-26 12:12:12", {}}),
        executeFunction(
            "subtractDays",
            toNullableVec(
                {"20121212", "20201212", "20201212", "2012-12-12 12-12", "20201212121212", "2012-12-12 12-12"}),
            toIntVec({-1, -2, -2914289, -2914289, 735250, 735250})));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-19", "2012-12-19 12:12:12", "2012-12-19", "2012-12-19 12:12:12"}),
        executeFunction(
            "subtractWeeks",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            toConst(-1)));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2013-01-12", "2013-01-12 12:12:12", "2013-01-12", "2013-01-12 12:12:12"}),
        executeFunction(
            "subtractMonths",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            toConst(-1)));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2013-12-12", "2013-12-12 12:12:12", "2013-12-12", "2013-12-12 12:12:12"}),
        executeFunction(
            "subtractYears",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            toConst(-1)));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-12 01:00:00", "2012-12-12 13:12:12", "2012-12-12 01:00:00", "2012-12-12 13:12:12"}),
        executeFunction(
            "subtractHours",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            toConst(-1)));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-12 00:01:00", "2012-12-12 12:13:12", "2012-12-12 00:01:00", "2012-12-12 12:13:12"}),
        executeFunction(
            "subtractMinutes",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            toConst(-1)));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-12 00:00:01", "2012-12-12 12:12:13", "2012-12-12 00:00:01", "2012-12-12 12:12:13"}),
        executeFunction(
            "subtractSeconds",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            toConst(-1)));

    ASSERT_COLUMN_EQ(
        toNullableVec({{}, {}, {}, {}, {}, {}, {}, {}}),
        executeFunction(
            "subtractSeconds",
            toNullableVec(
                {"20130229",
                 "20121312",
                 "20120012",
                 "20121200",
                 "20121232",
                 "20121212241212",
                 "20121212126012",
                 "20121212121260"}),
            toConst(-1)));
}

TEST_F(Datesub, dateSubStringRealUnitTest)
{
    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-14", "2012-12-14 12:12:12", "2012-12-14", "2012-12-14 12:12:12"}),
        executeFunction(
            "subtractDays",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.6))));
    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-13", "2012-12-13 12:12:12", "2012-12-13", "2012-12-13 12:12:12"}),
        executeFunction(
            "subtractDays",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.4))));
    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-14", "2012-12-14 12:12:12", "2012-12-14", "2012-12-14 12:12:12"}),
        executeFunction(
            "subtractDays",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toFloatVec({-1.6, -1.6, -1.6, -1.6}))));
    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-13", "2012-12-13 12:12:12", "2012-12-13", "2012-12-13 12:12:12"}),
        executeFunction(
            "subtractDays",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toFloatVec({-1.4, -1.4, -1.4, -1.4}))));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-26", "2012-12-26 12:12:12", "2012-12-26", "2012-12-26 12:12:12"}),
        executeFunction(
            "subtractWeeks",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.6))));
    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-19", "2012-12-19 12:12:12", "2012-12-19", "2012-12-19 12:12:12"}),
        executeFunction(
            "subtractWeeks",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.4))));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2013-02-12", "2013-02-12 12:12:12", "2013-02-12", "2013-02-12 12:12:12"}),
        executeFunction(
            "subtractMonths",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.6))));
    ASSERT_COLUMN_EQ(
        toNullableVec({"2013-01-12", "2013-01-12 12:12:12", "2013-01-12", "2013-01-12 12:12:12"}),
        executeFunction(
            "subtractMonths",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.4))));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2014-12-12", "2014-12-12 12:12:12", "2014-12-12", "2014-12-12 12:12:12"}),
        executeFunction(
            "subtractYears",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.6))));
    ASSERT_COLUMN_EQ(
        toNullableVec({"2013-12-12", "2013-12-12 12:12:12", "2013-12-12", "2013-12-12 12:12:12"}),
        executeFunction(
            "subtractYears",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.4))));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-12 02:00:00", "2012-12-12 14:12:12", "2012-12-12 02:00:00", "2012-12-12 14:12:12"}),
        executeFunction(
            "subtractHours",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.6))));
    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-12 01:00:00", "2012-12-12 13:12:12", "2012-12-12 01:00:00", "2012-12-12 13:12:12"}),
        executeFunction(
            "subtractHours",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.4))));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-12 00:02:00", "2012-12-12 12:14:12", "2012-12-12 00:02:00", "2012-12-12 12:14:12"}),
        executeFunction(
            "subtractMinutes",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.6))));
    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-12 00:01:00", "2012-12-12 12:13:12", "2012-12-12 00:01:00", "2012-12-12 12:13:12"}),
        executeFunction(
            "subtractMinutes",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.4))));

    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-12 00:00:02", "2012-12-12 12:12:14", "2012-12-12 00:00:02", "2012-12-12 12:12:14"}),
        executeFunction(
            "subtractSeconds",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.6))));
    ASSERT_COLUMN_EQ(
        toNullableVec({"2012-12-12 00:00:01", "2012-12-12 12:12:13", "2012-12-12 00:00:01", "2012-12-12 12:12:13"}),
        executeFunction(
            "subtractSeconds",
            toNullableVec({"20121212", "20121212121212", "2012-12-12", "2012-12-12 12:12:12"}),
            executeFunction("tidbRound", toConst(-1.4))));
}

} // namespace DB::tests
