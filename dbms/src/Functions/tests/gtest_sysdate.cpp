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
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
static const String int_to_2_width_string[]
    = {"00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16",
       "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
       "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
       "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67",
       "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "80", "81", "82", "83", "84",
       "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96", "97", "98", "99"};

class Sysdate : public DB::tests::FunctionTest
{
protected:
    static constexpr int seconds_in_one_day = 60 * 60 * 24;

    void setTimezoneByOffset(Int64 offset)
    {
        auto & timezone_info = context->getTimezoneInfo();
        timezone_info.is_name_based = false;
        timezone_info.timezone_offset = offset * 3600;
        timezone_info.timezone = &DateLUT::instance("UTC");
        timezone_info.timezone_name = "";
        timezone_info.is_utc_timezone = offset == 0;
    }

    void setTimezoneByName(String name)
    {
        auto & timezone_info = context->getTimezoneInfo();
        timezone_info.is_name_based = true;
        timezone_info.timezone_offset = 0;
        timezone_info.timezone = &DateLUT::instance(name);
        timezone_info.timezone_name = timezone_info.timezone->getTimeZone();
        timezone_info.is_utc_timezone = timezone_info.timezone_name == "UTC";
    }

    void ASSERT_FSP(UInt32 fsp, const MyDateTime & date_time) const
    {
        UInt32 origin_micro_second = date_time.micro_second;
        if (fsp == 0)
        {
            ASSERT_EQ(0, origin_micro_second);
            return;
        }
        String micro_second_str;
        micro_second_str.append(int_to_2_width_string[origin_micro_second / 10000])
            .append(int_to_2_width_string[origin_micro_second % 10000 / 100])
            .append(int_to_2_width_string[origin_micro_second % 100]);
        micro_second_str.resize(fsp);
        UInt32 new_micro_second = std::stoul(micro_second_str) * std::pow(10, 6 - fsp);
        ASSERT_EQ(origin_micro_second, new_micro_second);
    }

    void ASSERT_CHECK_SYSDATE(const MyDateTime base_date_time, ColumnWithTypeAndName actual_col, int offset)
    {
        UInt64 base_packed = base_date_time.toPackedUInt();
        UInt64 packed = actual_col.column.get()->get64(0);
        MyDateTime date_time = MyDateTime(packed);
        ASSERT_LE(base_packed, packed);
        ASSERT_EQ(
            ((date_time.yearDay() - base_date_time.yearDay()) * 24 + (date_time.hour - base_date_time.hour)),
            offset);
    }
};

TEST_F(Sysdate, sysdate_unit_Test)
{
    UInt32 fsp = 3;
    auto data_column = createColumn<String>(std::vector<String>{"test"});
    auto fsp_column = createConstColumn<Int64>(1, fsp);
    ColumnNumbers with_fsp_arguments = {0};
    ColumnNumbers without_fsp_arguments = {};

    setTimezoneByOffset(0);
    auto without_fsp_col = executeFunction("sysDateWithoutFsp", without_fsp_arguments, data_column);
    UInt64 without_fsp_packed = without_fsp_col.column.get()->get64(0);
    MyDateTime without_fsp_date_time(without_fsp_packed);


    auto with_fsp_col = executeFunction("sysDateWithFsp", with_fsp_arguments, fsp_column, data_column);
    UInt64 with_fsp_packed = with_fsp_col.column.get()->get64(0);
    MyDateTime with_fsp_date_time(with_fsp_packed);

    auto date_time = MyDateTime::getSystemDateTimeByTimezone(context->getTimezoneInfo(), fsp);

    auto with_fsp_second_diff = (date_time.yearDay() - with_fsp_date_time.yearDay()) * seconds_in_one_day
        + (date_time.hour - with_fsp_date_time.hour) * 60 * 60 + (date_time.minute - with_fsp_date_time.minute) * 60
        + (date_time.second - with_fsp_date_time.second);
    auto with_out_fsp_second_diff = (date_time.yearDay() - without_fsp_date_time.yearDay()) * seconds_in_one_day
        + (date_time.hour - without_fsp_date_time.hour) * 60 * 60
        + (date_time.minute - without_fsp_date_time.minute) * 60 + (date_time.second - without_fsp_date_time.second);

    ASSERT_LE(with_fsp_second_diff, 10);
    ASSERT_LE(with_out_fsp_second_diff, 10);
}

TEST_F(Sysdate, fsp_unit_Test)
{
    std::vector<time_t> expect_seconds{13, 13, 12, 12, 12, 12, 12};
    std::vector<UInt32> expect_micro_seconds{0, 0, 990000, 988000, 987700, 987650, 987654};

    for (int fsp = 0; fsp <= 6; ++fsp)
    {
        time_t second = 12;
        UInt64 nano_second = 987654321;
        auto second_and_micro_second = roundTimeByFsp(second, nano_second, fsp);
        second = second_and_micro_second.first;
        UInt32 micro_second = second_and_micro_second.second;
        ASSERT_EQ(expect_seconds.at(fsp), second);
        ASSERT_EQ(expect_micro_seconds.at(fsp), micro_second);
    }

    auto data_column = createColumn<String>(std::vector<String>{"test"});
    ColumnNumbers with_fsp_arguments = {0};

    setTimezoneByOffset(0);

    for (int fsp = 0; fsp <= 6; ++fsp)
    {
        auto fsp_column = createConstColumn<Int64>(1, fsp);
        ColumnWithTypeAndName with_fsp_col
            = executeFunction("sysDateWithFsp", with_fsp_arguments, fsp_column, data_column);
        UInt64 with_fsp_packed = with_fsp_col.column.get()->get64(0);
        MyDateTime with_fsp_date_time(with_fsp_packed);
        ASSERT_FSP(fsp, with_fsp_date_time);
    }
}

TEST_F(Sysdate, timezone_unit_Test)
{
    int fsp = 3;
    setTimezoneByOffset(0);
    // base timezone is UTC +0:00
    auto with_fsp_date_time = MyDateTime::getSystemDateTimeByTimezone(context->getTimezoneInfo(), fsp);
    auto without_fsp_date_time = MyDateTime::getSystemDateTimeByTimezone(context->getTimezoneInfo(), 0);
    auto data_column = createColumn<String>(std::vector<String>{"test"});
    auto fsp_column = createConstColumn<Int64>(1, fsp);
    ColumnNumbers with_fsp_arguments = {0};
    ColumnNumbers without_fsp_arguments = {};

    // If the time zone has daylight saving time, the UTC offset of the time zone may be different on different dates, resulting in unstable testing
    std::vector<String> timezone_names{
        "Atlantic/Reykjavik",
        "Africa/Bangui",
        "Africa/Maputo",
        "Europe/Moscow",
        "Asia/Baku",
        "Indian/Maldives",
        "Asia/Dhaka",
        "Asia/Bangkok",
        "Asia/Shanghai"};

    for (int i = 0; i < 8; ++i)
    {
        // test timezone name
        setTimezoneByName(timezone_names.at(i));
        ASSERT_CHECK_SYSDATE(
            with_fsp_date_time,
            executeFunction("sysDateWithFsp", with_fsp_arguments, fsp_column, data_column),
            i);
        ASSERT_CHECK_SYSDATE(
            without_fsp_date_time,
            executeFunction("sysDateWithoutFsp", without_fsp_arguments, data_column),
            i);

        // test timezone offset
        setTimezoneByOffset(i);
        ASSERT_CHECK_SYSDATE(
            with_fsp_date_time,
            executeFunction("sysDateWithFsp", with_fsp_arguments, fsp_column, data_column),
            i);
        ASSERT_CHECK_SYSDATE(
            without_fsp_date_time,
            executeFunction("sysDateWithoutFsp", without_fsp_arguments, data_column),
            i);
    }
}
} // namespace DB::tests
