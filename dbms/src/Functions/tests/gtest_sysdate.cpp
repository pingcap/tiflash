#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
#define INT_TO_STRING                                                                                                                   \
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", \
        "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "52",   \
        "53", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73",   \
        "74", "75", "76", "77", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94",   \
        "95", "96", "97", "98", "99",

static const String int_to_2_width_string[] = {"00", "01", "02", "03", "04", "05", "06", "07", "08", "09", INT_TO_STRING};

class Sysdate : public DB::tests::FunctionTest
{
protected:
    const int SECONDS_IN_ONE_DAY = 60 * 60 * 24;

    ColumnWithTypeAndName executeFunctionWithTimezone(const String & func_name, const ColumnNumbers & arguments, const ColumnsWithTypeAndName & columns, const Int64 offset)
    {
        Context contest = TiFlashTestEnv::getContext();
        auto & timezone_info = context.getTimezoneInfo();
        timezone_info.is_name_based = false;
        timezone_info.timezone_offset = offset;
        timezone_info.timezone = &DateLUT::instance("UTC");
        timezone_info.timezone_name = "";
        timezone_info.is_utc_timezone = offset == 0;

        auto & factory = FunctionFactory::instance();
        Block block(columns);
        ColumnNumbers cns;
        ColumnsWithTypeAndName argument_columns;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            cns.push_back(arguments[i]);
            argument_columns.push_back(columns.at(i));
        }
        auto bp = factory.tryGet(func_name, context);
        if (!bp)
            throw TiFlashTestException(fmt::format("Function {} not found!", func_name));
        auto func = bp->build(argument_columns);
        block.insert({nullptr, func->getReturnType(), "res"});
        func->execute(block, cns, columns.size());
        return block.getByPosition(columns.size());
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
        micro_second_str
            .append(int_to_2_width_string[origin_micro_second / 10000])
            .append(int_to_2_width_string[origin_micro_second % 10000 / 100])
            .append(int_to_2_width_string[origin_micro_second % 100]);
        micro_second_str.resize(fsp);
        UInt32 new_micro_second = std::stoul(micro_second_str) * std::pow(10, 6 - fsp);
        ASSERT_EQ(origin_micro_second, new_micro_second);
    }

    void ASSERT_CHECK_SYSDATE(const MyDateTime dateTime, ColumnWithTypeAndName actual_col, int offset)
    {
        UInt64 base_packed = dateTime.toPackedUInt();
        MyDateTime base_mdt = MyDateTime(base_packed);
        UInt64 packed = actual_col.column.get()->get64(0);
        MyDateTime mdt = MyDateTime(packed);
        ASSERT_LE(base_packed, packed);
        ASSERT_EQ(((mdt.yearDay() - base_mdt.yearDay()) * 24 + (mdt.hour - base_mdt.hour)), offset);
    }
};

TEST_F(Sysdate, sysdate_unit_Test)
{
    UInt32 fsp = 3;
    auto data_column = createColumn<String>(std::vector<String>{"test"});
    auto fsp_column = createConstColumn<Int64>(1, fsp);
    auto with_fsp_columns = {fsp_column, data_column};
    auto without_fsp_columns = {data_column};
    ColumnNumbers with_fsp_arguments = {0};
    ColumnNumbers without_fsp_arguments = {};

    ColumnWithTypeAndName without_fsp_col = executeFunctionWithTimezone("sysDateWithoutFsp", without_fsp_arguments, without_fsp_columns, 0);
    UInt64 without_fsp_packed = without_fsp_col.column.get()->get64(0);
    MyDateTime without_fsp_date_time(without_fsp_packed);


    ColumnWithTypeAndName with_fsp_col = executeFunctionWithTimezone("sysDateWithFsp", with_fsp_arguments, with_fsp_columns, 0);
    UInt64 with_fsp_packed = with_fsp_col.column.get()->get64(0);
    MyDateTime with_fsp_date_time(with_fsp_packed);

    TimezoneInfo timezone_info;
    timezone_info.is_name_based = false;
    timezone_info.timezone_offset = 0;
    timezone_info.timezone = &DateLUT::instance("UTC");
    timezone_info.timezone_name = "";
    timezone_info.is_utc_timezone = true;
    auto date_time = MyDateTime::getSystemDateTimeByTimezone(timezone_info, fsp);

    auto with_fsp_second_diff = (date_time.yearDay() - with_fsp_date_time.yearDay()) * SECONDS_IN_ONE_DAY + (date_time.hour - with_fsp_date_time.hour) * 60 * 60 + (date_time.minute - with_fsp_date_time.minute) * 60 + (date_time.second - with_fsp_date_time.second);
    auto with_out_fsp_second_diff = (date_time.yearDay() - without_fsp_date_time.yearDay()) * SECONDS_IN_ONE_DAY + (date_time.hour - without_fsp_date_time.hour) * 60 * 60 + (date_time.minute - without_fsp_date_time.minute) * 60 + (date_time.second - without_fsp_date_time.second);

    ASSERT_LE(with_fsp_second_diff, 10);
    ASSERT_LE(with_out_fsp_second_diff, 10);
}

TEST_F(Sysdate, fsp_unit_Test)
{
    auto data_column = createColumn<String>(std::vector<String>{"test"});
    ColumnNumbers with_fsp_arguments = {0};

    for (int fsp = 0; fsp <= 6; ++fsp)
    {
        auto fsp_column = createConstColumn<Int64>(1, fsp);
        auto with_fsp_columns = {fsp_column, data_column};
        ColumnWithTypeAndName with_fsp_col = executeFunctionWithTimezone("sysDateWithFsp", with_fsp_arguments, with_fsp_columns, 0);
        UInt64 with_fsp_packed = with_fsp_col.column.get()->get64(0);
        MyDateTime with_fsp_date_time(with_fsp_packed);
        ASSERT_FSP(fsp, with_fsp_date_time);
    }
}

TEST_F(Sysdate, timezone_unit_Test)
{
    UInt32 fsp = 3;
    TimezoneInfo timezone_info;
    timezone_info.is_name_based = false;
    timezone_info.timezone_offset = 0;
    timezone_info.timezone = &DateLUT::instance("UTC");
    timezone_info.timezone_name = "";
    timezone_info.is_utc_timezone = true;
    auto date_time = MyDateTime::getSystemDateTimeByTimezone(timezone_info, fsp);

    auto data_column = createColumn<String>(std::vector<String>{"test"});
    auto fsp_column = createConstColumn<Int64>(1, fsp);
    auto with_fsp_columns = {fsp_column, data_column};
    auto without_fsp_columns = {data_column};
    ColumnNumbers with_fsp_arguments = {0};
    ColumnNumbers without_fsp_arguments = {};

    for (int i = 1; i < 8; ++i)
    {
        ASSERT_CHECK_SYSDATE(date_time, executeFunctionWithTimezone("sysDateWithFsp", with_fsp_arguments, with_fsp_columns, i * 3600), i);
        ASSERT_CHECK_SYSDATE(date_time, executeFunctionWithTimezone("sysDateWithoutFsp", without_fsp_arguments, without_fsp_columns, i * 3600), i);
    }
}
} // namespace DB::tests
