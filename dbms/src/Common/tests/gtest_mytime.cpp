#include <Common/Exception.h>
#include <Common/MyTime.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <Functions/FunctionsTiDBConversion.h>
#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <tuple>
#include <vector>

namespace DB
{
namespace tests
{

class TestMyTime : public testing::Test
{
protected:
    virtual void SetUp() override {}
    virtual void TearDown() override {}

public:
    static void checkParseMyDateTime(const std::string & str, const std::string & expected, const DataTypeMyDateTime & type)
    {
        try
        {
            UInt64 res = parseMyDateTime(str, type.getFraction()).template safeGet<UInt64>();
            MyDateTime datetime(res);
            std::string actual = datetime.toString(type.getFraction());
            EXPECT_EQ(actual, expected) << "Original datetime string: " << str;
        }
        catch (...)
        {
            std::cerr << "Error occurs when parsing: \"" << str << "\"" << std::endl;
            throw;
        }
    }

    static void checkParseMyDateTime(const std::string & str, const MyDateTime & expected, const DataTypeMyDateTime & type)
    {
        try
        {
            UInt64 res = parseMyDateTime(str, type.getFraction()).template safeGet<UInt64>();
            MyDateTime source(res);
            EXPECT_EQ(source.year, expected.year) << "Original datetime string: " << str;
            EXPECT_EQ(source.month, expected.month) << "Original datetime string: " << str;
            EXPECT_EQ(source.day, expected.day) << "Original datetime string: " << str;
            EXPECT_EQ(source.hour, expected.hour) << "Original datetime string: " << str;
            EXPECT_EQ(source.minute, expected.minute) << "Original datetime string: " << str;
            EXPECT_EQ(source.second, expected.second) << "Original datetime string: " << str;
            EXPECT_EQ(source.micro_second, expected.micro_second) << "Original datetime string: " << str;
        }
        catch (...)
        {
            std::cerr << "Error occurs when parsing: \"" << str << "\"" << std::endl;
            throw;
        }
    }

    static void checkNumberToMyDateTime(const Int64 & input, const MyDateTime & expected, bool expect_error, DAGContext * ctx)
    {
        if (expect_error)
        {
            MyDateTime datetime(0, 0, 0, 0, 0, 0, 0);
            EXPECT_THROW({ numberToDateTime(input, datetime, ctx); }, TiFlashException) << "Original time number: " << input;
            return;
        }

        try
        {
            MyDateTime source(0, 0, 0, 0, 0, 0, 0);
            numberToDateTime(input, source, ctx);
            EXPECT_EQ(source.year, expected.year) << "Original time number: " << input;
            EXPECT_EQ(source.month, expected.month) << "Original time number: " << input;
            EXPECT_EQ(source.day, expected.day) << "Original time number: " << input;
            EXPECT_EQ(source.hour, expected.hour) << "Original time number: " << input;
            EXPECT_EQ(source.minute, expected.minute) << "Original time number: " << input;
            EXPECT_EQ(source.second, expected.second) << "Original time number: " << input;
            EXPECT_EQ(source.micro_second, expected.micro_second) << "Original time number: " << input;
        }
        catch (...)
        {
            std::cerr << "Error occurs when parsing: \"" << input << "\"" << std::endl;
            throw;
        }
    }
};

TEST_F(TestMyTime, ParseMyDateTimeWithFraction)
try
{
    std::vector<std::tuple<std::string, std::string>> cases_with_fsp{
        {"2020-12-10 11:11:11.123456", "2020-12-10 11:11:11.123456"}, // YYYY-MM-DD HH:MM:SS.mmmmmm
        {"00-00-00 00:00:00.123", "2000-00-00 00:00:00.123000"},
        {"1701020304.1", "2017-01-02 03:04:01.000000"},
        {"1701020302.11", "2017-01-02 03:02:11.000000"},
        {"170102037.11", "2017-01-02 03:07:11.000000"},
        {"2018.01.01", "2018-01-01 00:00:00.000000"},
        {"2020.10.10 10.10.10", "2020-10-10 10:10:10.000000"},
        {"2020-10-10 10-10.10", "2020-10-10 10:10:10.000000"},
        {"2020-10-10 10.10", "2020-10-10 10:10:00.000000"},
        {"2018.01.01", "2018-01-01 00:00:00.000000"},
        {"2020--12-10 11:11:11..123456", "2020-12-10 11:11:11.123456"},
    };
    DataTypeMyDateTime type_with_fraction(6);
    for (auto & [str, expected] : cases_with_fsp)
    {
        checkParseMyDateTime(str, expected, type_with_fraction);
    }
}
catch (Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    GTEST_FAIL();
}

TEST_F(TestMyTime, ParseMyDateTimeWithoutFraction)
try
{
    std::vector<std::tuple<std::string, std::string>> cases_without_fsp{
        {"2012-12-31 11:30:45", "2012-12-31 11:30:45"},
        {"0000-00-00 00:00:00", "0000-00-00 00:00:00"},
        {"0001-01-01 00:00:00", "0001-01-01 00:00:00"},
        {"00-12-31 11:30:45", "2000-12-31 11:30:45"},
        {"12-12-31 11:30:45", "2012-12-31 11:30:45"},
        {"2012-12-31", "2012-12-31 00:00:00"},
        {"20121231", "2012-12-31 00:00:00"},
        {"121231", "2012-12-31 00:00:00"},
        {"2012^12^31 11+30+45", "2012-12-31 11:30:45"},
        {"2012^12^31T11+30+45", "2012-12-31 11:30:45"},
        {"2012-2-1 11:30:45", "2012-02-01 11:30:45"},
        {"12-2-1 11:30:45", "2012-02-01 11:30:45"},
        {"20121231113045", "2012-12-31 11:30:45"},
        {"121231113045", "2012-12-31 11:30:45"},
        {"2012-02-29", "2012-02-29 00:00:00"},
        {"00-00-00", "0000-00-00 00:00:00"},
        {"11111111111", "2011-11-11 11:11:01"},
        {"1701020301.", "2017-01-02 03:01:00"},
        {"170102036", "2017-01-02 03:06:00"},
        {"170102039.", "2017-01-02 03:09:00"},
        {"2018-01-01 18", "2018-01-01 18:00:00"},
        {"18-01-01 18", "2018-01-01 18:00:00"},
        {"2018.01.01 00:00:00", "2018-01-01 00:00:00"},
        {"2018/01/01-00:00:00", "2018-01-01 00:00:00"},
        {"4710072", "2047-10-07 02:00:00"},
    };
    DataTypeMyDateTime type_without_fraction(0);
    for (auto & [str, expected] : cases_without_fsp)
    {
        checkParseMyDateTime(str, expected, type_without_fraction);
    }
}
catch (Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    GTEST_FAIL();
}

TEST_F(TestMyTime, ParseMyDateTimeWithTimezone)
try
{
    std::vector<std::tuple<std::string, MyDateTime>> cases{
        {"2006-01-02T15:04:05Z", MyDateTime(2006, 1, 2, 15, 4, 5, 0)},
        {"2020-10-21T16:05:10Z", MyDateTime(2020, 10, 21, 16, 5, 10, 0)},
        {"2020-10-21T16:05:10.50+08", MyDateTime(2020, 10, 21, 8, 5, 10, 500 * 1000)},
        {"2020-10-21T16:05:10.50-0700", MyDateTime(2020, 10, 21, 23, 5, 10, 500 * 1000)},
        {"2020-10-21T16:05:10.50+09:00", MyDateTime(2020, 10, 21, 7, 5, 10, 500 * 1000)},
        {"2006-01-02T15:04:05+09:00", MyDateTime(2006, 1, 2, 6, 4, 5, 0)},
        {"2006-01-02T15:04:05-02:00", MyDateTime(2006, 1, 2, 17, 4, 5, 0)},
        {"2006-01-02T15:04:05-14:00", MyDateTime(2006, 1, 3, 5, 4, 5, 0)},
    };
    DataTypeMyDateTime type(6);
    for (auto & [str, expected] : cases)
    {
        checkParseMyDateTime(str, expected, type);
    }
}
catch (Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    GTEST_FAIL();
}

TEST_F(TestMyTime, NumberToDateTime)
try
{
    std::vector<std::tuple<Int64, bool /* ExpectError */, MyDateTime>> cases{
        {20101010111111, false, MyDateTime(2010, 10, 10, 11, 11, 11, 0)},
        {2010101011111, false, MyDateTime(201, 1, 1, 1, 11, 11, 0)},
        {201010101111, false, MyDateTime(2020, 10, 10, 10, 11, 11, 0)},
        {20101010111, false, MyDateTime(2002, 1, 1, 1, 1, 11, 0)},
        {2010101011, true, MyDateTime(0, 0, 0, 0, 0, 0, 0)},
        {201010101, false, MyDateTime(2000, 2, 1, 1, 1, 1, 0)},
        {20101010, false, MyDateTime(2010, 10, 10, 0, 0, 0, 0)},
        {2010101, false, MyDateTime(201, 1, 1, 0, 0, 0, 0)},
        {201010, false, MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {20101, false, MyDateTime(2002, 1, 1, 0, 0, 0, 0)},
        {2010, true, MyDateTime(0, 0, 0, 0, 0, 0, 0)},
        {201, false, MyDateTime(2000, 2, 1, 0, 0, 0, 0)},
        {20, true, MyDateTime(0, 0, 0, 0, 0, 0, 0)},
        {2, true, MyDateTime(0, 0, 0, 0, 0, 0, 0)},
        {0, false, MyDateTime(0, 0, 0, 0, 0, 0, 0)},
        {-1, true, MyDateTime(0, 0, 0, 0, 0, 0, 0)},
        {99999999999999, true, MyDateTime(0, 0, 0, 0, 0, 0, 0)},
        {100000000000000, true, MyDateTime(0, 0, 0, 0, 0, 0, 0)},
        {10000102000000, false, MyDateTime(1000, 1, 2, 0, 0, 0, 0)},
        {19690101000000, false, MyDateTime(1969, 1, 1, 0, 0, 0, 0)},
        {991231235959, false, MyDateTime(1999, 12, 31, 23, 59, 59, 0)},
        {691231235959, false, MyDateTime(2069, 12, 31, 23, 59, 59, 0)},
        {370119031407, false, MyDateTime(2037, 1, 19, 3, 14, 7, 0)},
        {380120031407, false, MyDateTime(2038, 1, 20, 3, 14, 7, 0)},
        {11111111111, false, MyDateTime(2001, 11, 11, 11, 11, 11, 0)},
    };
    for (auto & [input, expect_error, expected] : cases)
    {
        checkNumberToMyDateTime(input, expected, expect_error, nullptr);
    }
}
catch (Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    GTEST_FAIL();
}

} // namespace tests
} // namespace DB
