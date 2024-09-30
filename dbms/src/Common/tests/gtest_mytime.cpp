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

#include <Common/Exception.h>
#include <Common/MyTime.h>
#include <DataTypes/DataTypeMyDateTime.h>
#include <Functions/FunctionsTiDBConversion.h>
#include <TestUtils/TiFlashTestBasic.h>

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
    static void checkParseMyDateTime(
        const std::string & str,
        const std::string & expected,
        const DataTypeMyDateTime & type)
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

    static void checkParseMyDateTime(
        const std::string & str,
        const MyDateTime & expected,
        const DataTypeMyDateTime & type)
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

    static void checkNumberToMyDateTime(
        const Int64 & input,
        const MyDateTime & expected,
        bool expect_error,
        DAGContext *)
    {
        if (expect_error)
        {
            MyDateTime datetime(0, 0, 0, 0, 0, 0, 0);
            EXPECT_TRUE(numberToDateTime(input, datetime));
            return;
        }

        try
        {
            MyDateTime source(0, 0, 0, 0, 0, 0, 0);
            numberToDateTime(input, source);
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
        {"2020-01-01 12:00:00.1234xxxx -0600 PST", "2020-01-01 12:00:00.123400"},
        {"2020-01-01 12:00:00.123456-05:00", "2020-01-01 17:00:00.123456"}, // tidb/issues/49555
    };
    DataTypeMyDateTime type_with_fraction(6);
    for (auto & [str, expected] : cases_with_fsp)
    {
        checkParseMyDateTime(str, expected, type_with_fraction);
    }
    cases_with_fsp = {
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
        // {"00-00-00 00:00:00.123", "2000-00-00 00:00:00.123"},
        {"11111111111", "2011-11-11 11:11:01"},
        {"1701020301.", "2017-01-02 03:01:00"},
        // {"1701020304.1", "2017-01-02 03:04:01.0"},
        // {"1701020302.11", "2017-01-02 03:02:11.00"},
        {"170102036", "2017-01-02 03:06:00"},
        {"170102039.", "2017-01-02 03:09:00"},
        // {"170102037.11", "2017-01-02 03:07:11.00"},
        {"2018-01-01 18", "2018-01-01 18:00:00"},
        {"18-01-01 18", "2018-01-01 18:00:00"},
        // {"2018.01.01", "2018-01-01 00:00:00.00"},
        // {"2020.10.10 10.10.10", "2020-10-10 10:10:10.00"},
        // {"2020-10-10 10-10.10", "2020-10-10 10:10:10.00"},
        // {"2020-10-10 10.10", "2020-10-10 10:10:00.00"},
        // {"2018.01.01", "2018-01-01 00:00:00.00"},
        {"2018.01.01 00:00:00", "2018-01-01 00:00:00"},
        {"2018/01/01-00:00:00", "2018-01-01 00:00:00"},
        {"4710072", "2047-10-07 02:00:00"},
        {"2016-06-01 00:00:00 00:00:00", "2016-06-01 00:00:00"},
        {"2020-06-01 00:00:00ads!,?*da;dsx", "2020-06-01 00:00:00"},

        {"2020-05-28 23:59:59 00:00:00", "2020-05-28 23:59:59"},
        {"2020-05-28 23:59:59-00:00:00", "2020-05-28 23:59:59"},
        {"2020-05-28 23:59:59T T00:00:00", "2020-05-28 23:59:59"},
        {"2020-10-22 10:31-10:12", "2020-10-22 10:31:10"},
        {"2018.01.01 01:00:00", "2018-01-01 01:00:00"},

        // {"2020-01-01 12:00:00.123456+05:00", "2020-01-01 07:00:00.123456"}
    };
    DataTypeMyDateTime type_with_zero_fraction(0);
    for (auto & [str, expected] : cases_with_fsp)
    {
        checkParseMyDateTime(str, expected, type_with_zero_fraction);
    }
    DataTypeMyDateTime tp(2);
    checkParseMyDateTime("2010-12-31 23:59:59.99999", "2011-01-01 00:00:00.00", tp);
    checkParseMyDateTime("2010-12-31 23:59:59.99xxxxx -0600 PST", "2010-12-31 23:59:59.99", tp);
    checkParseMyDateTime("2020-01-01 12:00:00.123456 +0600 PST", "2020-01-01 12:00:00.12", tp);
    checkParseMyDateTime("2020-01-01 12:00:00.123456 -0600 PST", "2020-01-01 12:00:00.12", tp);
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

TEST_F(TestMyTime, Parser)
try
{
    std::vector<std::tuple<String, String, std::optional<MyDateTime>>> cases{
        {" 2/Jun", "%d/%b/%Y", MyDateTime{0, 6, 2, 0, 0, 0, 0}}, // More patterns than input string
        {" liter", "lit era l", MyDateTime{0, 0, 0, 0, 0, 0, 0}}, // More patterns than input string
        // Test case for empty input
        {"   ", " ", MyDateTime{0, 0, 0, 0, 0, 0, 0}}, //
        {"    ", "%d/%b/%Y", MyDateTime{0, 0, 0, 0, 0, 0, 0}},
        // Prefix white spaces should be ignored
        {"  2/Jun/2019 ", "%d/%b/%Y", MyDateTime{2019, 6, 2, 0, 0, 0, 0}},
        {"   2/Jun/2019 ", " %d/%b/%Y", MyDateTime{2019, 6, 2, 0, 0, 0, 0}},
        //
        {"31/May/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", MyDateTime{2016, 5, 31, 12, 34, 56, 123400}},
        {"31/may/2016 12:34:56.1234",
         "%d/%b/%Y %H:%i:%S.%f",
         MyDateTime{2016, 5, 31, 12, 34, 56, 123400}}, // case insensitive
        {"31/mayy/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", std::nullopt}, // invalid %b
        {"31/mey/2016 12:34:56.1234", "%d/%b/%Y %H:%i:%S.%f", std::nullopt}, // invalid %b
        {"30/April/2016 12:34:56.",
         "%d/%M/%Y %H:%i:%s.%f",
         MyDateTime{2016, 4, 30, 12, 34, 56, 0}}, // empty %f is valid
        {"30/april/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", MyDateTime{2016, 4, 30, 12, 34, 56, 0}}, // case insensitive
        {"30/Apri/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", std::nullopt}, // invalid %M
        {"30/Aprill/2016 12:34:56.", "%d/%M/%Y %H:%i:%s.%f", std::nullopt}, // invalid %M
        {"30/Feb/2016 12:34:56.1234",
         "%d/%b/%Y %H:%i:%S.%f",
         MyDateTime{
             2016,
             2,
             30,
             12,
             34,
             56,
             123400}}, // Feb 30th (not exist in actual) is valid for parsing (in mariadb)
        {"31/April/2016 12:34:56.",
         "%d/%M/%Y %H:%i:%s.%f",
         MyDateTime{2016, 4, 31, 12, 34, 56, 0}}, // April 31th (not exist in actual)
        {"01,5,2013 9", "%d,%c,%Y %f", MyDateTime{2013, 5, 1, 0, 0, 0, 900000}},
        {"01,52013", "%d,%c%Y", std::nullopt}, // %c will try to parse '52' as month and fail
        {"01,5,2013", "%d,%c,%Y", MyDateTime{2013, 5, 1, 0, 0, 0, 0}}, //
        {"01,5,2013 ", "%d,%c,%Y %f", MyDateTime{2013, 5, 1, 0, 0, 0, 0}},

        /// Test cases for AM/PM set
        {"10:11:12 AM", "%H:%i:%S %p", std::nullopt}, // should not set %H %p at the same time
        {"10:11:12 Am", "%h:%i:%S %p", MyDateTime(0, 0, 0, 10, 11, 12, 0)},
        {"10:11:12 A", "%h:%i:%S %p", std::nullopt}, // EOF while parsing "AM"/"PM"
        {"00:11:12 AM", "%h:%i:%S %p", std::nullopt}, // should not happen: %p set, %h not set
        {"11:12 AM", "%i:%S %p", std::nullopt}, // should not happen: %p set, %h not set
        {"11:12 abcd", "%i:%S ", MyDateTime{0, 0, 0, 0, 11, 12, 0}}, // without %p, %h not set is ok
        {"00:11:12 ", "%h:%i:%S ", std::nullopt}, // 0 is not a valid number of %h
        {"12:11:12 AP", "%h:%i:%S %p", std::nullopt}, // only AM/PM is valid
        {"12:11:12 AM", "%h:%i:%S %p", MyDateTime(0, 0, 0, 0, 11, 12, 0)},
        {"12:11:12 PM", "%h:%i:%S %p", MyDateTime(0, 0, 0, 12, 11, 12, 0)},
        {"11:11:12 pM", "%h:%i:%S %p", MyDateTime(0, 0, 0, 23, 11, 12, 0)},
        /// Special case for %h with 12
        {"12:11:23 ", "%h:%i:%S ", MyDateTime(0, 0, 0, 0, 11, 23, 0)},
        // For %% -- FIXME: Ignored by now, both tidb and mariadb 10.3.14 can not handle it
        // {"01/Feb/2016 % 23:45:54", "%d/%b/%Y %% %H:%i:%S", MyDateTime(2016, 2, 1, 23, 45, 54, 0)},
        // {"01/Feb/2016 %% 23:45:54", "%d/%b/%Y %%%% %H:%i:%S", MyDateTime(2016, 2, 1, 23, 45, 54, 0)},
        {"01/Feb/2016 % 23:45:54", "%d/%b/%Y %% %H:%i:%S", std::nullopt},
        {"01/Feb/2016 %% 23:45:54", "%d/%b/%Y %%%% %H:%i:%S", std::nullopt},

        /// Test cases for %r
        {" 04 :13:56 AM13/05/2019", "%r %d/%c/%Y", MyDateTime{2019, 5, 13, 4, 13, 56, 0}},
        {"13:13:56 AM13/5/2019", "%r", std::nullopt}, // hh = 13 with am is invalid
        {"00:13:56 AM13/05/2019", "%r", std::nullopt}, // hh = 0 with am is invalid
        {"00:13:56 pM13/05/2019", "%r", std::nullopt}, // hh = 0 with pm is invalid
        {"12: 13:56 AM 13/05/2019", "%r%d/%c/%Y", MyDateTime{2019, 5, 13, 0, 13, 56, 0}},
        {"12:13 :56 pm 13/05/2019", "%r %d/%c/%Y", MyDateTime{2019, 5, 13, 12, 13, 56, 0}},
        {"11:13: 56pm  13/05/2019", "%r %d/%c/%Y", MyDateTime{2019, 5, 13, 23, 13, 56, 0}},
        {"11:13:56a", "%r", std::nullopt}, // EOF while parsing "AM"/"PM"
        {"11:13", "%r", MyDateTime{0, 0, 0, 11, 13, 0, 0}}, //
        {"11:", "%r", MyDateTime{0, 0, 0, 11, 0, 0, 0}}, //
        {"12", "%r", MyDateTime{0, 0, 0, 0, 0, 0, 0}},

        /// Test cases for %T
        {" 4 :13:56 13/05/2019", "%T %d/%c/%Y", MyDateTime{2019, 5, 13, 4, 13, 56, 0}},
        {"23: 13:56  13/05/2019", "%T%d/%c/%Y", MyDateTime{2019, 5, 13, 23, 13, 56, 0}},
        {"12:13 :56 13/05/2019", "%T %d/%c/%Y", MyDateTime{2019, 5, 13, 12, 13, 56, 0}},
        {"19:13: 56  13/05/2019", "%T %d/%c/%Y", MyDateTime{2019, 5, 13, 19, 13, 56, 0}},
        {"21:13", "%T", MyDateTime{0, 0, 0, 21, 13, 0, 0}}, //
        {"21:", "%T", MyDateTime{0, 0, 0, 21, 0, 0, 0}},

        // mutiple chars between pattern
        {"01/Feb/2016 abcdefg 23:45:54", "%d/%b/%Y abcdefg %H:%i:%S", MyDateTime(2016, 2, 1, 23, 45, 54, 0)},
        // the number of whitespace between pattern and input doesn't matter
        {"01/Feb/2016   abcdefg 23:45: 54", "%d/%b/%Y abcdefg %H  :%i:%S", MyDateTime(2016, 2, 1, 23, 45, 54, 0)},
        {"01/Feb/  2016   abc  defg   23:45:54",
         "%d/  %b/%Y abcdefg %H:   %i:%S",
         MyDateTime(2016, 2, 1, 23, 45, 54, 0)},
        {"01/Feb  /2016   ab cdefg 23:  45:54",
         "%d  /%b/%Y abc  defg %H:%i  :%S",
         MyDateTime{2016, 2, 1, 23, 45, 54, 0}},

        /// Cases collect from MySQL 8.0 document
        {"01,5,2013", "%d,%m,%Y", MyDateTime{2013, 5, 1, 0, 0, 0, 0}}, //
        {"May 1, 2013", "%M %d,%Y", MyDateTime{2013, 5, 1, 0, 0, 0, 0}}, //
        {"a09:30:17", "a%h:%i:%s", MyDateTime{0, 0, 0, 9, 30, 17, 0}}, //
        {"a09:30:17", "%h:%i:%s", std::nullopt}, //
        {"09:30:17a", "%h:%i:%s", MyDateTime{0, 0, 0, 9, 30, 17, 0}}, //
        {"abc", "abc", MyDateTime{0, 0, 0, 0, 0, 0, 0}}, //
        {"9", "%m", MyDateTime{0, 9, 0, 0, 0, 0, 0}}, //
        {"9", "%s", MyDateTime{0, 0, 0, 0, 0, 9, 0}}, //
        {"00/00/0000", "%m/%d/%Y", MyDateTime{0, 0, 0, 0, 0, 0, 0}}, //
        {"04/31/2004", "%m/%d/%Y", MyDateTime{2004, 4, 31, 0, 0, 0, 0}},

        /// Below cases are ported from TiDB
        {"10/28/2011 9:46:29 pm", "%m/%d/%Y %l:%i:%s %p", MyDateTime(2011, 10, 28, 21, 46, 29, 0)},
        {"10/28/2011 9:46:29 Pm", "%m/%d/%Y %l:%i:%s %p", MyDateTime(2011, 10, 28, 21, 46, 29, 0)},
        {"2011/10/28 9:46:29 am", "%Y/%m/%d %l:%i:%s %p", MyDateTime(2011, 10, 28, 9, 46, 29, 0)},
        {"20161122165022", "%Y%m%d%H%i%s", MyDateTime(2016, 11, 22, 16, 50, 22, 0)},
        {"2016 11 22 16 50 22", "%Y%m%d%H%i%s", MyDateTime(2016, 11, 22, 16, 50, 22, 0)}, // fail, should ignore sep
        {"16-50-22 2016 11 22", "%H-%i-%s%Y%m%d", MyDateTime(2016, 11, 22, 16, 50, 22, 0)}, // fail, should ignore sep
        {"16-50 2016 11 22", "%H-%i-%s%Y%m%d", std::nullopt},
        {"15-01-2001 1:59:58.999", "%d-%m-%Y %I:%i:%s.%f", MyDateTime(2001, 1, 15, 1, 59, 58, 999000)},
        {"15-01-2001 1:59:58.1", "%d-%m-%Y %H:%i:%s.%f", MyDateTime(2001, 1, 15, 1, 59, 58, 100000)},
        {"15-01-2001 1:59:58.", "%d-%m-%Y %H:%i:%s.%f", MyDateTime(2001, 1, 15, 1, 59, 58, 0)},
        {"15-01-2001 1:9:8.999", "%d-%m-%Y %H:%i:%s.%f", MyDateTime(2001, 1, 15, 1, 9, 8, 999000)},
        {"15-01-2001 1:9:8.999", "%d-%m-%Y %H:%i:%S.%f", MyDateTime(2001, 1, 15, 1, 9, 8, 999000)},
        {"2003-01-02 10:11:12 PM", "%Y-%m-%d %H:%i:%S %p", std::nullopt}, // should not set %H %p at the same time
        {"10:20:10AM", "%H:%i:%S%p", std::nullopt}, // should not set %H %p at the same time
        // test %@(skip alpha), %#(skip number), %.(skip punct)
        {"2020-10-10ABCD", "%Y-%m-%d%@", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"2020-10-101234", "%Y-%m-%d%#", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"2020-10-10....", "%Y-%m-%d%.", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"2020-10-10.1", "%Y-%m-%d%.%#%@", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"abcd2020-10-10.1", "%@%Y-%m-%d%.%#%@", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"abcd-2020-10-10.1", "%@-%Y-%m-%d%.%#%@", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"2020-10-10", "%Y-%m-%d%@", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},
        {"2020-10-10abcde123abcdef", "%Y-%m-%d%@%#", MyDateTime(2020, 10, 10, 0, 0, 0, 0)},

        /// Cases ported from mysql testing by executing following SQL in mysql
        /// create table t1 (date char(30) COLLATE latin1_bin, format char(30) COLLATE latin1_bin not null);
        /// insert into t1 values (...),...
        /// select if( str_to_date is not null, concat( '{"', date, '", "', format, '", ', concat( "MyDateTime{", year(str_to_date), date_format(str_to_date, ",%c,%e,"), hour(str_to_date), ",", minute(str_to_date), ",", second(str_to_date), ",", MICROSECOND(str_to_date), "}" ), '}, //' ), concat( '{"', date, '", "', format, '", std::nullopt}, //' )) as s from ( select date, format, str_to_date(date, format) as str_to_date from t1 ) a group by date, format, str_to_date order by date;
        {"0003-01-02 8:11:2.123456", "%Y-%m-%d %H:%i:%S.%#", MyDateTime{3, 1, 2, 8, 11, 2, 0}}, //
        {"03-01-02 8:11:2.123456", "%Y-%m-%d %H:%i:%S.%#", MyDateTime{2003, 1, 2, 8, 11, 2, 0}}, //
        {"03-01-02 8:11:2.123456", "%y-%m-%d %H:%i:%S.%#", MyDateTime{2003, 1, 2, 8, 11, 2, 0}}, //
        {"10:20:10", "%H:%i:%s", MyDateTime{0, 0, 0, 10, 20, 10, 0}}, //
        {"10:20:10", "%T", MyDateTime{0, 0, 0, 10, 20, 10, 0}}, //
        {"10:20:10", "%h:%i:%s.%f", MyDateTime{0, 0, 0, 10, 20, 10, 0}}, //
        {"10:20:10.44AM", "%h:%i:%s.%f%p", MyDateTime{0, 0, 0, 10, 20, 10, 440000}}, //
        {"10:20:10AM", "%h:%i:%s%p", MyDateTime{0, 0, 0, 10, 20, 10, 0}}, //
        {"10:20:10AM", "%r", MyDateTime{0, 0, 0, 10, 20, 10, 0}}, //
        {"15 MAY 2001", "%d %b %Y", MyDateTime{2001, 5, 15, 0, 0, 0, 0}}, //
        // {"15 SEPTEMB 2001", "%d %M %Y", MyDateTime{2001, 9, 15, 0, 0, 0, 0}}, // The SEPTEMB is a broken string of 'SEPTEMBER', ignore this case
        {"15 September 2001", "%d %M %Y", MyDateTime{2001, 9, 15, 0, 0, 0, 0}}, //
        {"15-01-20", "%d-%m-%y", MyDateTime{2020, 1, 15, 0, 0, 0, 0}}, //
        {"15-01-2001", "%d-%m-%Y %H:%i:%S", MyDateTime{2001, 1, 15, 0, 0, 0, 0}}, //
        {"15-01-2001 12:59:58", "%d-%m-%Y %H:%i:%S", MyDateTime{2001, 1, 15, 12, 59, 58, 0}}, //
        {"15-2001-1", "%d-%Y-%c", MyDateTime{2001, 1, 15, 0, 0, 0, 0}}, //
        {"2003-01-02 01:11:12.12345AM", "%Y-%m-%d %h:%i:%S.%f%p", MyDateTime{2003, 1, 2, 1, 11, 12, 123450}}, //
        {"2003-01-02 02:11:12.12345AM", "%Y-%m-%d %h:%i:%S.%f %p", MyDateTime{2003, 1, 2, 2, 11, 12, 123450}}, //
        {"2003-01-02 10:11:12", "%Y-%m-%d %H:%i:%S", MyDateTime{2003, 1, 2, 10, 11, 12, 0}}, //
        {"2003-01-02 10:11:12 PM", "%Y-%m-%d %h:%i:%S %p", MyDateTime{2003, 1, 2, 22, 11, 12, 0}}, //
        {"2003-01-02 11:11:12Pm", "%Y-%m-%d %h:%i:%S%p", MyDateTime{2003, 1, 2, 23, 11, 12, 0}}, //
        {"2003-01-02 12:11:12.12345 am", "%Y-%m-%d %h:%i:%S.%f%p", MyDateTime{2003, 1, 2, 0, 11, 12, 123450}}, //
        // some cases that are not implemented
        // {"060 2004", "%j %Y", MyDateTime{2004, 2, 29, 0, 0, 0, 0}},                 //
        // {"15th May 2001", "%D %b %Y", MyDateTime{2001, 5, 15, 0, 0, 0, 0}},         //
        // {"4 53 1998", "%w %u %Y", MyDateTime{1998, 12, 31, 0, 0, 0, 0}},            //
        // {"Sund 15 MAY 2001", "%W %d %b %Y", MyDateTime{2001, 5, 15, 0, 0, 0, 0}},   //
        // {"Sunday 01 2001", "%W %v %x", MyDateTime{2001, 1, 7, 0, 0, 0, 0}},         //
        // {"Sunday 15 MAY 2001", "%W %d %b %Y", MyDateTime{2001, 5, 15, 0, 0, 0, 0}}, //
        // {"Thursday 53 1998", "%W %u %Y", MyDateTime{1998, 12, 31, 0, 0, 0, 0}},     //
        // {"Tuesday 00 2002", "%W %U %Y", MyDateTime{2002, 1, 1, 0, 0, 0, 0}},        //
        // {"Tuesday 52 2001", "%W %V %X", MyDateTime{2002, 1, 1, 0, 0, 0, 0}},        //
        // Test 'maybe' date formats and 'strange but correct' results
        {"03-01-02 10:11:12 PM", "%Y-%m-%d %h:%i:%S %p", MyDateTime{2003, 1, 2, 22, 11, 12, 0}}, //
        {"10:20:10AM", "%h:%i:%s", MyDateTime{0, 0, 0, 10, 20, 10, 0}}, //
        {"2003-01-02 10:11:12", "%Y-%m-%d %h:%i:%S", MyDateTime{2003, 1, 2, 10, 11, 12, 0}}, //
        // Test wrong dates or converion specifiers
        {"10:20:10AM", "%H:%i:%s%p", std::nullopt}, //
        {"15 Ju 2001", "%d %M %Y", std::nullopt}, //
        {"15 Septembei 2001", "%d %M %Y", std::nullopt}, //
        {"2003-01-02 10:11:12 PM", "%Y-%m-%d %H:%i:%S %p", std::nullopt}, //
        {"2003-01-02 10:11:12 PM", "%y-%m-%d %H:%i:%S %p", std::nullopt}, //
        {"2003-01-02 10:11:12.123456", "%Y-%m-%d %h:%i:%S %p", std::nullopt}, //
        {"2003-01-02 10:11:12AM", "%Y-%m-%d %h:%i:%S.%f %p", std::nullopt}, //
        {"2003-01-02 10:11:12AN", "%Y-%m-%d %h:%i:%S%p", std::nullopt}, //
        // {"7 53 1998", "%w %u %Y", std::nullopt},                              //
        // {"Sund 15 MA", "%W %d %b %Y", std::nullopt},                          //
        // {"Sunday 01 2001", "%W %v %X", std::nullopt},                         //
        // {"Thursdai 12 1998", "%W %u %Y", std::nullopt},                       //
        // {"Tuesday 52 2001", "%W %V %Y", std::nullopt},                        //
        // {"Tuesday 52 2001", "%W %V %x", std::nullopt},                        //
        // {"Tuesday 52 2001", "%W %u %x", std::nullopt},                        //

        // Test cases for %b
        {"10/JAN/2010", "%d/%b/%Y", MyDateTime{2010, 1, 10, 0, 0, 0, 0}}, // Right spill, case-insensitive
        {"10/FeB/2010", "%d/%b/%Y", MyDateTime{2010, 2, 10, 0, 0, 0, 0}},
        {"10/MAr/2010", "%d/%b/%Y", MyDateTime{2010, 3, 10, 0, 0, 0, 0}},
        {"10/ApR/2010", "%d/%b/%Y", MyDateTime{2010, 4, 10, 0, 0, 0, 0}},
        {"10/mAY/2010", "%d/%b/%Y", MyDateTime{2010, 5, 10, 0, 0, 0, 0}},
        {"10/JuN/2010", "%d/%b/%Y", MyDateTime{2010, 6, 10, 0, 0, 0, 0}},
        {"10/JUL/2010", "%d/%b/%Y", MyDateTime{2010, 7, 10, 0, 0, 0, 0}},
        {"10/Aug/2010", "%d/%b/%Y", MyDateTime{2010, 8, 10, 0, 0, 0, 0}},
        {"10/seP/2010", "%d/%b/%Y", MyDateTime{2010, 9, 10, 0, 0, 0, 0}},
        {"10/Oct/2010", "%d/%b/%Y", MyDateTime{2010, 10, 10, 0, 0, 0, 0}},
        {"10/NOV/2010", "%d/%b/%Y", MyDateTime{2010, 11, 10, 0, 0, 0, 0}},
        {"10/DEC/2010", "%d/%b/%Y", MyDateTime{2010, 12, 10, 0, 0, 0, 0}},
        {"10/January/2010", "%d/%b/%Y", std::nullopt}, // Test full spilling

        // Test cases for %M
        {"10/January/2010", "%d/%M/%Y", MyDateTime{2010, 1, 10, 0, 0, 0, 0}}, // Test full spilling
        {"10/February/2010", "%d/%M/%Y", MyDateTime{2010, 2, 10, 0, 0, 0, 0}},
        {"10/March/2010", "%d/%M/%Y", MyDateTime{2010, 3, 10, 0, 0, 0, 0}},
        {"10/April/2010", "%d/%M/%Y", MyDateTime{2010, 4, 10, 0, 0, 0, 0}},
        {"10/May/2010", "%d/%M/%Y", MyDateTime{2010, 5, 10, 0, 0, 0, 0}},
        {"10/June/2010", "%d/%M/%Y", MyDateTime{2010, 6, 10, 0, 0, 0, 0}},
        {"10/July/2010", "%d/%M/%Y", MyDateTime{2010, 7, 10, 0, 0, 0, 0}},
        {"10/August/2010", "%d/%M/%Y", MyDateTime{2010, 8, 10, 0, 0, 0, 0}},
        {"10/September/2010", "%d/%M/%Y", MyDateTime{2010, 9, 10, 0, 0, 0, 0}},
        {"10/October/2010", "%d/%M/%Y", MyDateTime{2010, 10, 10, 0, 0, 0, 0}},
        {"10/November/2010", "%d/%M/%Y", MyDateTime{2010, 11, 10, 0, 0, 0, 0}},
        {"10/December/2010", "%d/%M/%Y", MyDateTime{2010, 12, 10, 0, 0, 0, 0}},

        // Test cases for %c
        // {"10/0/2010", "%d/%c/%Y", MyDateTime{2010, 0, 10, 0, 0, 0, 0}},   // TODO: Need Check NO_ZERO_DATE
        {"10/1/2010", "%d/%c/%Y", MyDateTime{2010, 1, 10, 0, 0, 0, 0}},
        {"10/01/2010", "%d/%c/%Y", MyDateTime{2010, 1, 10, 0, 0, 0, 0}},
        {"10/001/2010", "%d/%c/%Y", std::nullopt},
        {"10/13/2010", "%d/%c/%Y", std::nullopt},
        {"10/12/2010", "%d/%c/%Y", MyDateTime{2010, 12, 10, 0, 0, 0, 0}},

        // Test cases for %d, %e
        // {"0/12/2010", "%d/%c/%Y", MyDateTime{2010, 12, 0, 0, 0, 0, 0}},  // TODO: Need Check NO_ZERO_DATE
        {"1/12/2010", "%d/%c/%Y", MyDateTime{2010, 12, 1, 0, 0, 0, 0}},
        {"05/12/2010", "%d/%c/%Y", MyDateTime{2010, 12, 5, 0, 0, 0, 0}},
        {"05/12/2010", "%e/%c/%Y", MyDateTime{2010, 12, 5, 0, 0, 0, 0}},
        {"31/12/2010", "%d/%c/%Y", MyDateTime{2010, 12, 31, 0, 0, 0, 0}},
        {"32/12/2010", "%d/%c/%Y", std::nullopt},
        {"30/11/2010", "%d/%c/%Y", MyDateTime{2010, 11, 30, 0, 0, 0, 0}},
        {"31/11/2010", "%e/%c/%Y", MyDateTime{2010, 11, 31, 0, 0, 0, 0}},
        {"28/2/2010", "%e/%c/%Y", MyDateTime{2010, 2, 28, 0, 0, 0, 0}},
        {"29/2/2010", "%e/%c/%Y", MyDateTime{2010, 2, 29, 0, 0, 0, 0}},
        {"29/2/2020", "%e/%c/%Y", MyDateTime{2020, 2, 29, 0, 0, 0, 0}},

        // Test cases for %Y
        // {"1/12/0000", "%d/%c/%Y", MyDateTime{0000, 12, 1, 0, 0, 0, 0}}, // TODO: Need Check NO_ZERO_DATE
        {"1/12/01", "%d/%c/%Y", MyDateTime{2001, 12, 1, 0, 0, 0, 0}},
        {"1/12/0001", "%d/%c/%Y", MyDateTime{0001, 12, 1, 0, 0, 0, 0}},
        {"1/12/2020", "%d/%c/%Y", MyDateTime{2020, 12, 1, 0, 0, 0, 0}},
        {"1/12/9999", "%d/%c/%Y", MyDateTime{9999, 12, 1, 0, 0, 0, 0}},

        // Test cases for %f
        {"01,5,2013 999999", "%d,%c,%Y %f", MyDateTime{2013, 5, 1, 0, 0, 0, 999999}},
        {"01,5,2013 0", "%d,%c,%Y %f", MyDateTime{2013, 5, 1, 0, 0, 0, 0}},
        {"01,5,2013 9999990000000", "%d,%c,%Y %f", MyDateTime{2013, 5, 1, 0, 0, 0, 999999}},
        {"01,5,2013 1", "%d,%c,%Y %f", MyDateTime{2013, 5, 1, 0, 0, 0, 100000}},
        {"01,5,2013 1230", "%d,%c,%Y %f", MyDateTime{2013, 5, 1, 0, 0, 0, 123000}},
        {"01,5,2013 01", "%d,%c,%Y %f", MyDateTime{2013, 5, 1, 0, 0, 0, 10000}}, // issue 3556

        // Test cases for %h, %I, %l
        {"00:11:12 ", "%h:%i:%S ", std::nullopt}, // 0 is not a valid number of %h
        {"01:11:12 ", "%I:%i:%S ", MyDateTime{0, 0, 0, 01, 11, 12, 0}},
        {"12:11:12 ", "%l:%i:%S ", MyDateTime{0, 0, 0, 0, 11, 12, 0}},

        // Test cases for %k, %H
        {"00:11:12 ", "%H:%i:%S ", MyDateTime{0, 0, 0, 00, 11, 12, 0}},
        {"01:11:12 ", "%k:%i:%S ", MyDateTime{0, 0, 0, 01, 11, 12, 0}},
        {"12:11:12 ", "%H:%i:%S ", MyDateTime{0, 0, 0, 12, 11, 12, 0}},
        {"24:11:12 ", "%k:%i:%S ", std::nullopt},

        // Test cases for %i
        {"00:00:12 ", "%H:%i:%S ", MyDateTime{0, 0, 0, 00, 00, 12, 0}},
        {"00:01:12 ", "%H:%i:%S ", MyDateTime{0, 0, 0, 00, 01, 12, 0}},
        {"00:59:12 ", "%H:%i:%S ", MyDateTime{0, 0, 0, 00, 59, 12, 0}},
        {"00:60:12 ", "%H:%i:%S ", std::nullopt},

        // Test cases for %s, %S
        {"00:00:00 ", "%H:%i:%s ", MyDateTime{0, 0, 0, 00, 00, 00, 0}},
        {"00:01:01 ", "%H:%i:%s ", MyDateTime{0, 0, 0, 00, 01, 01, 0}},
        {"00:59:59 ", "%H:%i:%S ", MyDateTime{0, 0, 0, 00, 59, 59, 0}},
        {"00:59:60 ", "%H:%i:%S ", std::nullopt},
    };

    auto result_formatter = MyDateTimeFormatter("%Y/%m/%d %T.%f");
    size_t idx = 0;
    for (const auto & [input, fmt, expected] : cases)
    {
        MyDateTimeParser parser(fmt);
        auto packed = parser.parseAsPackedUInt(input);
        if (expected == std::nullopt)
        {
            MyTimeBase actual_time;
            String actual_str;
            if (packed)
            {
                actual_time = MyTimeBase(*packed);
                result_formatter.format(actual_time, actual_str);
            }
            EXPECT_FALSE((bool)packed) //
                << "[case=" << idx << "] "
                << "[fmt=" << fmt << "] [input=" << input << "] [actual=" << actual_str << "]";
        }
        else
        {
            MyTimeBase actual_time;
            String actual_str, expect_str;
            result_formatter.format(*expected, expect_str);
            if (packed)
            {
                actual_time = MyTimeBase(*packed);
                result_formatter.format(actual_time, actual_str);
                EXPECT_EQ(*packed, expected->toPackedUInt())
                    << "[case=" << idx << "] "
                    << "[fmt=" << fmt << "] [input=" << input << "] [expect=" << expect_str
                    << "] [actual=" << actual_str << "]";
            }
            else
            {
                EXPECT_TRUE((bool)packed) //
                    << "[case=" << idx << "] "
                    << "[fmt=" << fmt << "] [input=" << input << "] [expect=" << expect_str
                    << "] [actual=<parse fail>]";
            }
        }
        idx++;
    }
}
CATCH

TEST_F(TestMyTime, ConvertTimeZone)
try
{
    const auto & time_zone_utc = DateLUT::instance("UTC");
    const auto & time_zone_sh = DateLUT::instance("Asia/Shanghai");
    std::vector<String> date_time_vec{
        "1970-01-01 00:00:01.00000",
        "1970-01-01 00:00:00.00000",
        "1969-12-31 01:00:00.00000",
        "1970-01-01 08:00:01.00000"};
    std::vector<UInt64> ref_value_vec{
        MyDateTime(1970, 1, 1, 8, 0, 1, 0).toPackedUInt(),
        0ULL,
        0ULL,
        0ULL,
        0ULL,
        0ULL,
        MyDateTime(1970, 1, 1, 16, 0, 1, 0).toPackedUInt(),
        MyDateTime(1970, 1, 1, 0, 0, 1, 0).toPackedUInt()};

    Int32 i = 0;
    for (const String & datetime : date_time_vec)
    {
        ReadBufferFromMemory read_buffer(datetime.c_str(), datetime.size());
        UInt64 origin_time_stamp = 0;
        tryReadMyDateTimeText(origin_time_stamp, 6, read_buffer);
        UInt64 converted_time = origin_time_stamp;
        {
            convertTimeZone(origin_time_stamp, converted_time, time_zone_utc, time_zone_sh);
            EXPECT_EQ(converted_time, ref_value_vec[i * 2]) << datetime;

            convertTimeZone(origin_time_stamp, converted_time, time_zone_sh, time_zone_utc);
            EXPECT_EQ(converted_time, ref_value_vec[i * 2 + 1]) << datetime;
        }
        i++;
    }
}
catch (Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    GTEST_FAIL();
}

} // namespace tests
} // namespace DB
