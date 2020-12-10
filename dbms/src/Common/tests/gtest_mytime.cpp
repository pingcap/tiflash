#include <Common/Exception.h>
#include <Common/MyTime.h>
#include <DataTypes/DataTypeMyDateTime.h>
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
            ASSERT_EQ(actual, expected) << "Original datetime string: " << str;
        }
        catch (...)
        {
            std::cerr << "Error occurs when parsing: \"" << str << "\"" << std::endl;
            throw;
        }
    }
};

TEST_F(TestMyTime, ParseMyDateTime)
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
    };
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
    DataTypeMyDateTime type_with_fraction(6);
    DataTypeMyDateTime type_without_fraction(0);
    for (auto & [str, expected] : cases_with_fsp)
    {
        checkParseMyDateTime(str, expected, type_with_fraction.getFraction());
    }
    for (auto & [str, expected] : cases_without_fsp)
    {
        checkParseMyDateTime(str, expected, type_without_fraction.getFraction());
    }
}
catch (Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    GTEST_FAIL();
}

} // namespace tests

} // namespace DB
