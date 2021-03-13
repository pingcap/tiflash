#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDateTime.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>

#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>
#include <gtest/gtest.h>

#pragma GCC diagnostic pop

namespace DB
{

Context * ctx;


class TestDateTimeExtract : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        ctx = new Context(Context::createGlobal());
        registerFunctions();
    }
};

TEST_F(TestDateTimeExtract, ExtractFromString)
try
{
    Context context = *ctx;

    auto & factory = FunctionFactory::instance();

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
    String datetime_value{"2021/1/29 12:34:56.123456"};
    std::vector<Int64> results{2021, 1, 1, 4, 29, 29123456123456, 29123456, 291234, 2912, 202101};

    for (size_t i = 0; i < units.size(); ++i)
    {
        const auto & unit = units[i];
        Block block;

        MutableColumnPtr col_units = ColumnString::create();
        col_units->insert(Field(unit.c_str(), unit.size()));
        col_units = ColumnConst::create(col_units->getPtr(), 1);

        auto col_datetime = ColumnString::create();
        {
            col_datetime->insert(Field(datetime_value.data(), datetime_value.size()));
        }
        ColumnWithTypeAndName unit_ctn = ColumnWithTypeAndName(std::move(col_units), std::make_shared<DataTypeString>(), "unit");
        ColumnWithTypeAndName datetime_ctn
            = ColumnWithTypeAndName(std::move(col_datetime), std::make_shared<DataTypeString>(), "datetime_value");
        ColumnsWithTypeAndName ctns{unit_ctn, datetime_ctn};
        block.insert(unit_ctn);
        // for result from extract
        block.insert(datetime_ctn);
        block.insert({});

        ColumnNumbers cns{0, 1};

        // test extract
        auto bp = factory.tryGet("extractMyDateTime", context);
        ASSERT_TRUE(bp != nullptr);

        bp->build(ctns)->execute(block, cns, 2);
        const IColumn * res = block.getByPosition(2).column.get();
        const ColumnInt64 * col_res = checkAndGetColumn<ColumnInt64>(res);

        Field resField;
        col_res->get(0, resField);
        Int64 s = resField.get<Int64>();
        EXPECT_EQ(results[i], s);
    }
}
catch (const Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    GTEST_FAIL();
}
catch (const std::exception & e)
{
    std::cerr << e.what() << std::endl;
    GTEST_FAIL();
}
catch (...)
{
    throw;
}

TEST_F(TestDateTimeExtract, ExtractFromMyDateTime)
try
{
    Context context = *ctx;

    auto & factory = FunctionFactory::instance();

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
        Block block;

        MutableColumnPtr col_units = ColumnString::create();
        col_units->insert(Field(unit.c_str(), unit.size()));
        col_units = ColumnConst::create(col_units->getPtr(), 1);

        auto col_datetime = ColumnUInt64::create();
        {
            col_datetime->insert(Field(datetime_value.toPackedUInt()));
        }
        ColumnWithTypeAndName unit_ctn = ColumnWithTypeAndName(std::move(col_units), std::make_shared<DataTypeString>(), "unit");
        ColumnWithTypeAndName datetime_ctn
            = ColumnWithTypeAndName(std::move(col_datetime), std::make_shared<DataTypeMyDateTime>(), "datetime_value");
        ColumnsWithTypeAndName ctns{unit_ctn, datetime_ctn};
        block.insert(unit_ctn);
        // for result from extract
        block.insert(datetime_ctn);
        block.insert({});

        ColumnNumbers cns{0, 1};

        // test extract
        auto bp = factory.tryGet("extractMyDateTime", context);
        ASSERT_TRUE(bp != nullptr);

        bp->build(ctns)->execute(block, cns, 2);
        const IColumn * res = block.getByPosition(2).column.get();
        const ColumnInt64 * col_res = checkAndGetColumn<ColumnInt64>(res);

        Field resField;
        col_res->get(0, resField);
        Int64 s = resField.get<Int64>();
        EXPECT_EQ(results[i], s);
    }
}
catch (const Exception & e)
{
    std::cerr << e.displayText() << std::endl;
    GTEST_FAIL();
}
catch (const std::exception & e)
{
    std::cerr << e.what() << std::endl;
    GTEST_FAIL();
}
catch (...)
{
    throw;
}

} // namespace DB
