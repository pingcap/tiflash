#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Functions/registerFunctions.h>
#include <Interpreters/Context.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>

#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{

class StringLength : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        try
        {
            registerFunctions();
        }
        catch (DB::Exception &)
        {
            // Maybe another test has already registed, ignore exception here.
        }
    }
};

// test string and fixed string
TEST_F(StringLength, str_and_fixed_str_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> fixed_strs{"hello", "HELLO", "23333", "#%@#^", "ni好"};
    std::vector<Int64> fixed_strs_results{5, 5, 5, 5, 5};

    std::vector<String> strs{"hi~", "23333", "pingcap", "你好", "233哈哈"};
    std::vector<Int64> results{3, 5, 7, 6, 9};

    for (int i = 0; i < 2; i++)
    {
        MutableColumnPtr csp;
        if (i == 0)
            csp = ColumnString::create();
        else
        {
            strs = fixed_strs;
            results = fixed_strs_results;
            csp = ColumnFixedString::create(5);
        }
        
        for (const auto & str : strs)
        {
            csp->insert(Field(str.c_str(), str.size()));
        }

        Block testBlock;
        ColumnWithTypeAndName ctn = ColumnWithTypeAndName(std::move(csp), std::make_shared<DataTypeString>(), "test_ascii");
        ColumnsWithTypeAndName ctns{ctn};
        testBlock.insert(ctn);
        // for result from length
        testBlock.insert({});
        ColumnNumbers cns{0};

        // test length
        auto bp = factory.tryGet("length", context);
        ASSERT_TRUE(bp != nullptr);
        ASSERT_FALSE(bp->isVariadic());

        bp->build(ctns)->execute(testBlock, cns, 1);
        const IColumn * res = testBlock.getByPosition(1).column.get();
        const ColumnInt64 * res_string = checkAndGetColumn<ColumnInt64>(res);

        Field resField;
        
        for (size_t t = 0; t < results.size(); t++)
        {
            res_string->get(t, resField);
            Int64 res_val = resField.get<Int64>();
            EXPECT_EQ(results[t], res_val);
        }
    }
}

} // namespace tests
} // namespace DB
