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

class StringPosition : public ::testing::Test
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
TEST_F(StringPosition, str_and_fixed_str_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    // case insensitive
    std::vector<String> c0_var_strs{   "ell",     "LL",    "3",     "ElL",   "ye",    "aaaa",  "world" };
    std::vector<String> c1_var_strs{   "hello",   "HELLO", "23333", "HeLlO", "hey",   "a",     "WoRlD" };

    std::vector<String> c0_fixed_strs{ "ell",     "LLo",   "333",   "ElL",   "yye",   "aaa",   "orl"   };
    std::vector<String> c1_fixed_strs{ "hello",   "HELLO", "23333", "HeLlO", "heyyy", "aaaaa", "WoRlD" };

    // var-var
    std::vector<Int64> result0{2, 3, 2, 2, 0, 0, 1};
    // fixed-fixed
    std::vector<Int64> result1{2, 3, 2, 2, 0, 1, 2};
    // var-fixed
    std::vector<Int64> result2{2, 3, 2, 2, 0, 1, 1};
    // fixed-var
    std::vector<Int64> result3{2, 3, 2, 2, 0, 0, 2};

    std::vector<String> c0_strs;
    std::vector<String> c1_strs;
    std::vector<Int64> results;
    for (int i = 0; i < 4; i++)
    {
        MutableColumnPtr csp0;
        MutableColumnPtr csp1;
        if (i == 0)
        {
            // var-var
            c0_strs = c0_var_strs;
            c1_strs = c1_var_strs;
            results = result0;

            csp0 = ColumnString::create();
            csp1 = ColumnString::create();
        }
        else if (i == 1)
        {
            // fixed-fixed
            c0_strs = c0_fixed_strs;
            c1_strs = c1_fixed_strs;
            results = result1;
            
            csp0 = ColumnFixedString::create(3);
            csp1 = ColumnFixedString::create(5);
        }
        else if (i == 2)
        {
            // var-fixed
            c0_strs = c0_var_strs;
            c1_strs = c1_fixed_strs;
            results = result2;

            csp0 = ColumnString::create();
            csp1 = ColumnFixedString::create(5);
        }
        else
        {
            // fixed-var
            c0_strs = c0_fixed_strs;
            c1_strs = c1_var_strs;
            results = result3;

            csp0 = ColumnFixedString::create(3);
            csp1 = ColumnString::create();
        }
        
        for (size_t i = 0; i < c0_strs.size(); i++)
        {
            csp0->insert(Field(c0_strs[i].c_str(), c0_strs[i].size()));
            csp1->insert(Field(c1_strs[i].c_str(), c1_strs[i].size()));
        }

        Block testBlock;
        ColumnWithTypeAndName ctn0 = ColumnWithTypeAndName(std::move(csp0), std::make_shared<DataTypeString>(), "test_position_0");
        ColumnWithTypeAndName ctn1 = ColumnWithTypeAndName(std::move(csp1), std::make_shared<DataTypeString>(), "test_position_1");
        ColumnsWithTypeAndName ctns{ctn0, ctn1};
        testBlock.insert(ctn0);
        testBlock.insert(ctn1);
        // for result from position
        testBlock.insert({});
        ColumnNumbers cns{0, 1};

        // test position
        auto bp = factory.tryGet("position", context);
        ASSERT_TRUE(bp != nullptr);
        ASSERT_FALSE(bp->isVariadic());

        bp->build(ctns)->execute(testBlock, cns, 2);
        const IColumn * res = testBlock.getByPosition(2).column.get();
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

// test string and fixed string in utf8
TEST_F(StringPosition, utf8_str_and_fixed_str_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    // case insensitive
    std::vector<String> c0_var_strs{   "好",     "平凯",      "aa哈",       "？！",      "呵呵呵",  "233",      "嗯？？" };
    std::vector<String> c1_var_strs{   "ni好",   "平凯星辰",  "啊啊aaa哈哈", "？？！！",  "呵呵呵",  "哈哈2333",  "嗯？" };

    std::vector<String> c0_fixed_strs{ "好",     "凯",        "哈",         "！",        "嗯",      "二",       "？"   };
    std::vector<String> c1_fixed_strs{ "好de_",  "平凯",      "aaa哈",      "！？",      "嗯嗯",     "2二33",    "？？" };

    // var-var
    std::vector<Int64> result0{3, 1, 4, 2, 1, 3, 0};
    // fixed-fixed
    std::vector<Int64> result1{1, 2, 4, 1, 1, 2, 1};
    // var-fixed
    std::vector<Int64> result2{1, 1, 2, 0, 0, 0, 0};
    // fixed-var
    std::vector<Int64> result3{3, 2, 6, 3, 0, 0, 2};

    std::vector<String> c0_strs;
    std::vector<String> c1_strs;
    std::vector<Int64> results;
    for (int i = 0; i < 4; i++)
    {
        MutableColumnPtr csp0;
        MutableColumnPtr csp1;
        if (i == 0)
        {
            // var-var
            c0_strs = c0_var_strs;
            c1_strs = c1_var_strs;
            results = result0;

            csp0 = ColumnString::create();
            csp1 = ColumnString::create();
        }
        else if (i == 1)
        {
            // fixed-fixed
            c0_strs = c0_fixed_strs;
            c1_strs = c1_fixed_strs;
            results = result1;
            
            csp0 = ColumnFixedString::create(3);
            csp1 = ColumnFixedString::create(6);
        }
        else if (i == 2)
        {
            // var-fixed
            c0_strs = c0_var_strs;
            c1_strs = c1_fixed_strs;
            results = result2;

            csp0 = ColumnString::create();
            csp1 = ColumnFixedString::create(6);
        }
        else
        {
            // fixed-var
            c0_strs = c0_fixed_strs;
            c1_strs = c1_var_strs;
            results = result3;

            csp0 = ColumnFixedString::create(3);
            csp1 = ColumnString::create();
        }
        
        for (size_t i = 0; i < c0_strs.size(); i++)
        {
            csp0->insert(Field(c0_strs[i].c_str(), c0_strs[i].size()));
            csp1->insert(Field(c1_strs[i].c_str(), c1_strs[i].size()));
        }

        Block testBlock;
        ColumnWithTypeAndName ctn0 = ColumnWithTypeAndName(std::move(csp0), std::make_shared<DataTypeString>(), "test_position_0");
        ColumnWithTypeAndName ctn1 = ColumnWithTypeAndName(std::move(csp1), std::make_shared<DataTypeString>(), "test_position_1");
        ColumnsWithTypeAndName ctns{ctn0, ctn1};
        testBlock.insert(ctn0);
        testBlock.insert(ctn1);
        // for result from position
        testBlock.insert({});
        ColumnNumbers cns{0, 1};

        // test position
        auto bp = factory.tryGet("position", context);
        ASSERT_TRUE(bp != nullptr);
        ASSERT_FALSE(bp->isVariadic());

        bp->build(ctns)->execute(testBlock, cns, 2);
        const IColumn * res = testBlock.getByPosition(2).column.get();
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
