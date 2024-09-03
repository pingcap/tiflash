#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

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
class StringPosition : public DB::tests::FunctionTest
{
};

// test string and fixed string
TEST_F(StringPosition, str_and_fixed_str_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    // case insensitive
    std::vector<String> c0_var_strs{"ell", "LL", "3", "ElL", "ye", "aaaa", "world", "", "", "biu"};
    std::vector<String> c1_var_strs{"hello", "HELLO", "23333", "HeLlO", "hey", "a", "WoRlD", "", "ping", ""};

    std::vector<String> c0_fixed_strs{"ell", "LLo", "333", "ElL", "yye", "aaa", "orl", "321", "xzx", "biu"};
    std::vector<String> c1_fixed_strs{"hello", "HELLO", "23333", "HeLlO", "heyyy", "aaaaa", "WoRlD", "12345", "apple", "b_i_u"};

    // var-var
    std::vector<Int64> result0{2, 3, 2, 0, 0, 0, 0, 1, 1, 0};
    // fixed-fixed
    std::vector<Int64> result1{2, 0, 2, 0, 0, 1, 0, 0, 0, 0};
    // var-fixed
    std::vector<Int64> result2{2, 3, 2, 0, 0, 1, 0, 1, 1, 0};
    // fixed-var
    std::vector<Int64> result3{2, 0, 2, 0, 0, 0, 0, 0, 0, 0};

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

        for (size_t j = 0; j < c0_strs.size(); j++)
        {
            csp0->insert(Field(c0_strs[j].c_str(), c0_strs[j].size()));
            csp1->insert(Field(c1_strs[j].c_str(), c1_strs[j].size()));
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

<<<<<<< HEAD
        bp->build(ctns)->execute(testBlock, cns, 2);
        const IColumn * res = testBlock.getByPosition(2).column.get();
        const ColumnInt64 * res_string = checkAndGetColumn<ColumnInt64>(res);
=======
        bp->build(ctns)->execute(test_block, cns, 2);
        const IColumn * res = test_block.getByPosition(2).column.get();
        const auto * res_string = checkAndGetColumn<ColumnInt64>(res);
>>>>>>> b30c1f5090 (Improve the performance of `length` and `ascii` functions (#9345))

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
    std::vector<String> c0_var_strs{"好", "平凯", "aa哈", "？！", "呵呵呵", "233", "嗯？？"};
    std::vector<String> c1_var_strs{"ni好", "平凯星辰", "啊啊aaa哈哈", "？？！！", "呵呵呵", "哈哈2333", "嗯？"};

    std::vector<String> c0_fixed_strs{"好", "凯", "哈", "！", "嗯", "二", "？"};
    std::vector<String> c1_fixed_strs{"好de_", "平凯", "aaa哈", "！？", "嗯嗯", "2二33", "？？"};

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

<<<<<<< HEAD
        bp->build(ctns)->execute(testBlock, cns, 2);
        const IColumn * res = testBlock.getByPosition(2).column.get();
        const ColumnInt64 * res_string = checkAndGetColumn<ColumnInt64>(res);
=======
        bp->build(ctns)->execute(test_block, cns, 2);
        const IColumn * res = test_block.getByPosition(2).column.get();
        const auto * res_string = checkAndGetColumn<ColumnInt64>(res);
>>>>>>> b30c1f5090 (Improve the performance of `length` and `ascii` functions (#9345))

        Field resField;

        for (size_t t = 0; t < results.size(); t++)
        {
            res_string->get(t, resField);
            Int64 res_val = resField.get<Int64>();
            EXPECT_EQ(results[t], res_val);
        }
    }
}

// test NULL
TEST_F(StringPosition, null_Test)
{
    const Context context = TiFlashTestEnv::getContext();

    auto & factory = FunctionFactory::instance();

    std::vector<String> c0_strs{"aa", "c", "香锅", "cap", "f"};
    std::vector<String> c1_strs{"a", "cc", "麻辣v香锅", "pingcap", "f"};
    std::vector<Int64> results{0, 0, 4, 5, 0};

    std::vector<int> c0_null_map{0, 1, 0, 0, 1};
    std::vector<int> c1_null_map{1, 0, 0, 0, 1};
    std::vector<int> c2_null_map{1, 1, 0, 0, 1}; // for result

    auto input_str_col0 = ColumnString::create();
    auto input_str_col1 = ColumnString::create();
    for (size_t i = 0; i < c0_strs.size(); i++)
    {
        Field field0(c0_strs[i].c_str(), c0_strs[i].size());
        Field field1(c1_strs[i].c_str(), c1_strs[i].size());
        input_str_col0->insert(field0);
        input_str_col1->insert(field1);
    }

    auto input_null_map0 = ColumnUInt8::create(c0_strs.size(), 0);
    auto input_null_map1 = ColumnUInt8::create(c1_strs.size(), 0);
    ColumnUInt8::Container & input_vec_null_map0 = input_null_map0->getData();
    ColumnUInt8::Container & input_vec_null_map1 = input_null_map1->getData();
    for (size_t i = 0; i < c0_null_map.size(); i++)
    {
        input_vec_null_map0[i] = c0_null_map[i];
        input_vec_null_map1[i] = c1_null_map[i];
    }

    DataTypePtr string_type = std::make_shared<DataTypeString>();
    DataTypePtr nullable_string_type = makeNullable(string_type);

    auto input_null_col0 = ColumnNullable::create(std::move(input_str_col0), std::move(input_null_map0));
    auto input_null_col1 = ColumnNullable::create(std::move(input_str_col1), std::move(input_null_map1));
    auto col0 = ColumnWithTypeAndName(std::move(input_null_col0), nullable_string_type, "position");
    auto col1 = ColumnWithTypeAndName(std::move(input_null_col1), nullable_string_type, "position");
    ColumnsWithTypeAndName ctns{col0, col1};

    Block testBlock;
    testBlock.insert(col0);
    testBlock.insert(col1);
    ColumnNumbers cns{0, 1};

    auto bp = factory.tryGet("position", context);
    ASSERT_TRUE(bp != nullptr);
    ASSERT_FALSE(bp->isVariadic());
    auto func = bp->build(ctns);
    testBlock.insert({nullptr, func->getReturnType(), "res"});
    func->execute(testBlock, cns, 2);
    auto res_col = testBlock.getByPosition(2).column;

    ColumnPtr result_null_map_column = static_cast<const ColumnNullable &>(*res_col).getNullMapColumnPtr();
    MutableColumnPtr mutable_result_null_map_column = (*std::move(result_null_map_column)).mutate();
    NullMap & result_null_map = static_cast<ColumnUInt8 &>(*mutable_result_null_map_column).getData();
<<<<<<< HEAD
    const IColumn * res = testBlock.getByPosition(2).column.get();
    const ColumnNullable * res_nullable_string = checkAndGetColumn<ColumnNullable>(res);
=======
    const IColumn * res = test_block.getByPosition(2).column.get();
    const auto * res_nullable_string = checkAndGetColumn<ColumnNullable>(res);
>>>>>>> b30c1f5090 (Improve the performance of `length` and `ascii` functions (#9345))
    const IColumn & res_string = res_nullable_string->getNestedColumn();

    Field resField;

    for (size_t i = 0; i < c2_null_map.size(); i++)
    {
        EXPECT_EQ(result_null_map[i], c2_null_map[i]);
        if (c2_null_map[i] == 0)
        {
            res_string.get(i, resField);
            Int64 res_val = resField.get<Int64>();
            EXPECT_EQ(results[i], res_val);
        }
    }
}

} // namespace tests
} // namespace DB
