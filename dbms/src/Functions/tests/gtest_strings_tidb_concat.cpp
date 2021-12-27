#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class StringTidbConcat : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "tidbConcat";

    // FunctionTiDBConcat.useDefaultImplementationForNulls() = true, no need to test null.
    std::vector<String> test_strings = {"", "www.pingcap", "中文.测.试。。。"};
};

TEST_F(StringTidbConcat, OneArgTest)
try
{
    for (const auto & value : test_strings)
    {
        // column
        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({value}),
            executeFunction(
                StringTidbConcat::func_name,
                createColumn<Nullable<String>>({value})));
        // const
        ASSERT_COLUMN_EQ(
            createConstColumn<Nullable<String>>(1, value),
            executeFunction(
                StringTidbConcat::func_name,
                createConstColumn<Nullable<String>>(1, value)));
    }
}
CATCH

TEST_F(StringTidbConcat, TwoArgsTest)
try
{
    auto two_args_test = [&](const String & value1, const String & value2) {
        String result = value1 + value2;
        auto inner_test = [&](bool is_value1_const, bool is_value2_const) {
            auto is_result_const = is_value1_const && is_value2_const;
            ASSERT_COLUMN_EQ(
                is_result_const ? createConstColumn<Nullable<String>>(1, result) : createColumn<Nullable<String>>({result}),
                executeFunction(
                    StringTidbConcat::func_name,
                    is_value1_const ? createConstColumn<Nullable<String>>(1, value1) : createColumn<Nullable<String>>({value1}),
                    is_value2_const ? createConstColumn<Nullable<String>>(1, value2) : createColumn<Nullable<String>>({value2})));
        };
        std::vector<bool> is_consts = {true, false};
        for (const auto & is_value1_const : is_consts)
            for (const auto & is_value2_const : is_consts)
                inner_test(is_value1_const, is_value2_const);
    };

    for (const auto & value1 : test_strings)
        for (const auto & value2 : test_strings)
            two_args_test(value1, value2);
}
CATCH

TEST_F(StringTidbConcat, MoreArgsTest)
try
{
    auto more_args_test = [&](std::vector<String> values, const String & result) {
        ColumnsWithTypeAndName args;
        for (const auto & value : values)
            args.push_back(createColumn<Nullable<String>>({value}));

        ASSERT_COLUMN_EQ(
            createColumn<Nullable<String>>({result}),
            executeFunction(StringTidbConcat::func_name, args));
    };

    // 3
    more_args_test({"大家好", ",", "hello word"}, "大家好,hello word");
    // 4
    more_args_test({"这是中文", "C'est français", "これが日本の", "This is English"}, "这是中文C'est françaisこれが日本のThis is English");
    // 5
    more_args_test({"", "", "", "", ""}, "");
    // 100
    std::string unit = "a*b=c";
    std::string res;
    std::vector<String> values;
    for (size_t i = 0; i < 100; ++i)
    {
        values.push_back(unit);
        res += unit;
    }
    more_args_test(values, res);
}
CATCH

} // namespace DB::tests
