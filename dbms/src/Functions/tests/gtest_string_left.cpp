#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class StringLeftTest : public DB::tests::FunctionTest
{
public:
    // leftUTF8(str,len) = substrUTF8(str,const 1,len)
    static constexpr auto func_name = "substringUTF8";

    template <typename Integer>
    void testBoundary()
    {
        std::vector<std::optional<String>> strings = {"", "www.pingcap", "中文.测.试。。。", {}};
        for (const auto & str : strings)
        {
            auto return_null_if_str_null = [&](const std::optional<String> & not_null_result) {
                return !str.has_value() ? std::optional<String>{} : not_null_result;
            };
            test<Integer>(str, 0, return_null_if_str_null(""));
            test<Integer>(str, std::numeric_limits<Integer>::max(), return_null_if_str_null(str));
            test<Integer>(str, std::optional<Integer>{}, std::optional<String>{});
            if constexpr (std::is_signed_v<Integer>)
            {
                test<Integer>(str, -1, return_null_if_str_null(""));
                test<Integer>(str, std::numeric_limits<Integer>::min(), return_null_if_str_null(""));
            }
        }
    }

    template <typename Integer>
    void test(const std::optional<String> & str, const std::optional<Integer> & length, const std::optional<String> & result)
    {
        const auto start_column = createConstColumn<Int64>(1, 1);
        auto inner_test = [&](bool is_str_const, bool is_length_const) {
            bool is_one_of_args_null_const = (is_str_const && !str.has_value()) || (is_length_const && !length.has_value());
            bool is_result_const = (is_str_const && is_length_const) || is_one_of_args_null_const;
            auto expected_res_column = is_result_const ? createConstColumn<Nullable<String>>(1, result) : createColumn<Nullable<String>>({result});
            auto str_column = is_str_const ? createConstColumn<Nullable<String>>(1, str) : createColumn<Nullable<String>>({str});
            auto length_column = is_length_const ? createConstColumn<Nullable<Integer>>(1, length) : createColumn<Nullable<Integer>>({length});
            auto actual_res_column = executeFunction(func_name, str_column, start_column, length_column);
            ASSERT_COLUMN_EQ(expected_res_column, actual_res_column);
        };
        std::vector<bool> is_consts = {true, false};
        for (bool is_str_const : is_consts)
            for (bool is_length_const : is_consts)
                inner_test(is_str_const, is_length_const);
    }

    template <typename Integer>
    void testInvalidLengthType()
    {
        static_assert(!std::is_same_v<Integer, Int64> && !std::is_same_v<Integer, UInt64>);
        auto inner_test = [&](bool is_str_const, bool is_length_const) {
            ASSERT_THROW(
                executeFunction(
                    func_name,
                    is_str_const ? createConstColumn<Nullable<String>>(1, "") : createColumn<Nullable<String>>({""}),
                    createConstColumn<Int64>(1, 1),
                    is_length_const ? createConstColumn<Nullable<Integer>>(1, 0) : createColumn<Nullable<Integer>>({0})),
                Exception);
        };
        std::vector<bool> is_consts = {true, false};
        for (bool is_str_const : is_consts)
            for (bool is_length_const : is_consts)
                inner_test(is_str_const, is_length_const);
    }
};

TEST_F(StringLeftTest, testBoundary)
try
{
    testBoundary<Int64>();
    testBoundary<UInt64>();
}
CATCH

TEST_F(StringLeftTest, testMoreCases)
try
{
    // test big string
    // big_string.size() > length
    String big_string;
    // unit_string length = 22
    String unit_string = "big string is 我!!!!!!!";
    for (size_t i = 0; i < 1000; ++i)
        big_string += unit_string;
    test<Int64>(big_string, 22, unit_string);
    test<UInt64>(big_string, 22, unit_string);

    // test origin_str.size() == length
    String origin_str = "我的 size = 12";
    test<Int64>(origin_str, 12, origin_str);
    test<UInt64>(origin_str, 12, origin_str);

    // test origin_str.size() < length
    test<Int64>(origin_str, 22, origin_str);
    test<UInt64>(origin_str, 22, origin_str);

    // Mixed language
    String english_str = "This is English";
    String mixed_language_str = english_str + ",这是中文,C'est français,これが日本の";
    test<Int64>(mixed_language_str, english_str.size(), english_str);
    test<UInt64>(mixed_language_str, english_str.size(), english_str);

    // column size != 1
    // case 1
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({unit_string, origin_str, origin_str, english_str}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({big_string, origin_str, origin_str, mixed_language_str}),
            createConstColumn<Int64>(8, 1),
            createColumn<Nullable<Int64>>({22, 12, 22, english_str.size()})));
    // case 2
    String second_case_string = "abc";
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "a", "", "a", "", "", "a", "a"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({second_case_string, second_case_string, second_case_string, second_case_string, second_case_string, second_case_string, second_case_string, second_case_string}),
            createConstColumn<Int64>(8, 1),
            createColumn<Nullable<Int64>>({0, 1, 0, 1, 0, 0, 1, 1})));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "a", "", "a", "", "", "a", "a"}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<String>>(8, second_case_string),
            createConstColumn<Int64>(8, 1),
            createColumn<Nullable<Int64>>({0, 1, 0, 1, 0, 0, 1, 1})));
}
CATCH

TEST_F(StringLeftTest, testInvalidLengthType)
try
{
    testInvalidLengthType<Int8>();
    testInvalidLengthType<Int16>();
    testInvalidLengthType<Int32>();
    testInvalidLengthType<UInt8>();
    testInvalidLengthType<UInt16>();
    testInvalidLengthType<UInt32>();
}
CATCH

} // namespace tests
} // namespace DB
