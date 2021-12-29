#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <limits>
#include <string>
#include <vector>

namespace DB::tests
{
class StringRight : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "rightUTF8";

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
    void test(const std::optional<String> & str, std::optional<Integer> length, const std::optional<String> & result)
    {
        auto inner_test = [&](bool is_str_const, bool is_length_const) {
            // for const length <= 0 and not null str, return const blank string.
//            bool is_const_zero_length = str.has_value() && length.has_value() && length.value_or(0) <= 0;
//            bool is_result_const = is_length_const && (is_str_const || is_const_zero_length);
            bool is_result_const = is_length_const && is_str_const;
            ASSERT_COLUMN_EQ(
                is_result_const ? createConstColumn<Nullable<String>>(1, result) : createColumn<Nullable<String>>({result}),
                executeFunction(
                    StringRight::func_name,
                    is_str_const ? createConstColumn<Nullable<String>>(1, str) : createColumn<Nullable<String>>({str}),
                    is_length_const ? createConstColumn<Nullable<Integer>>(1, length) : createColumn<Nullable<Integer>>({length})));
        };
        std::vector<bool> is_consts = {true, false};
        for (bool is_str_const : is_consts)
            for (bool is_length_const : is_consts)
                inner_test(is_str_const, is_length_const);
    }
};

TEST_F(StringRight, UnitTest)
try
{
//    testBoundary<Int64>();
//    testBoundary<UInt64>();
//
//    // test big string
//    // big_string.size() > length
//    String big_string;
//    String unit_string = "big string is 我!!!!!!!";
//    for (size_t i = 0; i < 1000; ++i)
//        big_string += unit_string;
//    test<Int64>(big_string, 22, unit_string);
//    test<UInt64>(big_string, 22, unit_string);
//
//    // test origin_str.size() == length
//    String origin_str = "我的 size = 12";
//    test<Int64>(origin_str, origin_str.size(), origin_str);
//    test<UInt64>(origin_str, origin_str.size(), origin_str);
//
////    // test origin_str.size() < length
//    test<Int64>(origin_str, origin_str.size() + 10, origin_str);
//    test<UInt64>(origin_str, origin_str.size() + 10, origin_str);


ASSERT_COLUMN_EQ(
    createColumn<Nullable<String>>({""}),
    executeFunction(
        StringRight::func_name,
        createColumn<Nullable<String>>({"abc"}),
        createColumn<Nullable<Int64>>({0})));
}
CATCH

} // namespace DB::tests
