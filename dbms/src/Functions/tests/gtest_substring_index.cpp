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
class SubstringIndexTest : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "substringIndex";

    template <typename Integer>
    void testBoundary()
    {
        std::vector<std::optional<String>> strings = {"", "www.pingcap", "中文.测.试。。。", {}};
        std::vector<std::optional<String>> delims = {"", ".", "i", "测", {}};
        std::vector<std::vector<std::optional<String>>> positive_one_results = {
            {"", "", "", "", {}},
            {"", "www", "www.p", "www.pingcap", {}},
            {"", "中文", "中文.测.试。。。", "中文.", {}},
            {{}, {}, {}, {}, {}}};
        std::vector<std::vector<std::optional<String>>> negative_one_results = {
            {"", "", "", "", {}},
            {"", "pingcap", "ngcap", "www.pingcap", {}},
            {"", "试。。。", "中文.测.试。。。", ".试。。。", {}},
            {{}, {}, {}, {}, {}}};
        for (size_t str_i = 0; str_i < strings.size(); ++str_i)
        {
            const auto & str = strings[str_i];
            for (size_t delim_i = 0; delim_i < strings.size(); ++delim_i)
            {
                const auto & delim = delims[delim_i];
                auto return_null_if_str_or_delim_null = [&](const std::optional<String> & not_null_result) {
                    return !str.has_value() || !delim.has_value() ? std::optional<String>{} : not_null_result;
                };
                auto return_blank_if_delim_blank = [&](const std::optional<String> & not_null_result) {
                    return delim.has_value() && delim.value().empty() ? "" : not_null_result;
                };
                test<Integer>(str, delim, 0, return_null_if_str_or_delim_null(""));
                test<Integer>(str, delim, 1, return_null_if_str_or_delim_null(positive_one_results[str_i][delim_i]));
                test<Integer>(str, delim, std::numeric_limits<Integer>::max(), return_null_if_str_or_delim_null(return_blank_if_delim_blank(str)));
                test<Integer>(str, delim, std::optional<Integer>{}, std::optional<String>{});
                if constexpr (std::is_signed_v<Integer>)
                {
                    test<Integer>(str, delim, -1, return_null_if_str_or_delim_null(negative_one_results[str_i][delim_i]));
                    test<Integer>(str, delim, std::numeric_limits<Integer>::min(), return_null_if_str_or_delim_null(return_blank_if_delim_blank(str)));
                }
            }
        }
    }

    template <typename Integer>
    void test(const std::optional<String> & str, std::optional<String> delim, const std::optional<Integer> & count, const std::optional<String> & result)
    {
        auto inner_test = [&](bool is_str_const, bool is_delim_const, bool is_count_const) {
            bool is_one_of_args_null_const = (is_str_const && !str.has_value()) || (is_delim_const && !delim.has_value()) || (is_count_const && !count.has_value());
            bool is_result_const = (is_str_const && is_delim_const && is_count_const) || is_one_of_args_null_const;
            if (is_result_const && !is_one_of_args_null_const && !result.has_value())
                throw Exception("Should not reach here");
            auto expected_res_column = is_result_const ? (is_one_of_args_null_const ? createConstColumn<Nullable<String>>(1, result) : createConstColumn<String>(1, result.value())) : createColumn<Nullable<String>>({result});
            auto str_column = is_str_const ? createConstColumn<Nullable<String>>(1, str) : createColumn<Nullable<String>>({str});
            auto delim_column = is_delim_const ? createConstColumn<Nullable<String>>(1, delim) : createColumn<Nullable<String>>({delim});
            auto count_column = is_count_const ? createConstColumn<Nullable<Integer>>(1, count) : createColumn<Nullable<Integer>>({count});
            auto actual_res_column = executeFunction(func_name, str_column, delim_column, count_column);
            ASSERT_COLUMN_EQ(expected_res_column, actual_res_column);
        };
        std::vector<bool> is_consts = {true, false};
        for (bool is_str_const : is_consts)
            for (bool is_delim_const : is_consts)
                for (bool is_count_const : is_consts)
                    inner_test(is_str_const, is_delim_const, is_count_const);
    }
};

TEST_F(SubstringIndexTest, testBoundary)
try
{
    testBoundary<UInt8>();
    testBoundary<UInt16>();
    testBoundary<UInt32>();
    testBoundary<UInt64>();
    testBoundary<Int8>();
    testBoundary<Int16>();
    testBoundary<Int32>();
    testBoundary<Int64>();
}
CATCH

TEST_F(SubstringIndexTest, testMoreCases)
try
{
    // Test string
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"www.pingcap", "pingcap.com", "", "www.pingcap.com"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(
                {"www.pingcap.com", "www.pingcap.com", "www.pingcap.com", "www.pingcap.com"}),
            createColumn<Nullable<String>>({".", ".", ".", "."}),
            createColumn<Nullable<Int64>>({2, -2, 0, 10})));
    // Test Null
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({{}, {}, {}, "www.pingcap.com"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(
                {{}, "www.pingcap.com", "www.pingcap.com", "www.pingcap.com"}),
            createColumn<Nullable<String>>({".", {}, ".", "."}),
            createColumn<Nullable<Int64>>({2, -2, {}, 10})));
    // More Test 1
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"pingcap.com", "www", "www.", ".com", "pingcap.com", ".com"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(
                {"www.pingcap.com", "www.pingcap.com", "www.pingcap.com", "www.pingcap.com", ".www.pingcap.com", ".www.pingcap.com"}),
            createColumn<Nullable<String>>({".", ".", "pingcap", "pingcap", ".", ".pingcap"}),
            createColumn<Nullable<Int64>>({-2, 1, 1, -1, -2, -1})));
    // More Test 2
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "", "aa", "aaaa", "aaaaaa", "aaaaaaaaa1"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>(
                {"aaaaaaaaa1", "aaaaaaaaa1", "aaaaaaaaa1", "aaaaaaaaa1", "aaaaaaaaa1", "aaaaaaaaa1"}),
            createColumn<Nullable<String>>({"a", "aa", "aa", "aa", "aa", "aa"}),
            createColumn<Nullable<Int64>>({1, 1, 2, 3, 4, 5})));
    // More Test 3
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "aaa", "aaaaaa", "aaaaaaaaa1", "", "aaaa"}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<String>>(6, "aaaaaaaaa1"),
            createColumn<Nullable<String>>({"aaa", "aaa", "aaa", "aaa", "aaaa", "aaaa"}),
            createColumn<Nullable<Int64>>({1, 2, 3, 4, 1, 2})));
    // More Test 4
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"aaaaaaaaa", "1", "a1", "aaa1", "aaaaa1", "aaaaaaa1", "aaaaaaaaa1"}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<String>>(7, "aaaaaaaaa1"),
            createColumn<Nullable<String>>({"1", "a", "aa", "aa", "aa", "aa", "aa"}),
            createColumn<Nullable<Int64>>({1, -1, -1, -2, -3, -4, -5})));
    // More Test 5
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"1", "aaa1", "aaaaaa1", "aaaaaaaaa1"}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<String>>(4, "aaaaaaaaa1"),
            createColumn<Nullable<String>>({"aaa", "aaa", "aaa", "aaa"}),
            createColumn<Nullable<Int64>>({-1, -2, -3, -4})));

    // Test Uint64
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"aaa.bbb.ccc.ddd.eee.fff", "aaa.bbb.ccc.ddd.eee.fff", "aaa.bbb.ccc.ddd.eee.fff", "aaa.bbb.ccc.ddd.eee.fff"}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<String>>(4, "aaa.bbb.ccc.ddd.eee.fff"),
            createColumn<Nullable<String>>({".", ".", ".", "."}),
            createColumn<Nullable<UInt64>>({18446744073709551615llu, 18446744073709551614llu, 18446744073709551613llu, 18446744073709551612llu})));

    // Test Int8
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"begin" + std::string(INT8_MAX - 1, '.'), "begin" + std::string(INT8_MAX - 2, '.'), std::string(INT8_MAX, '.') + "end", std::string(INT8_MAX - 1, '.') + "end"}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<String>>(4, "begin" + std::string(300, '.') + "end"),
            createColumn<Nullable<String>>({".", ".", ".", "."}),
            createColumn<Nullable<Int8>>({INT8_MAX, INT8_MAX - 1, INT8_MIN, INT8_MIN + 1})));

    // Test UInt8
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"begin" + std::string(UINT8_MAX - 1, '.'), "begin" + std::string(UINT8_MAX - 2, '.')}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<String>>(2, "begin" + std::string(300, '.') + "end"),
            createColumn<Nullable<String>>({".", "."}),
            createColumn<Nullable<UInt8>>({UINT8_MAX, UINT8_MAX - 1})));

    // Test Int64
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"ping.cap", "ping.cap", "ping.cap", "ping.cap"}),
        executeFunction(
            func_name,
            createConstColumn<Nullable<String>>(4, "ping.cap"),
            createColumn<Nullable<String>>({".", ".", ".", "."}),
            createColumn<Nullable<Int64>>({INT64_MAX, INT64_MAX - 1, INT64_MIN, INT64_MIN + 1})));

    // Test vector_const_const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"www.pingcap", "www.", "中文.测", "www.www"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"www.pingcap.com", "www...www", "中文.测.试。。。", "www.www"}),
            createConstColumn<Nullable<String>>(4, "."),
            createConstColumn<Nullable<Int64>>(4, 2)));

    // Test type/nullable/const
    auto data_string_col = createColumn<String>({"www.pingcap.com", "www.pingcap.com", "www.pingcap.com", "www.pingcap.com"});
    auto data_nullable_string_col = createColumn<Nullable<String>>({"www.pingcap.com", "www.pingcap.com", "www.pingcap.com", "www.pingcap.com"});
    auto data_const_col = createConstColumn<String>(4, "www.pingcap.com");

    auto delim_string_col = createColumn<String>({".", ".", ".", "."});
    auto delim_nullable_string_col = createColumn<Nullable<String>>({".", ".", ".", "."});
    auto delim_const_col = createConstColumn<String>(4, ".");

    auto count_int64_col = createColumn<Int64>({2, 2, 2, 2});
    auto count_nullable_int64_col = createColumn<Nullable<Int64>>({2, 2, 2, 2});
    auto count_const_col = createConstColumn<Int64>(4, 2);

    auto result_string_col = createColumn<String>({"www.pingcap", "www.pingcap", "www.pingcap", "www.pingcap"});
    auto result_nullable_string_col = createColumn<Nullable<String>>({"www.pingcap", "www.pingcap", "www.pingcap", "www.pingcap"});
    auto result_const_col = createConstColumn<String>(4, "www.pingcap");
    // Test type, type, type/nullable/const
    ASSERT_COLUMN_EQ(result_string_col, executeFunction(func_name, data_string_col, delim_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_string_col, delim_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_string_col, executeFunction(func_name, data_string_col, delim_string_col, count_const_col));
    // Test type, nullable, type/nullable/const
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_string_col, delim_nullable_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_string_col, delim_nullable_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_string_col, delim_nullable_string_col, count_const_col));
    // Test type, const, type/nullable/const
    ASSERT_COLUMN_EQ(result_string_col, executeFunction(func_name, data_string_col, delim_const_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_string_col, delim_const_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_string_col, executeFunction(func_name, data_string_col, delim_const_col, count_const_col));

    // Test nullable, type, type/nullable/const
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_nullable_string_col, delim_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_nullable_string_col, delim_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_nullable_string_col, delim_string_col, count_const_col));
    // Test nullable, nullable, type/nullable/const
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_nullable_string_col, delim_nullable_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_nullable_string_col, delim_nullable_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_nullable_string_col, delim_nullable_string_col, count_const_col));
    // Test nullable, const, type/nullable/const
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_nullable_string_col, delim_const_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_nullable_string_col, delim_const_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_nullable_string_col, delim_const_col, count_const_col));

    // Test const, type, type/nullable/const
    ASSERT_COLUMN_EQ(result_string_col, executeFunction(func_name, data_const_col, delim_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_const_col, delim_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_string_col, executeFunction(func_name, data_const_col, delim_string_col, count_const_col));
    // Test const, nullable, type/nullable/const
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_const_col, delim_nullable_string_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_const_col, delim_nullable_string_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_const_col, delim_nullable_string_col, count_const_col));
    // Test const, const, type/nullable/const
    ASSERT_COLUMN_EQ(result_string_col, executeFunction(func_name, data_const_col, delim_const_col, count_int64_col));
    ASSERT_COLUMN_EQ(result_nullable_string_col, executeFunction(func_name, data_const_col, delim_const_col, count_nullable_int64_col));
    ASSERT_COLUMN_EQ(result_const_col, executeFunction(func_name, data_const_col, delim_const_col, count_const_col));

    // Test vector, empty const, const
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "", "", ""}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"www.pingcap.com", "www...www", "中文.测.试。。。", "www.www"}),
            createConstColumn<Nullable<String>>(4, ""),
            createConstColumn<Nullable<Int64>>(4, 2)));

    // Test vector, empty vector, vector
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"", "", "", ""}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"www.pingcap.com", "www...www", "中文.测.试。。。", "www.www"}),
            createColumn<Nullable<String>>({"", "", "", ""}),
            createColumn<Nullable<Int64>>({2, 2, 2, 2})));

    // Test issue 9116
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"aaabbba", "aaabbbaa", "aaabbbaaa", "aaabbbaaa", "aaabbbaaa"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"aaabbbaaa", "aaabbbaaa", "aaabbbaaa", "aaabbbaaa", "aaabbbaaa"}),
            createColumn<Nullable<String>>({"a", "a", "a", "a", "a"}),
            createColumn<Nullable<Int64>>({5, 6, 7, 8, 9})));
}
CATCH

} // namespace tests
} // namespace DB