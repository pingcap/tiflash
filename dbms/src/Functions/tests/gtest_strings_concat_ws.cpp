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
#include <Flash/Coprocessor/DAGContext.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <fmt/format.h>

#include <string>
#include <vector>

namespace DB::tests
{
class StringTiDBConcatWS : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "tidbConcatWS";

    using Type = Nullable<String>;

    std::vector<String> not_null_strings = {"", "www.pingcap", "中文.测.试。。。"};
    std::vector<ColumnWithTypeAndName> nulls
        = {createOnlyNullColumnConst(1),
           createOnlyNullColumn(1),
           createColumn<Type>({{}}),
           createConstColumn<Type>(1, {})};
    std::vector<ColumnWithTypeAndName> all_inputs = nulls;
    StringTiDBConcatWS()
    {
        for (const auto & value : not_null_strings)
        {
            all_inputs.push_back(toConst(value));
            all_inputs.push_back(toColumn(value));
        }
    }

    static ColumnWithTypeAndName toColumn(std::optional<String> value) { return createColumn<Type>({value}); }

    static ColumnWithTypeAndName toConst(std::optional<String> value) { return createConstColumn<Type>(1, value); }

    static bool isConst(const ColumnWithTypeAndName & value) { return value.column->isColumnConst(); }

    static bool isNull(const ColumnWithTypeAndName & value)
    {
        return value.type->onlyNull() || value.column->onlyNull() || (*value.column)[0].isNull();
    }

    static String getNotNullValue(const ColumnWithTypeAndName & value) { return (*value.column)[0].get<String>(); }
};

TEST_F(StringTiDBConcatWS, TwoArgsTest)
try
{
    auto test_separator_is_null = [&](const ColumnWithTypeAndName & null_separator) {
        for (const auto & value : all_inputs)
        {
            // separator is null, result is null.
            auto res = null_separator.type->onlyNull()
                ? createOnlyNullColumnConst(1)
                : (isConst(null_separator) && isConst(value) ? toConst({}) : toColumn({}));
            ASSERT_COLUMN_EQ(res, executeFunction(StringTiDBConcatWS::func_name, null_separator, value));
        }
    };
    for (const auto & item : nulls)
        test_separator_is_null(item);


    auto test_separator_is_not_null = [&](const ColumnWithTypeAndName & value) {
        for (const auto & separator : not_null_strings)
        {
            auto separator_not_null_test = [&](bool is_separator_const) {
                auto separator_column = is_separator_const ? toConst(separator) : toColumn(separator);
                auto res = isNull(value) ? "" : getNotNullValue(value);
                bool is_result_const = is_separator_const && isConst(value);
                auto res_column = is_result_const ? toConst(res) : toColumn(res);
                ASSERT_COLUMN_EQ(res_column, executeFunction(StringTiDBConcatWS::func_name, separator_column, value));
            };
            separator_not_null_test(true);
            separator_not_null_test(false);
        }
    };
    for (const auto & value : all_inputs)
        test_separator_is_not_null(value);
}
CATCH

TEST_F(StringTiDBConcatWS, ThreeArgsTest)
try
{
    auto test_separator_is_null = [&](const ColumnWithTypeAndName & null_separator) {
        for (const auto & value1 : all_inputs)
        {
            for (const auto & value2 : all_inputs)
            {
                // separator is null, result is null.
                auto res = null_separator.type->onlyNull()
                    ? createOnlyNullColumnConst(1)
                    : (isConst(null_separator) && isConst(value1) && isConst(value2) ? toConst({}) : toColumn({}));
                ASSERT_COLUMN_EQ(res, executeFunction(StringTiDBConcatWS::func_name, null_separator, value1, value2));
            }
        }
    };
    for (const auto & item : nulls)
        test_separator_is_null(item);


    auto test_separator_is_not_null = [&](const ColumnWithTypeAndName & value1, const ColumnWithTypeAndName & value2) {
        for (const auto & separator : not_null_strings)
        {
            // null value is ignored.
            auto value_has_null_test = [&](bool is_separator_const, const ColumnWithTypeAndName & not_null_value) {
                auto separator_column = is_separator_const ? toConst(separator) : toColumn(separator);
                auto ignored_null_value_result
                    = executeFunction(StringTiDBConcatWS::func_name, separator_column, not_null_value);
                auto origin_result = executeFunction(StringTiDBConcatWS::func_name, separator_column, value1, value2);
                assert(getNotNullValue(ignored_null_value_result) == getNotNullValue(origin_result));
                assert((is_separator_const && isConst(not_null_value)) == isConst(ignored_null_value_result));
                assert((is_separator_const && isConst(value1) && isConst(value2)) == isConst(origin_result));
            };
            auto value_not_null_test = [&](bool is_separator_const) {
                auto separator_column = is_separator_const ? toConst(separator) : toColumn(separator);
                auto res = getNotNullValue(value1) + separator + getNotNullValue(value2);
                bool is_result_const = is_separator_const && isConst(value1) && isConst(value2);
                auto res_column = is_result_const ? toConst(res) : toColumn(res);
                ASSERT_COLUMN_EQ(
                    res_column,
                    executeFunction(StringTiDBConcatWS::func_name, separator_column, value1, value2));
            };
            if (isNull(value1))
            {
                value_has_null_test(true, value2);
                value_has_null_test(false, value2);
            }
            else if (isNull(value2))
            {
                value_has_null_test(true, value1);
                value_has_null_test(false, value1);
            }
            else
            {
                value_not_null_test(true);
                value_not_null_test(false);
            }
        }
    };
    for (const auto & value1 : all_inputs)
        for (const auto & value2 : all_inputs)
            test_separator_is_not_null(value1, value2);
}
CATCH

TEST_F(StringTiDBConcatWS, MoreArgsTest)
try
{
    auto more_args_test = [&](const std::vector<std::optional<String>> & values, const String & result) {
        ColumnsWithTypeAndName args;
        for (const auto & value : values)
            args.push_back(toColumn(value));

        ASSERT_COLUMN_EQ(toColumn(result), executeFunction(StringTiDBConcatWS::func_name, args));
    };
    // 4
    more_args_test({",", "大家好", {}, "hello word"}, "大家好,hello word");
    // 5
    more_args_test(
        {";", "这是中文", "C'est français", "これが日本の", "This is English"},
        "这是中文;C'est français;これが日本の;This is English");
    // 6
    more_args_test({"", "", "", "", "", ""}, "");
    // 7
    more_args_test({"a", "b", "", "", "", "", ""}, "baaaaa");
    // 101
    std::string separator = "&&";
    std::string unit = "a*b=c";
    std::vector<std::optional<String>> args;
    std::vector<String> res_builder;
    args.push_back(separator);
    for (size_t i = 0; i < 100; ++i)
    {
        args.push_back(unit);
        res_builder.push_back(unit);
    }
    std::string res = fmt::format("{}", fmt::join(res_builder.cbegin(), res_builder.cend(), separator));
    more_args_test(args, res);
}
CATCH

} // namespace DB::tests
