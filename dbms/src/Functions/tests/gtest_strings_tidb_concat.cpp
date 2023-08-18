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

    using Type = Nullable<String>;
    using NotNullType = String;

    InferredDataVector<Type> test_strings = {"", "www.pingcap", "中文.测.试。。。", {}};
};

TEST_F(StringTidbConcat, OneArgTest)
try
{
    for (const auto & value : test_strings)
    {
        // column
        ASSERT_COLUMN_EQ(
            createColumn<Type>({value}),
            executeFunction(StringTidbConcat::func_name, createColumn<Type>({value})));
        // const
        ASSERT_COLUMN_EQ(
            value.has_value() ? createConstColumn<NotNullType>(1, value.value()) : createConstColumn<Type>(1, value),
            executeFunction(StringTidbConcat::func_name, createConstColumn<Type>(1, value)));
    }
}
CATCH

TEST_F(StringTidbConcat, TwoArgsTest)
try
{
    auto two_args_test = [&](const InferredFieldType<Type> & value1, const InferredFieldType<Type> & value2) {
        // one of value is null, result is null.
        bool is_result_not_null = value1.has_value() && value2.has_value();
        InferredFieldType<Type> result
            = is_result_not_null ? (value1.value() + value2.value()) : std::optional<String>{};
        auto inner_test = [&](bool is_value1_const, bool is_value2_const) {
            // all args is const or has only null const
            auto is_result_const = (is_value1_const && is_value2_const) || (!value1.has_value() && is_value1_const)
                || (!value2.has_value() && is_value2_const);
            ASSERT_COLUMN_EQ(
                is_result_const
                    ? (is_result_not_null ? createConstColumn<NotNullType>(1, result.value())
                                          : createConstColumn<Type>(1, result))
                    : createColumn<Type>({result}),
                executeFunction(
                    StringTidbConcat::func_name,
                    is_value1_const ? createConstColumn<Type>(1, value1) : createColumn<Type>({value1}),
                    is_value2_const ? createConstColumn<Type>(1, value2) : createColumn<Type>({value2})));
        };
        std::vector<bool> is_consts = {true, false};
        for (bool is_value1_const : is_consts)
            for (bool is_value2_const : is_consts)
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
    auto more_args_test = [&](const std::vector<String> & values, const String & result) {
        ColumnsWithTypeAndName args;
        for (const auto & value : values)
            args.push_back(createColumn<String>({value}));

        ASSERT_COLUMN_EQ(createColumn<String>({result}), executeFunction(StringTidbConcat::func_name, args));
    };

    // 3
    more_args_test({"大家好", ",", "hello word"}, "大家好,hello word");
    // 4
    more_args_test(
        {"这是中文", "C'est français", "これが日本の", "This is English"},
        "这是中文C'est françaisこれが日本のThis is English");
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
