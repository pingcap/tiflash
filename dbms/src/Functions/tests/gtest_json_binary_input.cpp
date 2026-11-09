// Copyright 2026 PingCAP, Inc.
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

#include <Common/Exception.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <vector>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
} // namespace DB::ErrorCodes

namespace DB::tests
{
class TestJsonBinaryInput : public FunctionTest
{
public:
    static constexpr size_t rows = 2;

    struct FunctionCase
    {
        String function;
        ColumnsWithTypeAndName extra_arguments;
        Field empty_string_result;
    };

    static String emptyJsonString() { return String("\x0c\0", 2); }

    static std::vector<FunctionCase> functionCases()
    {
        const auto path = createColumn<String>({"$", "$"});
        const auto const_path = createConstColumn<String>(rows, "$");
        const auto one = createColumn<String>({"one", "one"});
        const auto all = createColumn<String>({"all", "all"});
        const auto const_one = createConstColumn<String>(rows, "one");
        const auto const_all = createConstColumn<String>(rows, "all");
        return {
            {"json_extract", {const_path}, Field(emptyJsonString())},
            {"json_extract", {path}, Field(emptyJsonString())},
            {"jsonLength", {}, Field(UInt64(1))},
            {"jsonLength", {const_path}, Field(UInt64(1))},
            {"jsonLength", {path}, Field(UInt64(1))},
            {"json_contains_path", {const_one, const_path}, Field(UInt64(1))},
            {"json_contains_path", {const_all, const_path}, Field(UInt64(1))},
            {"json_contains_path", {one, path}, Field(UInt64(1))},
            {"json_contains_path", {all, path}, Field(UInt64(1))},
            {"json_keys_2_args", {const_path}, Field(Null())},
            {"json_keys_2_args", {path}, Field(Null())},
        };
    }

    ColumnWithTypeAndName executeCase(const FunctionCase & test_case, const ColumnWithTypeAndName & json)
    {
        ColumnsWithTypeAndName arguments{json};
        arguments.insert(arguments.end(), test_case.extra_arguments.begin(), test_case.extra_arguments.end());
        return executeFunction(test_case.function, arguments);
    }
};

TEST_F(TestJsonBinaryInput, RejectsEmptyInput)
try
{
    const ColumnsWithTypeAndName inputs{
        createColumn<String>({"", ""}),
        createColumn<Nullable<String>>({String{}, String{}}),
        createColumn<Nullable<String>>({{}, String{}}),
        createColumn<String>({emptyJsonString(), String{}}),
        createConstColumn<String>(rows, String{}),
        createConstColumn<Nullable<String>>(rows, String{}),
    };
    const auto cases = functionCases();
    for (size_t case_index = 0; case_index < cases.size(); ++case_index)
    {
        SCOPED_TRACE(case_index);
        SCOPED_TRACE(cases[case_index].function);
        for (size_t input_index = 0; input_index < inputs.size(); ++input_index)
        {
            SCOPED_TRACE(input_index);
            try
            {
                executeCase(cases[case_index], inputs[input_index]);
                FAIL() << "Expected empty binary JSON input to be rejected";
            }
            catch (const Exception & e)
            {
                EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS) << e.displayText();
            }
        }
    }
}
CATCH

TEST_F(TestJsonBinaryInput, AcceptsJsonEmptyString)
try
{
    const auto json = emptyJsonString();
    const ColumnsWithTypeAndName inputs{
        createColumn<String>({json, json}),
        createColumn<Nullable<String>>({json, json}),
        createConstColumn<String>(rows, json),
        createConstColumn<Nullable<String>>(rows, json),
    };
    for (const auto & test_case : functionCases())
    {
        SCOPED_TRACE(test_case.function);
        for (const auto & input : inputs)
        {
            const auto result = executeCase(test_case, input);
            ASSERT_EQ(result.column->size(), rows);
            for (size_t row = 0; row < rows; ++row)
                EXPECT_FIELD_EQ((*result.column)[row], test_case.empty_string_result);
        }
    }
}
CATCH

TEST_F(TestJsonBinaryInput, PreservesSqlNull)
try
{
    for (const auto & test_case : functionCases())
    {
        SCOPED_TRACE(test_case.function);
        const auto mixed = executeCase(test_case, createColumn<Nullable<String>>({{}, emptyJsonString()}));
        EXPECT_TRUE((*mixed.column)[0].isNull());
        EXPECT_FIELD_EQ((*mixed.column)[1], test_case.empty_string_result);

        const auto constant_null = executeCase(test_case, createConstColumn<Nullable<String>>(rows, {}));
        ASSERT_EQ(constant_null.column->size(), rows);
        for (size_t row = 0; row < rows; ++row)
            EXPECT_TRUE((*constant_null.column)[row].isNull());
    }
}
CATCH
} // namespace DB::tests
