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

#include <Columns/ColumnNullable.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/JsonBinary.h>

#include <string>
#include <vector>

namespace DB::tests
{
class TestJsonValid : public DB::tests::FunctionTest
{
};

TEST_F(TestJsonValid, TestJsonValidOthers)
try
{
    static constexpr auto func_name = "json_valid_others";
    {
        auto input = createColumn<Int64>({10, 100, 29, 22});
        ASSERT_COLUMN_EQ(createConstColumn<UInt8>(input.column->size(), false), executeFunction(func_name, input));
    }
    {
        auto input = createColumn<UInt64>({0, 10, 100, 29, 22});
        ASSERT_COLUMN_EQ(createConstColumn<UInt8>(input.column->size(), false), executeFunction(func_name, input));
    }
    {
        auto input = createColumn<UInt8>({1, 1, 0, 0, 1, 0});
        ASSERT_COLUMN_EQ(createConstColumn<UInt8>(input.column->size(), false), executeFunction(func_name, input));
    }
    {
        // Although it is stated in https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid that returns NULL if the argument is NULL,
        // both MySQL and TiDB will directly return false instead of NULL.
        auto input = createOnlyNullColumn(10);
        ASSERT_COLUMN_EQ(createConstColumn<UInt8>(input.column->size(), false), executeFunction(func_name, input));
    }
}
CATCH

TEST_F(TestJsonValid, TestJsonValidJson)
try
{
    static constexpr auto func_name = "json_valid_json";
    auto input = createColumn<String>({"{}", "[]", "1", "\"fdfd\""});
    // Use string as input column to improve readability.
    ColumnsWithTypeAndName origin_inputs{input};
    auto json_column = executeFunction("cast_string_as_json", origin_inputs, nullptr, true);
    ASSERT_COLUMN_EQ(createConstColumn<UInt8>(input.column->size(), true), executeFunction(func_name, json_column));
}
CATCH

TEST_F(TestJsonValid, TestJsonValidString)
try
{
    auto execute_and_assert = [&](const String & input, bool expect) {
        ASSERT_COLUMN_EQ(
            createColumn<UInt8>({expect, expect}),
            executeFunction("json_valid_string", {createColumn<String>({input, input})}));
    };

    execute_and_assert("", false);
    execute_and_assert("[]", true);
    execute_and_assert("{}", true);
    execute_and_assert("1", true);
    execute_and_assert("-991", true);
    execute_and_assert("1.111", true);
    execute_and_assert("-991.111", true);
    execute_and_assert("-991gdfgf.111", false);
    execute_and_assert("gsdfgsdf", false);
    execute_and_assert("\"gsdfgsdf\"", true);
    execute_and_assert(R"({"a":[]})", true);
    execute_and_assert(R"({"a":[], "b" :1, "c" :{}, "d":"d"})", true);
}
CATCH

} // namespace DB::tests
