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
class TestJsonLength : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "jsonLength";

    ColumnWithTypeAndName castStringToJson(const ColumnWithTypeAndName & column)
    {
        assert(removeNullable(column.type)->isString());
        ColumnsWithTypeAndName origin_inputs{column};
        return executeFunction("cast_string_as_json", origin_inputs, nullptr, true);
    }
};

TEST_F(TestJsonLength, OneArg)
try
{
    auto execute_assert = [&](const String & json, const UInt64 & expect) {
        auto res_col = executeFunction(func_name, {castStringToJson(createColumn<String>({json, json}))});
        auto expect_col = createColumn<UInt64>({expect, expect});
        ASSERT_COLUMN_EQ(expect_col, res_col);
    };

    // int
    execute_assert("0", 1);
    execute_assert("1", 1);
    execute_assert("-1", 1);
    // double
    execute_assert("1.1111", 1);
    execute_assert("-1.1111", 1);
    // bool
    execute_assert("true", 1);
    execute_assert("false", 1);
    // string
    execute_assert("\"sdhfgjksdahfjksdhfjhsdjkfhjskdhfkjsdhfjksdhfkj\"", 1);
    execute_assert("\"\"", 1);

    // array
    execute_assert("[]", 0);
    execute_assert("[[]]", 1);
    execute_assert("[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]", 1);
    execute_assert("[1, 2, 3, 4]", 4);

    // obj
    execute_assert("{}", 0);
    execute_assert("{\"a\":1}", 1);
    execute_assert("{\"a\":{}}", 1);
    execute_assert(R"({"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{}}}}}}}}}}})", 1);
    execute_assert(R"({"a":1,"b":2,"c":3,"d":4})", 4);
}
CATCH

TEST_F(TestJsonLength, TwoArg)
try
{
    auto execute_assert = [&](const String & json, const String & path, const std::optional<UInt64> & expect) {
        auto expect_col = createColumn<Nullable<UInt64>>({expect, expect});
        // column, column
        {
            auto res_col = executeFunction(
                func_name,
                {castStringToJson(createColumn<String>({json, json})), createColumn<String>({path, path})});
            ASSERT_COLUMN_EQ(expect_col, res_col);
        }
        // column, const
        {
            auto res_col = executeFunction(
                func_name,
                {castStringToJson(createColumn<String>({json, json})), createConstColumn<String>(2, path)});
            ASSERT_COLUMN_EQ(expect_col, res_col);
        }
    };

    // int
    execute_assert("0", "$", 1);
    execute_assert("1", "$", 1);
    execute_assert("-1", "$", 1);
    // double
    execute_assert("1.1111", "$", 1);
    execute_assert("-1.1111", "$", 1);
    // bool
    execute_assert("true", "$", 1);
    execute_assert("false", "$", 1);
    // string
    execute_assert("\"sdhfgjksdahfjksdhfjhsdjkfhjskdhfkjsdhfjksdhfkj\"", "$", 1);
    execute_assert("\"\"", "$", 1);

    // array
    execute_assert("[1, 2, 3]", "$.a", {});
    execute_assert("[1, 2, 3]", "$", 3);

    // obj
    execute_assert(R"({"a":1,"b":2})", "$.a", 1);
    execute_assert(R"({"a":{},"b":2})", "$.a", 0);
    execute_assert(R"({"a":1,"b":2})", "$.c", {});
    execute_assert(R"({"a":1,"b":2})", "$", 2);
}
CATCH

TEST_F(TestJsonLength, Null)
try
{
    size_t rows_count = 2;
    ColumnWithTypeAndName json_column = castStringToJson(createColumn<Nullable<String>>({"[]", "[]"}));
    ColumnWithTypeAndName json_column2 = castStringToJson(createColumn<Nullable<String>>({"{}", "{}"}));
    ColumnWithTypeAndName json_column3 = castStringToJson(createColumn<Nullable<String>>({"1", "1"}));
    ColumnWithTypeAndName path_column = createColumn<Nullable<String>>({"$", "$"});
    ColumnWithTypeAndName path_column2 = createColumn<Nullable<String>>({"$.a", "$.a"});
    ColumnWithTypeAndName null_string_const = createConstColumn<Nullable<String>>(rows_count, {});
    ColumnWithTypeAndName null_string_column = createColumn<Nullable<String>>({{}, {}});
    ColumnWithTypeAndName only_null_const = createOnlyNullColumnConst(rows_count);
    ColumnWithTypeAndName int_column0 = createColumn<Nullable<UInt64>>({0, 0});
    ColumnWithTypeAndName int_const0 = createColumn<Nullable<UInt64>>({0, 0});
    ColumnWithTypeAndName int_column1 = createColumn<Nullable<UInt64>>({1, 1});
    ColumnWithTypeAndName int_const1 = createColumn<Nullable<UInt64>>({1, 1});
    ColumnWithTypeAndName null_int_const = createConstColumn<Nullable<UInt64>>(rows_count, {});
    ColumnWithTypeAndName null_int_column = createColumn<Nullable<UInt64>>({{}, {}});

    // one arg
    ASSERT_COLUMN_EQ(int_column0, executeFunction(func_name, json_column));
    ASSERT_COLUMN_EQ(int_column0, executeFunction(func_name, json_column2));
    ASSERT_COLUMN_EQ(int_column1, executeFunction(func_name, json_column3));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, only_null_const));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, null_string_const));
    ASSERT_COLUMN_EQ(null_int_column, executeFunction(func_name, null_string_column));

    // two arg
    ASSERT_COLUMN_EQ(int_column0, executeFunction(func_name, json_column, path_column));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, json_column, only_null_const));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, json_column, null_string_const));
    ASSERT_COLUMN_EQ(null_int_column, executeFunction(func_name, json_column, null_string_column));
    ASSERT_COLUMN_EQ(int_column0, executeFunction(func_name, json_column2, path_column));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, json_column2, only_null_const));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, json_column2, null_string_const));
    ASSERT_COLUMN_EQ(null_int_column, executeFunction(func_name, json_column2, null_string_column));
    ASSERT_COLUMN_EQ(int_column1, executeFunction(func_name, json_column3, path_column));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, json_column3, only_null_const));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, json_column3, null_string_const));
    ASSERT_COLUMN_EQ(null_int_column, executeFunction(func_name, json_column3, null_string_column));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, only_null_const, path_column));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, only_null_const, only_null_const));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, only_null_const, null_string_const));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, only_null_const, null_string_column));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, null_string_const, path_column));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, null_string_const, only_null_const));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, null_string_const, null_string_const));
    ASSERT_COLUMN_EQ(null_int_const, executeFunction(func_name, null_string_const, null_string_column));
}
CATCH

} // namespace DB::tests
