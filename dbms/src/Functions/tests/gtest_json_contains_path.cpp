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
class TestJsonContainsPath : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "json_contains_path";

    ColumnWithTypeAndName castStringToJson(const ColumnWithTypeAndName & column)
    {
        assert(removeNullable(column.type)->isString());
        ColumnsWithTypeAndName origin_inputs{column};
        return executeFunction("cast_string_as_json", origin_inputs, nullptr, true);
    }
};

TEST_F(TestJsonContainsPath, TestOnlyNull)
try
{
    size_t rows_count = 2;
    ColumnWithTypeAndName json_column = castStringToJson(createColumn<Nullable<String>>({"[]", "[]"}));
    auto type_column = createColumn<Nullable<String>>({"one", "one"});
    ColumnWithTypeAndName path_column = createColumn<Nullable<String>>({"$", "$"});
    ColumnWithTypeAndName path_column2 = createColumn<Nullable<String>>({"$.a", "$.a"});
    ColumnWithTypeAndName null_string_const = createConstColumn<Nullable<String>>(rows_count, {});
    ColumnWithTypeAndName null_bool_const = createConstColumn<Nullable<UInt8>>(rows_count, {});
    ColumnWithTypeAndName only_null_const = createOnlyNullColumnConst(rows_count);

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({true, true}),
        executeFunction(func_name, json_column, type_column, path_column));
    ASSERT_COLUMN_EQ(only_null_const, executeFunction(func_name, only_null_const, type_column, path_column));
    ASSERT_COLUMN_EQ(null_bool_const, executeFunction(func_name, null_string_const, type_column, path_column));
    ASSERT_COLUMN_EQ(only_null_const, executeFunction(func_name, json_column, only_null_const, path_column));
    ASSERT_COLUMN_EQ(null_bool_const, executeFunction(func_name, json_column, null_string_const, path_column));
    ASSERT_COLUMN_EQ(only_null_const, executeFunction(func_name, json_column, type_column, only_null_const));
    ASSERT_COLUMN_EQ(null_bool_const, executeFunction(func_name, json_column, type_column, null_string_const));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({true, true}),
        executeFunction(func_name, json_column, type_column, path_column, only_null_const));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({true, true}),
        executeFunction(func_name, json_column, type_column, path_column, null_string_const));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({{}, {}}),
        executeFunction(func_name, json_column, type_column, path_column2, only_null_const));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({{}, {}}),
        executeFunction(func_name, json_column, type_column, path_column2, null_string_const));
    ASSERT_COLUMN_EQ(
        only_null_const,
        executeFunction(func_name, json_column, type_column, only_null_const, path_column));
    ASSERT_COLUMN_EQ(
        null_bool_const,
        executeFunction(func_name, json_column, type_column, null_string_const, path_column));

    // type and path const.
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({true, true}),
        executeFunction(
            func_name,
            json_column,
            createConstColumn<String>(2, "one"),
            createConstColumn<String>(2, "$"),
            null_string_const));
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({true, true}),
        executeFunction(
            func_name,
            json_column,
            createConstColumn<String>(2, "one"),
            createConstColumn<String>(2, "$"),
            only_null_const));
    ASSERT_COLUMN_EQ(
        null_bool_const,
        executeFunction(
            func_name,
            json_column,
            createConstColumn<String>(2, "one"),
            null_string_const,
            createConstColumn<String>(2, "$")));
    ASSERT_COLUMN_EQ(
        only_null_const,
        executeFunction(
            func_name,
            json_column,
            createConstColumn<String>(2, "one"),
            only_null_const,
            createConstColumn<String>(2, "$")));
}
CATCH

TEST_F(TestJsonContainsPath, TestNullable)
try
{
    ColumnWithTypeAndName json_column = castStringToJson(createColumn<Nullable<String>>({{}, "[]", "[]", "[]"}));
    auto type_column = createColumn<Nullable<String>>({"one", {}, "one", "one"});
    ColumnWithTypeAndName path_column = createColumn<Nullable<String>>({"$", "$", {}, "$"});

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({{}, {}, {}, true}),
        executeFunction(func_name, json_column, type_column, path_column));
}
CATCH

TEST_F(TestJsonContainsPath, TestNotNull)
try
{
    auto exec_assert1 = [&](const String & json, const String & type, const String & path, bool expect) {
        ASSERT_COLUMN_EQ(
            createColumn<UInt8>({expect, expect}),
            executeFunction(
                func_name,
                {castStringToJson(createColumn<String>({json, json})),
                 createColumn<String>({type, type}),
                 createColumn<String>({path, path})}));
        ASSERT_COLUMN_EQ(
            createColumn<UInt8>({expect, expect}),
            executeFunction(
                func_name,
                {castStringToJson(createConstColumn<String>(2, json)),
                 createColumn<String>({type, type}),
                 createColumn<String>({path, path})}));
        ASSERT_COLUMN_EQ(
            createColumn<UInt8>({expect, expect}),
            executeFunction(
                func_name,
                {castStringToJson(createColumn<String>({json, json})),
                 createConstColumn<String>(2, type),
                 createColumn<String>({path, path})}));
        ASSERT_COLUMN_EQ(
            createColumn<UInt8>({expect, expect}),
            executeFunction(
                func_name,
                {castStringToJson(createColumn<String>({json, json})),
                 createColumn<String>({type, type}),
                 createConstColumn<String>(2, path)}));
        ASSERT_COLUMN_EQ(
            createColumn<UInt8>({expect, expect}),
            executeFunction(
                func_name,
                {castStringToJson(createColumn<String>({json, json})),
                 createConstColumn<String>(2, type),
                 createConstColumn<String>(2, path)}));
    };
    auto exec_assert2
        = [&](const String & json, const String & type, const String & path1, const String & path2, bool expect) {
              ASSERT_COLUMN_EQ(
                  createColumn<UInt8>({expect, expect}),
                  executeFunction(
                      func_name,
                      {castStringToJson(createColumn<String>({json, json})),
                       createColumn<String>({type, type}),
                       createColumn<String>({path1, path1}),
                       createColumn<String>({path2, path2})}));
              ASSERT_COLUMN_EQ(
                  createColumn<UInt8>({expect, expect}),
                  executeFunction(
                      func_name,
                      {castStringToJson(createColumn<String>({json, json})),
                       createColumn<String>({type, type}),
                       createColumn<String>({path1, path1}),
                       createColumn<String>({path2, path2})}));
              ASSERT_COLUMN_EQ(
                  createColumn<UInt8>({expect, expect}),
                  executeFunction(
                      func_name,
                      {castStringToJson(createConstColumn<String>(2, json)),
                       createColumn<String>({type, type}),
                       createColumn<String>({path1, path1}),
                       createColumn<String>({path2, path2})}));
              ASSERT_COLUMN_EQ(
                  createColumn<UInt8>({expect, expect}),
                  executeFunction(
                      func_name,
                      {castStringToJson(createColumn<String>({json, json})),
                       createConstColumn<String>(2, type),
                       createColumn<String>({path1, path1}),
                       createColumn<String>({path2, path2})}));
              ASSERT_COLUMN_EQ(
                  createColumn<UInt8>({expect, expect}),
                  executeFunction(
                      func_name,
                      {castStringToJson(createColumn<String>({json, json})),
                       createColumn<String>({type, type}),
                       createConstColumn<String>(2, path1),
                       createColumn<String>({path2, path2})}));
              ASSERT_COLUMN_EQ(
                  createColumn<UInt8>({expect, expect}),
                  executeFunction(
                      func_name,
                      {castStringToJson(createColumn<String>({json, json})),
                       createColumn<String>({type, type}),
                       createColumn<String>({path1, path1}),
                       createConstColumn<String>(2, path2)}));
              ASSERT_COLUMN_EQ(
                  createColumn<UInt8>({expect, expect}),
                  executeFunction(
                      func_name,
                      {castStringToJson(createColumn<String>({json, json})),
                       createConstColumn<String>(2, type),
                       createConstColumn<String>(2, path1),
                       createConstColumn<String>(2, path2)}));
          };

    exec_assert1("{}", "one", "$", true);
    exec_assert1("{}", "one", "$.a", false);
    exec_assert1("{}", "all", "$", true);
    exec_assert1("{}", "all", "$.a", false);

    exec_assert2("{}", "one", "$", "$", true);
    exec_assert2("{}", "one", "$.a", "$.b", false);
    exec_assert2("{}", "one", "$", "$.b", true);
    exec_assert2("{}", "one", "$.a", "$", true);
    exec_assert2("{}", "all", "$", "$", true);
    exec_assert2("{}", "all", "$.a", "$.b", false);
    exec_assert2("{}", "all", "$", "$.b", false);
    exec_assert2("{}", "all", "$.a", "$", false);

    exec_assert1("{}", "ONE", "$", true);
    exec_assert1("{}", "OnE", "$", true);
    exec_assert1("{}", "oNe", "$", true);
    exec_assert1("{}", "onE", "$", true);
    exec_assert1("{}", "One", "$", true);
    exec_assert1("{}", "ALL", "$", true);
    exec_assert1("{}", "All", "$", true);
    exec_assert1("{}", "aLl", "$", true);
    exec_assert1("{}", "alL", "$", true);
    exec_assert1("{}", "aLL", "$", true);
}
CATCH

} // namespace DB::tests
