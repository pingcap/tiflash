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
#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace DB::tests
{
class TestJsonKeys : public DB::tests::FunctionTest
{
public:
    ColumnWithTypeAndName castStringToJson(const ColumnWithTypeAndName & column)
    {
        assert(removeNullable(column.type)->isString());
        ColumnsWithTypeAndName origin_inputs{column};
        return executeFunction("cast_string_as_json", origin_inputs, nullptr, true);
    }
};

TEST_F(TestJsonKeys, TestJsonKeys)
try
{
    static constexpr auto func_name = "json_keys";
    auto execute_and_assert = [&](const String & input, const std::optional<String> & expect) {
        ASSERT_COLUMN_EQ(
            castStringToJson(createColumn<Nullable<String>>({expect})),
            executeFunction(func_name, {castStringToJson(createColumn<String>({input}))}));
    };

    execute_and_assert("1", {});
    execute_and_assert("-1", {});
    execute_and_assert("1.1", {});
    execute_and_assert("-1.1", {});
    execute_and_assert("\"sdafgsdjfhghjdsg\"", {});
    execute_and_assert("\"\"", {});
    execute_and_assert("[1, []]", {});
    execute_and_assert("{}", "[]");
    execute_and_assert(R"({"1":[]})", R"(["1"])");
    // in tidb will be  `["", "1", "2", "3"]`.
    execute_and_assert(R"({"1":[], "2":{}, "3":"fsdfsd", "":1233})", R"(["1", "2", "3", ""])");
}
CATCH

TEST_F(TestJsonKeys, TestJsonKeys2args)
try
{
    static constexpr auto func_name = "json_keys_2_args";

    // only null
    ColumnWithTypeAndName only_null_const = createOnlyNullColumnConst(1);
    ASSERT_COLUMN_EQ(only_null_const, executeFunction(func_name, {only_null_const, only_null_const}));
    ASSERT_COLUMN_EQ(only_null_const, executeFunction(func_name, {createColumn<String>({"{}"}), only_null_const}));
    ASSERT_COLUMN_EQ(only_null_const, executeFunction(func_name, {only_null_const, createColumn<String>({"$"})}));

    // not only null inputs
    auto execute_func = [&](const std::optional<String> & json, const std::optional<String> & path) {
        return executeFunction(
            func_name,
            {castStringToJson(createColumn<Nullable<String>>({json})), createColumn<Nullable<String>>({path})});
    };
    auto execute_and_assert = [&](const std::optional<String> & json,
                                  const std::optional<String> & path,
                                  const std::optional<String> & expect) {
        ASSERT_COLUMN_EQ(castStringToJson(createColumn<Nullable<String>>({expect})), execute_func(json, path));
    };

    execute_and_assert({}, "$", {});
    execute_and_assert("{}", {}, {});
    execute_and_assert({}, {}, {});

    execute_and_assert("1", "$", {});
    execute_and_assert("-1", "$", {});
    execute_and_assert("1.1", "$", {});
    execute_and_assert("-1.1", "$", {});
    execute_and_assert("\"sdafgsdjfhghjdsg\"", "$", {});
    execute_and_assert("\"\"", "$", {});
    execute_and_assert("[1, []]", "$", {});

    execute_and_assert("{}", "$", "[]");
    execute_and_assert(R"({"1":[]})", "$", R"(["1"])");
    // in tidb will be  `["", "1", "2", "3"]`.
    execute_and_assert(R"({"1":[], "2":{}, "3":"fsdfsd", "":1233})", "$", R"(["1", "2", "3", ""])");

    // In this situation, path expressions may not contain the * and ** tokens or range selection.
    ASSERT_THROW(execute_func(R"({"1":[]})", "$.*"), Exception);
    // Invalid JSON path expression.
    ASSERT_THROW(execute_func(R"({"1":[]})", ""), Exception);

    execute_and_assert(R"({"1":[]})", R"($."1")", {});
    execute_and_assert(R"({"1":{}})", R"($."1")", "[]");
    execute_and_assert(R"({"1":{}})", R"($."2")", {});
    execute_and_assert(R"({"1":{"1":[], "2":{}, "3":"fsdfsd", "":1233}})", R"($."1")", R"(["1", "2", "3", ""])");
}
CATCH

} // namespace DB::tests
