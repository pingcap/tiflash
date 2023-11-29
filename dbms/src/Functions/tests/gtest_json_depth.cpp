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
/**
  * Because FunctionJsonDepth have 
  *    ```
  *    bool useDefaultImplementationForNulls() const override { return true; }
  *    bool useDefaultImplementationForConstants() const override { return true; }
  *    ```
  * there is no need to test const, null_value, and only null.
  */
class TestJsonDepth : public DB::tests::FunctionTest
{
public:
    ColumnWithTypeAndName executeFunctionWithCast(const ColumnWithTypeAndName & column)
    {
        // Use string as input column to improve readability.
        assert(column.type->isString());
        ColumnsWithTypeAndName origin_inputs{column};
        auto json_column = executeFunction("cast_string_as_json", origin_inputs, nullptr, true);
        return executeFunction("json_depth", json_column);
    }

    void executeAndAssert(const String & input, const UInt64 & expect)
    {
        ASSERT_COLUMN_EQ(createColumn<UInt64>({expect}), executeFunctionWithCast({createColumn<String>({input})}));
    }
};

TEST_F(TestJsonDepth, TestAll)
try
{
    // int
    executeAndAssert("0", 1);
    executeAndAssert("1", 1);
    executeAndAssert("-1", 1);
    // double
    executeAndAssert("1.1111", 1);
    executeAndAssert("-1.1111", 1);
    // bool
    executeAndAssert("true", 1);
    executeAndAssert("false", 1);
    // string
    executeAndAssert("\"sdhfgjksdahfjksdhfjhsdjkfhjskdhfkjsdhfjksdhfkj\"", 1);
    executeAndAssert("\"\"", 1);

    // array
    executeAndAssert("[]", 1);
    executeAndAssert("[[]]", 2);
    executeAndAssert("[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]", 21);

    // obj
    executeAndAssert("{}", 1);
    executeAndAssert("{\"a\":1}", 2);
    executeAndAssert("{\"a\":{}}", 2);
    executeAndAssert(R"({"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{}}}}}}}}}}})", 11);

    // complex
    executeAndAssert(R"([{}, "a", 1, 1.232, {"a": ["a", [{"a": {"b":true}}]]}])", 7);
}
CATCH

} // namespace DB::tests
