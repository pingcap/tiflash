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
struct TestJsonMemberOf : public DB::tests::FunctionTest
{
    static constexpr auto func_name = "json_member_of";

    ColumnWithTypeAndName castStringToJson(const ColumnWithTypeAndName & column)
    {
        assert(removeNullable(column.type)->isString());
        ColumnsWithTypeAndName origin_inputs{column};
        return executeFunction("cast_string_as_json", origin_inputs, nullptr, true);
    }

    void executeAndAssert(const String & target, const String & obj, bool expect)
    {
        ASSERT_COLUMN_EQ(
            createColumn<UInt8>({expect, expect}),
            executeFunction(
                func_name,
                {castStringToJson(createColumn<String>({target, target})),
                 castStringToJson(createColumn<String>({obj, obj}))}));
        ASSERT_COLUMN_EQ(
            createColumn<UInt8>({expect, expect}),
            executeFunction(
                func_name,
                {castStringToJson(createConstColumn<String>(2, target)),
                 castStringToJson(createColumn<String>({obj, obj}))}));
    }
};

TEST_F(TestJsonMemberOf, TestAll)
try
{
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<UInt8>>({{}, {}}),
        executeFunction(
            func_name,
            {castStringToJson(createColumn<Nullable<String>>({{}, "{}"})),
             castStringToJson(createColumn<Nullable<String>>({"{}", {}}))}));

    executeAndAssert("1", "1", true);
    executeAndAssert("1", "999", false);
    executeAndAssert("1.11", "1.11", true);
    executeAndAssert("1.11", "1.22", false);
    executeAndAssert("[]", "[]", false);
    executeAndAssert("{}", "{}", true);
    executeAndAssert(R"({"a":123, "b":234})", R"({"a":123, "b":234})", true);
    executeAndAssert(R"({"a":123, "b":23})", R"({"a":123, "b":234})", false);
    executeAndAssert("123", "[]", false);
    executeAndAssert("123", "[123]", true);
    executeAndAssert(R"({"a":123, "b":234})", R"([{"a":123, "b":234}])", true);
    executeAndAssert(R"({"a":123, "b":23})", R"([{"a":123, "b":234}])", false);
    executeAndAssert("1", "[1, 2, 3, 4, 5, 6]", true);
    executeAndAssert("2", "[1, 2, 3, 4, 5, 6]", true);
    executeAndAssert("3", "[1, 2, 3, 4, 5, 6]", true);
    executeAndAssert("4", "[1, 2, 3, 4, 5, 6]", true);
    executeAndAssert("5", "[1, 2, 3, 4, 5, 6]", true);
    executeAndAssert("6", "[1, 2, 3, 4, 5, 6]", true);
    executeAndAssert("0", "[1, 2, 3, 4, 5, 6]", false);
    executeAndAssert("7", "[1, 2, 3, 4, 5, 6]", false);

    // for number type
    executeAndAssert("1", "[1]", true);
    executeAndAssert("1", "[1.0]", true);
    executeAndAssert("1.00", "[1]", true);
}
CATCH

} // namespace DB::tests
