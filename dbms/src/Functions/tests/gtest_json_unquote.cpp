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
class TestJsonUnquote : public DB::tests::FunctionTest
{
};

TEST_F(TestJsonUnquote, TestAll)
try
{
    /// Normal case: ColumnVector(nullable)
    const String func_name = "json_unquote";
    static auto const nullable_string_type_ptr = makeNullable(std::make_shared<DataTypeString>());
    static auto const string_type_ptr = std::make_shared<DataTypeString>();
    String bj2("\"hello, \\\"你好, \\u554A world, null, true]\"");
    String bj4("[[0, 1], [2, 3], [4, [5, 6]]]");
    auto input_col = createColumn<Nullable<String>>({bj2, {}, bj4});
    auto output_col
        = createColumn<Nullable<String>>({"hello, \"你好, 啊 world, null, true]", {}, "[[0, 1], [2, 3], [4, [5, 6]]]"});
    auto res = executeFunction(func_name, input_col);
    ASSERT_COLUMN_EQ(res, output_col);

    /// Normal case: ColumnVector(null)
    input_col = createColumn<Nullable<String>>({{}, {}, {}});
    output_col = createColumn<Nullable<String>>({{}, {}, {}});
    res = executeFunction(func_name, input_col);
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnConst(null)
    auto null_input_col = createConstColumn<Nullable<String>>(3, {});
    output_col = createConstColumn<Nullable<String>>(3, {});
    res = executeFunction(func_name, null_input_col);
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnVector(non-null)
    auto non_null_input_col = createColumn<String>({bj2, bj2, bj4});
    res = executeFunction(func_name, non_null_input_col);
    output_col = createColumn<Nullable<String>>(
        {"hello, \"你好, 啊 world, null, true]",
         "hello, \"你好, 啊 world, null, true]",
         "[[0, 1], [2, 3], [4, [5, 6]]]"});
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnConst(non-null)
    auto const_non_null_input_col = createConstColumn<String>(3, bj2);
    res = executeFunction(func_name, const_non_null_input_col);
    output_col = createConstColumn<Nullable<String>>(3, {"hello, \"你好, 啊 world, null, true]"});
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnConst(nullable)
    auto const_nullable_input_col = createConstColumn<Nullable<String>>(3, {bj2});
    res = executeFunction(func_name, const_nullable_input_col);
    output_col = createConstColumn<Nullable<String>>(3, {"hello, \"你好, 啊 world, null, true]"});
    ASSERT_COLUMN_EQ(res, output_col);
}
CATCH

} // namespace DB::tests