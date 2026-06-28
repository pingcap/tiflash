// Copyright 2022 PingCAP, Ltd.
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

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>


namespace DB::tests
{
class StringInstr : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name_utf8 = "instrUTF8";

    static constexpr auto func_name = "instr";

protected:
    static ColumnWithTypeAndName toVec(const std::vector<String> & v)
    {
        return createColumn<String>(v);
    }
    static ColumnWithTypeAndName toNullableVec(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }

    static ColumnWithTypeAndName toVecInt(const std::vector<std::optional<Int64>> & v)
    {
        return createColumn<Nullable<Int64>>(v);
    }

    static ColumnWithTypeAndName toConstString(const String & s)
    {
        return createConstColumn<Nullable<String>>(1, s);
    }

    static ColumnWithTypeAndName toConstInt(const Int64 & s)
    {
        return createConstColumn<Int64>(1, s);
    }
};

TEST_F(StringInstr, instrUTF8Test)
try
{
    ASSERT_COLUMN_EQ(
        toVecInt({4, 0, 3, 0, 3, 6, 5, 1, 1, 7, 7, 3, 4, 3}),
        executeFunction(
            func_name_utf8,
            toNullableVec({"foobarbar", "xbar", "中文美好", "中文美好", "中文abc", "live long and prosper", "not binary string", "upper case", "upper case", "UPPER CASE", "UPPER CASE", "中文abc", "abcテストabc", "ѐёђѓєѕіїјљњћќѝўџ"}),
            toNullableVec({"bar", "foobar", "美好", "世界", "a", "long", "binary", "upper", "uPpEr", "CASE", "CasE", "abc", "テスト", "ђѓєѕ"})));

    ASSERT_COLUMN_EQ(
        toConstInt(4),
        executeFunction(
            func_name_utf8,
            toConstString("foobarbar"),
            toConstString("bar")));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({4}),
        executeFunction(
            func_name_utf8,
            toNullableVec({"foobarbar"}),
            toConstString("bar")));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({4}),
        executeFunction(
            func_name_utf8,
            toConstString("foobarbar"),
            toNullableVec({"bar"})));

    ASSERT_COLUMN_EQ(
        toVecInt({{}, {}, {}, {}, 1, 1}),
        executeFunction(
            func_name_utf8,
            toNullableVec({"foobar", {}, {}, "", "", "abc"}),
            toNullableVec({{}, "foobar", {}, {}, "", ""})));
}
CATCH

// test instr NULL
TEST_F(StringInstr, instrTest)
try
{
    ASSERT_COLUMN_EQ(
        toVecInt({4, 0, 3, 0, 3, 6, 5, 1, 0, 7, 0, 3, 4, 3}),
        executeFunction(
            func_name,
            toNullableVec({"foobarbar", "xbar", "中文美好", "中文美好", "中文abc", "live long and prosper", "not binary string", "upper case", "upper case", "UPPER CASE", "UPPER CASE", "中文abc", "abcテストabc", "ѐёђѓєѕіїјљњћќѝўџ"}),
            toNullableVec({"bar", "foobar", "美好", "世界", "a", "long", "binary", "upper", "uPpEr", "CASE", "CasE", "abc", "テスト", "ђѓєѕ"})));

    ASSERT_COLUMN_EQ(
        toConstInt(4),
        executeFunction(
            func_name,
            toConstString("foobarbar"),
            toConstString("bar")));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({4}),
        executeFunction(
            func_name,
            toNullableVec({"foobarbar"}),
            toConstString("bar")));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int64>>({4}),
        executeFunction(
            func_name,
            toConstString("foobarbar"),
            toNullableVec({"bar"})));

    ASSERT_COLUMN_EQ(
        toVecInt({{}, {}, {}, {}, 1, 1, {}}),
        executeFunction(
            func_name,
            toNullableVec({"foobar", {}, {}, "", "", "abc", "abc"}),
            toNullableVec({{}, "foobar", {}, {}, "", "", {}})));
}
CATCH

} // namespace DB::tests
