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

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsString.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
class StringRepeatTest : public DB::tests::FunctionTest
{
public:
    static constexpr auto funcName = "repeat";

protected:
    static ColumnWithTypeAndName toVecString(const std::vector<std::optional<String>> & v)
    {
        return createColumn<Nullable<String>>(v);
    }
    static ColumnWithTypeAndName toVecInt(const std::vector<std::optional<Int64>> & v)
    {
        return createColumn<Nullable<Int64>>(v);
    }
    static ColumnWithTypeAndName toConstString(const String & s) { return createConstColumn<Nullable<String>>(1, s); }
    static ColumnWithTypeAndName toConstInt(const Int64 & s) { return createConstColumn<Nullable<Int64>>(1, s); }
};

TEST_F(StringRepeatTest, StringRepeatUnit)
try
{
    /// vec const
    ASSERT_COLUMN_EQ(
        toVecString({"aaa", "bbb", "ccc", "ddd"}),
        executeFunction(funcName, toVecString({"a", "b", "c", "d"}), toConstInt(3)));
    /// vec vec
    ASSERT_COLUMN_EQ(
        toVecString({"a", "bb", "ccc", "dddd"}),
        executeFunction(funcName, toVecString({"a", "b", "c", "d"}), toVecInt({1, 2, 3, 4})));
    /// const const
    ASSERT_COLUMN_EQ(
        createConstColumn<String>(1, "aaaaa"),
        executeFunction(funcName, toConstString("a"), toConstInt(5)));
    /// const vec
    ASSERT_COLUMN_EQ(
        toVecString({"a", "aa", "aaa", "aaaa"}),
        executeFunction(funcName, toConstString("a"), toVecInt({1, 2, 3, 4})));
}
CATCH

TEST_F(StringRepeatTest, MoreUnit)
try
{
    /// repeat_times < 1, should return ""
    ASSERT_COLUMN_EQ(
        toVecString({"a", "", ""}),
        executeFunction(funcName, toVecString({"a", "b", "c"}), toVecInt({1, 0, -1})));
    /// Test Null
    ASSERT_COLUMN_EQ(toVecString({{}, {}}), executeFunction(funcName, toVecString({{}, "b"}), toVecInt({1, {}})));

    /// U/Int8, ...U/Int64
    ASSERT_COLUMN_EQ(
        toVecString({"aaa"}),
        executeFunction(funcName, toVecString({"a"}), createConstColumn<Nullable<Int8>>(1, 3)));
    ASSERT_COLUMN_EQ(
        toVecString({"aaa"}),
        executeFunction(funcName, toVecString({"a"}), createConstColumn<Nullable<Int16>>(1, 3)));
    ASSERT_COLUMN_EQ(
        toVecString({"aaa"}),
        executeFunction(funcName, toVecString({"a"}), createConstColumn<Nullable<Int32>>(1, 3)));
    ASSERT_COLUMN_EQ(
        toVecString({"aaa"}),
        executeFunction(funcName, toVecString({"a"}), createConstColumn<Nullable<Int64>>(1, 3)));
    ASSERT_COLUMN_EQ(
        toVecString({"aaa"}),
        executeFunction(funcName, toVecString({"a"}), createConstColumn<Nullable<UInt8>>(1, 3)));
    ASSERT_COLUMN_EQ(
        toVecString({"aaa"}),
        executeFunction(funcName, toVecString({"a"}), createConstColumn<Nullable<UInt16>>(1, 3)));
    ASSERT_COLUMN_EQ(
        toVecString({"aaa"}),
        executeFunction(funcName, toVecString({"a"}), createConstColumn<Nullable<UInt32>>(1, 3)));
    ASSERT_COLUMN_EQ(
        toVecString({"aaa"}),
        executeFunction(funcName, toVecString({"a"}), createConstColumn<Nullable<UInt64>>(1, 3)));


    ASSERT_COLUMN_EQ(
        toVecString({"", "你好你好你好", "\n\n\n", "u是你u是你"}),
        executeFunction(funcName, toVecString({"", "你好", "\n", "u是你"}), toVecInt({3, 3, 3, 2})));
}
CATCH


} // namespace tests
} // namespace DB
