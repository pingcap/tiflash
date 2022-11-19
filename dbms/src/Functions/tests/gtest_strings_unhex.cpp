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

#include <DataTypes/DataTypeNullable.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>


namespace DB
{
namespace tests
{
class UnHexTest : public DB::tests::FunctionTest
{
};

TEST_F(UnHexTest, unhex_all_unit_Test)
try
{
    const String & func_name = "tidbUnHex";

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"www.pingcap.com", "abcd", std::nullopt, std::nullopt, ""}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"7777772E70696E676361702E636F6D", "61626364", std::nullopt, "GG", ""})));

    // CJK and emoji
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"さらに入", "测试测试测试测试abcd测试", "🍻", "🏴‍☠️"}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"E38195E38289E381ABE585A5", "E6B58BE8AF95E6B58BE8AF95E6B58BE8AF95E6B58BE8AF9561626364E6B58BE8AF95", "F09F8DBB", "F09F8FB4E2808DE298A0EFB88F"})));

    // Special Empty Character
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"\t", "\t", "\n", "\n", " "}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"9", "09", "A", "0A", "20"})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"abcd", "\tg", std::nullopt, std::nullopt}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({61626364, 967, std::nullopt, -1})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"abcd", "\tg", std::nullopt}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt64>>({61626364, 967, std::nullopt})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"abc", "\tg", std::nullopt, std::nullopt}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int32>>({616263, 967, std::nullopt, -1})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"abc", "\tg", std::nullopt}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt32>>({616263, 967, std::nullopt})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"ab", "\tg", std::nullopt, std::nullopt}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int16>>({6162, 967, std::nullopt, -1})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"ab", "\tg", std::nullopt}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt16>>({6162, 967, std::nullopt})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"a", "\t", std::nullopt, std::nullopt}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int8>>({61, 9, std::nullopt, -1})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"a", "\t", std::nullopt}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt8>>({61, 9, std::nullopt})));
}
CATCH
} // namespace tests
} // namespace DB
