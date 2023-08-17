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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <random>
#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class TestIsIPAddr : public DB::tests::FunctionTest
{
};
TEST_F(TestIsIPAddr, isIPv4)
try
{
    // test ColumnVector without nullable
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0}),
        executeFunction(
            "tiDBIsIPv4",
            {createColumn<String>(
                {"123.123.123.123",
                 "0.0.0.0",
                 "127.0.0.1",
                 "192.168.0.0/10",
                 "192.168.99.22.123",
                 "999.999.999.999",
                 "3.2.1.",
                 "3..2.1",
                 "...",
                 "4556456",
                 "ajdjioa",
                 ""})}));

    // test ColumnVector with nullable
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({1, 0, 0, 1, 0}),
        executeFunction(
            "tiDBIsIPv4",
            {createColumn<Nullable<String>>(
                {"123.123.123.123", "aidjio", "1236.461.841.312", "99.99.99.99", std::nullopt})}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({0, 0, 0, 0, 0}),
        executeFunction(
            "tiDBIsIPv4",
            {createColumn<Nullable<String>>({std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt})}));
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 0, 0, 0, 0}), executeFunction("tiDBIsIPv4", {createOnlyNullColumn(5)}));

    // test ColumnConst without nullable
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(4, 1),
        executeFunction("tiDBIsIPv4", {createConstColumn<String>(4, "123.123.123.123")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(4, 0),
        executeFunction("tiDBIsIPv4", {createConstColumn<String>(4, "aidjio")}));

    // test ColumnConst with nullable but non-null value
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(2, 1),
        executeFunction("tiDBIsIPv4", {createConstColumn<Nullable<String>>(2, "123.123.123.123")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(2, 0),
        executeFunction("tiDBIsIPv4", {createConstColumn<Nullable<String>>(2, "1236.461.841.312")}));

    // test ColumnConst with nullable and null value
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(4, 0),
        executeFunction("tiDBIsIPv4", {createConstColumn<Nullable<String>>(4, std::nullopt)}));
    ASSERT_COLUMN_EQ(createConstColumn<UInt8>(4, 0), executeFunction("tiDBIsIPv4", {createOnlyNullColumnConst(4)}));
}
CATCH


TEST_F(TestIsIPAddr, isIPv6)
try
{
    // test ColumnVector without nullable
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0}),
        executeFunction(
            "tiDBIsIPv6",
            {createColumn<String>(
                {"F746:C349:48E3:22F2:81E0:0EA8:E7B6:8286",
                 "0000:0000:0000:0000:0000:0000:0000:0000",
                 "2001:0:2851:b9f0:6d:2326:9036:f37a",
                 "fe80::2dc3:25a5:49a1:6002%24",
                 "4207:A33A:58D3:F2C3:8EDC:A548:3EC7:0D00:0D00:0D00",
                 "4207:A33A:58D3:F2C3:8EDC:A548::0D00",
                 "4207::::8EDC:A548:3EC7:0D00",
                 "4207:::::A548:3EC7:0D00",
                 "::::::",
                 "4556456",
                 "ajdjioa",
                 ""})}));

    // test ColumnVector with nullable
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({1, 0, 0, 0, 0}),
        executeFunction(
            "tiDBIsIPv6",
            {createColumn<Nullable<String>>(
                {"F746:C349:48E3:22F2:81E0:0EA8:E7B6:8286",
                 "aidjio",
                 "1236.461.841.312",
                 "99.99.99.99",
                 std::nullopt})}));
    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({0, 0, 0, 0, 0}),
        executeFunction(
            "tiDBIsIPv6",
            {createColumn<Nullable<String>>({std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt})}));
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 0, 0, 0, 0}), executeFunction("tiDBIsIPv6", {createOnlyNullColumn(5)}));

    // test ColumnConst without nullable
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(4, 1),
        executeFunction("tiDBIsIPv6", {createConstColumn<String>(4, "F746:C349:48E3:22F2:81E0:0EA8:E7B6:8286")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(4, 0),
        executeFunction("tiDBIsIPv6", {createConstColumn<String>(4, "aidjio")}));

    // test ColumnConst with nullable but non-null value
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(2, 1),
        executeFunction(
            "tiDBIsIPv6",
            {createConstColumn<Nullable<String>>(2, "F746:C349:48E3:22F2:81E0:0EA8:E7B6:8286")}));
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(2, 0),
        executeFunction("tiDBIsIPv6", {createConstColumn<Nullable<String>>(2, "aidjio")}));

    // test ColumnConst with nullable and null value
    ASSERT_COLUMN_EQ(
        createConstColumn<UInt8>(4, 0),
        executeFunction("tiDBIsIPv6", {createConstColumn<Nullable<String>>(4, std::nullopt)}));
    ASSERT_COLUMN_EQ(createConstColumn<UInt8>(4, 0), executeFunction("tiDBIsIPv6", {createOnlyNullColumnConst(4)}));
}
CATCH


} // namespace tests
} // namespace DB
