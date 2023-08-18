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

#include <DataTypes/DataTypeNullable.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>


namespace DB
{
namespace tests
{
class HexIntTest : public DB::tests::FunctionTest
{
};

TEST_F(HexIntTest, hexint_all_unit_Test)
try
{
    const String & func_name = "hexInt";

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"1348B21", std::nullopt, "0", "FFFFFFFFFECB74DF", "8000000000000000", "7FFFFFFFFFFFFFFF"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>(
                {20220705,
                 std::nullopt,
                 0,
                 -20220705,
                 std::numeric_limits<Int64>::min(),
                 std::numeric_limits<Int64>::max()})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"1348B21", std::nullopt, "0", "0", "FFFFFFFFFFFFFFFF"}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt64>>(
                {20220705, std::nullopt, 0, std::numeric_limits<UInt64>::min(), std::numeric_limits<UInt64>::max()})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>(
            {"13414DA", std::nullopt, "0", "FFFFFFFFFECBEB26", "FFFFFFFF80000000", "7FFFFFFF"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int32>>(
                {20190426,
                 std::nullopt,
                 0,
                 -20190426,
                 std::numeric_limits<Int32>::min(),
                 std::numeric_limits<Int32>::max()})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"13414DA", std::nullopt, "0", "0", "FFFFFFFF"}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt32>>(
                {20190426, std::nullopt, 0, std::numeric_limits<UInt32>::min(), std::numeric_limits<UInt32>::max()})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"3039", std::nullopt, "0", "FFFFFFFFFFFFCFC7", "FFFFFFFFFFFF8000", "7FFF"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int16>>(
                {12345,
                 std::nullopt,
                 0,
                 -12345,
                 std::numeric_limits<Int16>::min(),
                 std::numeric_limits<Int16>::max()})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"3039", std::nullopt, "0", "0", "FFFF"}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt16>>(
                {12345, std::nullopt, 0, std::numeric_limits<UInt16>::min(), std::numeric_limits<UInt16>::max()})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"78", std::nullopt, "0", "FFFFFFFFFFFFFF88", "FFFFFFFFFFFFFF80", "7F"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int8>>(
                {120, std::nullopt, 0, -120, std::numeric_limits<Int8>::min(), std::numeric_limits<Int8>::max()})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"8F", std::nullopt, "0", "0", "FF"}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt8>>(
                {143, std::nullopt, 0, std::numeric_limits<UInt8>::min(), std::numeric_limits<UInt8>::max()})));
}
CATCH
} // namespace tests
} // namespace DB