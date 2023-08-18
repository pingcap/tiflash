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

#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
namespace DB::tests
{
class TestBin : public DB::tests::FunctionTest
{
};

TEST_F(TestBin, Simple)
try
{
    ASSERT_COLUMN_EQ(createColumn<String>({"1100100"}), executeFunction("bin", createColumn<Int64>({100})));
}
CATCH

TEST_F(TestBin, Boundary)
try
{
    ASSERT_COLUMN_EQ(
        createColumn<String>({"0", "1111111111111111111111111111111111111111111111111111111110000000", "1111111"}),
        executeFunction("bin", createColumn<Int8>({0, INT8_MIN, INT8_MAX})));
    ASSERT_COLUMN_EQ(
        createColumn<String>(
            {"0", "1111111111111111111111111111111111111111111111111000000000000000", "111111111111111"}),
        executeFunction("bin", createColumn<Int16>({0, INT16_MIN, INT16_MAX})));
    ASSERT_COLUMN_EQ(
        createColumn<String>(
            {"0",
             "1111111111111111111111111111111110000000000000000000000000000000",
             "1111111111111111111111111111111"}),
        executeFunction("bin", createColumn<Int32>({0, INT32_MIN, INT32_MAX})));
    ASSERT_COLUMN_EQ(
        createColumn<String>(
            {"0",
             "1000000000000000000000000000000000000000000000000000000000000000",
             "111111111111111111111111111111111111111111111111111111111111111"}),
        executeFunction("bin", createColumn<Int64>({0, INT64_MIN, INT64_MAX})));
    ASSERT_COLUMN_EQ(createColumn<String>({"0", "11111111"}), executeFunction("bin", createColumn<UInt8>({0, 255})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({"0", "1111111111111111"}),
        executeFunction("bin", createColumn<UInt16>({0, 65535})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({"0", "11111111111111111111111111111111"}),
        executeFunction("bin", createColumn<UInt32>({0, 4294967295})));
    ASSERT_COLUMN_EQ(
        createColumn<String>({"0", "1111111111111111111111111111111111111111111111111111111111111111"}),
        executeFunction("bin", createColumn<UInt64>({0, ULLONG_MAX})));
}
CATCH

} // namespace DB::tests
