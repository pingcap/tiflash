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
class HexIntTest : public DB::tests::FunctionTest
{
};

TEST_F(HexIntTest, hexint_all_unit_Test)
try
{
    const String & func_name = "hexInt";

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({std::nullopt, "3039", "FFFFFFFFFFFFFFFF"}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt64>>({std::nullopt, 12345, 0xFFFFFFFFFFFFFFFF})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"3039", "FFFFFFFFFFFFCFC7"}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int64>>({12345, -12345})));

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"FF", "1"}),
        executeFunction(
            func_name,
            createColumn<Nullable<UInt8>>({255, 1})));
}
CATCH
} // namespace tests
} // namespace DB