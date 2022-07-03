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
class HexStrTest : public DB::tests::FunctionTest
{
};

TEST_F(HexStrTest, hexstr_all_unit_Test)
try
{
    const String & func_name = "hexStr";

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"7777772E70696E676361702E636F6D", "61626364", "E6B58BE8AF95E6B58BE8AF95E6B58BE8AF95E6B58BE8AF9561626364E6B58BE8AF95", std::nullopt}),
        executeFunction(
            func_name,
            createColumn<Nullable<String>>({"www.pingcap.com", "abcd", "测试测试测试测试abcd测试", std::nullopt})));
}
CATCH
} // namespace tests
} // namespace DB