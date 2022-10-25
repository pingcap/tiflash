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
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB::tests
{
class StringFormatDecimalStr : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "formatDecimalStr";
};

TEST_F(StringFormatDecimalStr, test)
try
{
    ColumnsWithTypeAndName args{createColumn<Nullable<String>>({"99.990000", "0.0120", "", "0.000", "1.0", "1.11", "100.00", "100"})};
    ASSERT_COLUMN_EQ(
        createColumn<Nullable<String>>({"99.99", "0.012", "", "0", "1", "1.11", "100", "100"}),
        executeFunction(
            func_name,
            args,
            nullptr,
            true));
}
CATCH

} // namespace DB::tests
