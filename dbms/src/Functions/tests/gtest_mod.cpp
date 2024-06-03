// Copyright 2024 PingCAP, Inc.
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

#include <Columns/ColumnsNumber.h>
#include <Common/Logger.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsTiDBConversion.h>
#include <TestUtils/FunctionTestUtils.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <gtest/gtest.h>

namespace DB::tests
{
namespace
{
const std::string func_name = "modulo";
class TestMod : public DB::tests::FunctionTest
{
public:
};

TEST_F(TestMod, ModZero)
try
{
    /// Decimal32
    ASSERT_COLUMN_EQ_V2(
        createColumn<Nullable<Decimal32>>(std::make_tuple(8, 1), {"0.7", {}}),
        DB::tests::executeFunction(
            *context,
            func_name,
            {createColumn<Decimal32>(std::make_tuple(8, 1), {"5.5", "3.3"}),
             createColumn<Decimal32>(std::make_tuple(8, 1), {"1.2", "0"})}));

    context->getDAGContext()->setFlags(TiDBSQLFlags::IN_INSERT_STMT);
    context->getDAGContext()->setSQLMode(TiDBSQLMode::ERROR_FOR_DIVISION_BY_ZERO | TiDBSQLMode::STRICT_ALL_TABLES);

    try
    {
        DB::tests::executeFunction(
            *context,
            func_name,
            {createColumn<Decimal32>(std::make_tuple(8, 1), {"5.5", "3.3"}),
             createColumn<Decimal32>(std::make_tuple(8, 1), {"1.2", "0"})});
    }
    catch (DB::Exception & e)
    {
        GTEST_ASSERT_EQ(e.message(), "Division by 0");
    }
}
CATCH

} // namespace
} // namespace DB::tests
