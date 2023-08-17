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
#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDateTime.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB
{
namespace tests
{
class TestDateOrDatetimeToSomething : public DB::tests::FunctionTest
{
};

TEST_F(TestDateOrDatetimeToSomething, TestToQuarter)
try
{
    static const String func_name = "toQuarter";
    static const auto data_type_ptr = makeNullable(std::make_shared<DataTypeMyDateTime>(6));

    // ColumnVector(nullable(DateTime))
    auto data_col_ptr = createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
                            {
                                MyDateTime(2020, 1, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2020, 2, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2020, 3, 10, 0, 0, 0, 0).toPackedUInt(),
                                {}, // Null
                                MyDateTime(2020, 4, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2020, 5, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2020, 6, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2020, 7, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2020, 8, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2020, 9, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2020, 10, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2020, 11, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(2020, 12, 10, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt() // Zero time
                            })
                            .column;
    ColumnWithTypeAndName input_col(data_col_ptr, data_type_ptr, "input");
    auto output_col = createColumn<Nullable<UInt8>>({1, 1, 1, {}, 2, 2, 2, 3, 3, 3, 4, 4, 4, 0});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    // ColumnConst(non-null)
    data_col_ptr = createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(
                       4,
                       MyDateTime(2020, 10, 10, 0, 0, 0, 0).toPackedUInt())
                       .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, data_type_ptr, "input");
    output_col = createConstColumn<UInt8>(4, 4);
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    // ColumnConst(null)
    data_col_ptr = createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(4, {}).column;
    input_col = ColumnWithTypeAndName(data_col_ptr, data_type_ptr, "input");
    output_col = createConstColumn<Nullable<UInt8>>(4, {});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    // ColumnVector(DateTime)
    data_col_ptr
        = createColumn<DataTypeMyDateTime::FieldType>({
                                                          MyDateTime(2020, 1, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 2, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 3, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 4, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 5, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 6, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 7, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 8, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 9, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 10, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 11, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(2020, 12, 10, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt() // Zero time
                                                      })
              .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, std::make_shared<DataTypeMyDateTime>(6), "input");
    output_col = createColumn<UInt8>({1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 0});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));
}
CATCH
} // namespace tests
} // namespace DB
