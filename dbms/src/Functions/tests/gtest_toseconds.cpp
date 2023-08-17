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

#include <Common/MyTime.h>
#include <Functions/FunctionFactory.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class TestToSeconds : public DB::tests::FunctionTest
{
};

TEST_F(TestToSeconds, TestAll)
try
{
    auto & dag_context = getDAGContext();
    UInt64 ori_flags = dag_context.getFlags();
    dag_context.addFlag(TiDBSQLFlags::TRUNCATE_AS_WARNING);
    /// ColumnVector(nullable)
    const String func_name = "tidbToSeconds";
    static auto const nullable_datetime_type_ptr = makeNullable(std::make_shared<DataTypeMyDateTime>(6));
    static auto const datetime_type_ptr = std::make_shared<DataTypeMyDateTime>(6);
    static auto const date_type_ptr = std::make_shared<DataTypeMyDate>();
    auto data_col_ptr = createColumn<Nullable<DataTypeMyDateTime::FieldType>>(
                            {
                                {}, // Null
                                MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(0, 1, 1, 0, 0, 0, 0).toPackedUInt(),
                                MyDateTime(1969, 1, 2, 1, 1, 1, 1).toPackedUInt(),
                                MyDateTime(2000, 12, 31, 10, 10, 10, 700).toPackedUInt(),
                                MyDateTime(2022, 3, 13, 6, 7, 8, 9).toPackedUInt(),
                            })
                            .column;
    auto input_col = ColumnWithTypeAndName(data_col_ptr, nullable_datetime_type_ptr, "input");
    auto output_col = createColumn<Nullable<UInt64>>({{}, {}, 86400, 62135773261ULL, 63145476610ULL, 63814370828ULL});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnVector(non-null)
    data_col_ptr
        = createColumn<DataTypeMyDateTime::FieldType>({
                                                          MyDateTime(0, 0, 0, 0, 0, 0, 0).toPackedUInt(),
                                                          MyDateTime(1969, 1, 2, 1, 1, 1, 1).toPackedUInt(),
                                                          MyDateTime(2000, 12, 31, 10, 10, 10, 700).toPackedUInt(),
                                                          MyDateTime(2022, 3, 13, 6, 7, 8, 9).toPackedUInt(),
                                                      })
              .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, datetime_type_ptr, "input");
    output_col = createColumn<Nullable<UInt64>>({{}, 62135773261ULL, 63145476610ULL, 63814370828ULL});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(non-null)
    input_col = ColumnWithTypeAndName(
        createConstColumn<DataTypeMyDateTime::FieldType>(1, MyDateTime(2022, 3, 13, 6, 7, 8, 9).toPackedUInt()).column,
        datetime_type_ptr,
        "input");
    output_col = createConstColumn<Nullable<UInt64>>(1, {63814370828ULL});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable)
    input_col = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(
            1,
            MyDateTime(2022, 3, 13, 6, 7, 8, 9).toPackedUInt())
            .column,
        nullable_datetime_type_ptr,
        "input");
    output_col = createConstColumn<Nullable<UInt64>>(1, {63814370828ULL});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// ColumnConst(nullable(null))
    input_col = ColumnWithTypeAndName(
        createConstColumn<Nullable<DataTypeMyDateTime::FieldType>>(1, {}).column,
        nullable_datetime_type_ptr,
        "input");
    output_col = createConstColumn<Nullable<UInt64>>(1, {});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));

    /// MyDate ColumnVector(non-null)
    data_col_ptr = createColumn<DataTypeMyDate::FieldType>({
                                                               MyDate(0000, 0, 1).toPackedUInt(),
                                                               MyDate(0000, 1, 1).toPackedUInt(),
                                                               MyDate(1969, 1, 1).toPackedUInt(),
                                                               MyDate(2000, 12, 1).toPackedUInt(),
                                                               MyDate(2022, 3, 14).toPackedUInt(),
                                                           })
                       .column;
    input_col = ColumnWithTypeAndName(data_col_ptr, date_type_ptr, "input");
    output_col = createColumn<Nullable<UInt64>>({{}, 86400, 62135683200ULL, 63142848000ULL, 63814435200ULL});
    ASSERT_COLUMN_EQ(output_col, executeFunction(func_name, input_col));
    dag_context.setFlags(ori_flags);
}
CATCH

} // namespace DB::tests
