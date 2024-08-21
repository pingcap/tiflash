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

#include <Columns/ColumnNullable.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/JsonBinary.h>
#include <TiDB/Schema/TiDBTypes.h>

namespace DB::tests
{
class TestJsonArray : public DB::tests::FunctionTest
{
public:
    ColumnWithTypeAndName executeFunctionWithCast(
        const ColumnNumbers & argument_column_numbers,
        const ColumnsWithTypeAndName & columns)
    {
        auto json_column = executeFunction("json_array", argument_column_numbers, columns);
        static auto json_array_return_type = std::make_shared<DataTypeString>();
        assert(json_array_return_type->equals(*json_column.type));
        // The `json_binary` should be cast as a string to improve readability.
        tipb::FieldType field_type;
        field_type.set_flen(-1);
        field_type.set_tp(TiDB::TypeString);
        return executeCastJsonAsStringFunction({json_column}, field_type);
    }
};

TEST_F(TestJsonArray, TestAll)
try
{
    auto const not_null_json_type = std::make_shared<DataTypeString>();
    auto const nullable_json_type = makeNullable(std::make_shared<DataTypeString>());

    /// prepare has value json column/const
    // []
    // clang-format off
    const UInt8 empty_array[] = {
        JsonBinary::TYPE_CODE_ARRAY, // array_type
        0x0, 0x0, 0x0, 0x0, // element_count
        0x8, 0x0, 0x0, 0x0}; // total_size
    const size_t rows_count = 2;
    // clang-format on
    ColumnWithTypeAndName not_null_column;
    {
        auto empty_array_json = ColumnString::create();
        empty_array_json->insertData(reinterpret_cast<const char *>(empty_array), sizeof(empty_array) / sizeof(UInt8));
        empty_array_json->insertData(reinterpret_cast<const char *>(empty_array), sizeof(empty_array) / sizeof(UInt8));
        not_null_column = ColumnWithTypeAndName(std::move(empty_array_json), not_null_json_type);
    }
    ColumnWithTypeAndName not_null_const;
    {
        auto empty_array_json = ColumnString::create();
        empty_array_json->insertData(reinterpret_cast<const char *>(empty_array), sizeof(empty_array) / sizeof(UInt8));
        auto const_col = ColumnConst::create(std::move(empty_array_json), rows_count);
        not_null_const = ColumnWithTypeAndName(std::move(const_col), not_null_json_type);
    }
    ColumnWithTypeAndName nullable_but_not_null_column;
    {
        auto empty_array_json = ColumnString::create();
        empty_array_json->insertData(reinterpret_cast<const char *>(empty_array), sizeof(empty_array) / sizeof(UInt8));
        empty_array_json->insertData(reinterpret_cast<const char *>(empty_array), sizeof(empty_array) / sizeof(UInt8));
        ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows_count, 0);
        auto json_col = ColumnNullable::create(std::move(empty_array_json), std::move(col_null_map));
        nullable_but_not_null_column = ColumnWithTypeAndName(std::move(json_col), nullable_json_type);
    }

    /// prepare null value json column/const
    ColumnWithTypeAndName null_column;
    {
        auto str_col = ColumnString::create();
        str_col->insertData("", 0);
        str_col->insertData("", 0);
        ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(rows_count, 1);
        auto json_col = ColumnNullable::create(std::move(str_col), std::move(col_null_map));
        null_column = ColumnWithTypeAndName(std::move(json_col), nullable_json_type);
    }
    ColumnWithTypeAndName null_const = createConstColumn<Nullable<String>>(rows_count, {});

    /// prepare only null column
    ColumnWithTypeAndName only_null_const = createOnlyNullColumnConst(rows_count);

    /// prepare input columns
    ColumnsWithTypeAndName inputs(
        {not_null_column, not_null_const, nullable_but_not_null_column, null_column, null_const, only_null_const});

    auto gen_column_expect = [](const String & value) {
        // double for rows_count 2.
        return createColumn<Nullable<String>>({value, value});
    };

    // json_array()
    {
        auto res = executeFunctionWithCast({}, inputs);
        auto expect = createConstColumn<Nullable<String>>(rows_count, "[]");
        ASSERT_COLUMN_EQ(expect, res);
    }

    // json_array(all columns)
    {
        auto res = executeFunctionWithCast({0, 1, 2, 3, 4, 5}, inputs);
        auto expect = gen_column_expect("[[], [], [], null, null, null]");
        ASSERT_COLUMN_EQ(expect, res);
    }

    // json_array(only_null, only_null)
    {
        auto res = executeFunctionWithCast({5, 5}, inputs);
        auto expect = createConstColumn<Nullable<String>>(rows_count, "[null, null]");
        ASSERT_COLUMN_EQ(expect, res);
    }

    // json_array(null_const, not_null_const, only_null)
    {
        auto res = executeFunctionWithCast({4, 1, 5}, inputs);
        auto expect = createConstColumn<Nullable<String>>(rows_count, "[null, [], null]");
        ASSERT_COLUMN_EQ(expect, res);
    }

    // json_array(not_null_const, not_null_const)
    {
        auto res = executeFunctionWithCast({1, 1}, inputs);
        auto expect = createConstColumn<Nullable<String>>(rows_count, "[[], []]");
        ASSERT_COLUMN_EQ(expect, res);
    }

    // json_array(not_null_column, null_column)
    {
        auto res = executeFunctionWithCast({0, 3}, inputs);
        auto expect = gen_column_expect("[[], null]");
        ASSERT_COLUMN_EQ(expect, res);
    }

    // json_array(not_null_column, not_null_const)
    {
        auto res = executeFunctionWithCast({0, 1}, inputs);
        auto expect = gen_column_expect("[[], []]");
        ASSERT_COLUMN_EQ(expect, res);
    }
}
CATCH

} // namespace DB::tests
