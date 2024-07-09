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
#include <Columns/ColumnsNumber.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/JsonBinary.h>

#include <string>
#include <vector>

namespace DB::tests
{
class TestCastJsonAsString : public DB::tests::FunctionTest
{
};

TEST_F(TestCastJsonAsString, TestAll)
try
{
    /// Normal case: ColumnVector(nullable)
    const String func_name = "cast_json_as_string";
    static auto const nullable_string_type_ptr = makeNullable(std::make_shared<DataTypeString>());
    static auto const string_type_ptr = std::make_shared<DataTypeString>();
    auto str_col = ColumnString::create();
    // clang-format off
    /// `[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`
    UInt8 bj2[] = {
        0x3, 0x6, 0x0, 0x0, 0x0, 0x6b, 0x0, 0x0, 0x0, 0x1, 0x26, 0x0, 0x0, 0x0, 0x9, 0x4e, 0x0, 0x0, 0x0, 0xb, 0x56, 0x0, 0x0, 0x0, 0xc, 0x5e,
        0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x0, 0x4, 0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x28, 0x0, 0x0, 0x0, 0x1e, 0x0, 0x0, 0x0, 0x1,
        0x0, 0x1f, 0x0, 0x0, 0x0, 0x1, 0x0, 0x9, 0x20, 0x0, 0x0, 0x0, 0x4, 0x1, 0x0, 0x0, 0x0, 0x61, 0x62, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
        0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc, 0x40, 0xc, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x77,
        0x6f, 0x72, 0x6c, 0x64
    };
    /// `[[0,1],[2,3],[4,[5,6]]]`
    UInt8 bj9[] = {
        0x3, 0x3, 0x0, 0x0, 0x0, 0x97, 0x0, 0x0, 0x0, 0x3, 0x17, 0x0, 0x0, 0x0, 0x3, 0x39, 0x0, 0x0, 0x0, 0x3, 0x5b, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0,
        0x22, 0x0, 0x0, 0x0, 0x9, 0x12, 0x0, 0x0, 0x0, 0x9, 0x1a, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x22, 0x0, 0x0, 0x0, 0x9, 0x12, 0x0, 0x0, 0x0, 0x9, 0x1a, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0,
        0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x3c, 0x0, 0x0, 0x0, 0x9, 0x12, 0x0, 0x0, 0x0, 0x3, 0x1a, 0x0, 0x0,
        0x0, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x22, 0x0, 0x0, 0x0, 0x9, 0x12, 0x0, 0x0, 0x0, 0x9, 0x1a, 0x0, 0x0, 0x0,
        0x5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0
    };
    // clang-format on
    str_col->insertData(reinterpret_cast<const char *>(bj2), sizeof(bj2) / sizeof(UInt8));
    str_col->insertData("", 0);
    str_col->insertData(reinterpret_cast<const char *>(bj9), sizeof(bj9) / sizeof(UInt8));
    ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(3, 0);
    ColumnUInt8::Container & vec_null_map = col_null_map->getData();
    vec_null_map[1] = 0;
    auto json_col = ColumnNullable::create(std::move(str_col), std::move(col_null_map));
    auto input_col = ColumnWithTypeAndName(std::move(json_col), nullable_string_type_ptr, "input0");

    auto output_col = createColumn<Nullable<String>>(
        {R"([{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true])", {}, "[[0, 1], [2, 3], [4, [5, 6]]]"});
    tipb::FieldType field_type;
    field_type.set_flen(-1);
    field_type.set_collate(TiDB::ITiDBCollator::BINARY);
    field_type.set_tp(TiDB::TypeString);
    auto res = executeCastJsonAsStringFunction(input_col, field_type);
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnVector(null)
    str_col = ColumnString::create();
    str_col->insertData("", 0);
    str_col->insertData("", 0);
    str_col->insertData("", 0);
    col_null_map = ColumnUInt8::create(3, 1);
    json_col = ColumnNullable::create(std::move(str_col), std::move(col_null_map));
    input_col = ColumnWithTypeAndName(std::move(json_col), nullable_string_type_ptr, "input0");

    output_col = createColumn<Nullable<String>>({{}, {}, {}});
    res = executeCastJsonAsStringFunction(input_col, field_type);
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnConst(null)
    auto null_input_col = createConstColumn<Nullable<String>>(3, {});
    output_col = createConstColumn<Nullable<String>>(3, {});
    res = executeCastJsonAsStringFunction(null_input_col, field_type);
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnVector(non-null)
    auto non_null_str_col = ColumnString::create();
    non_null_str_col->insertData(reinterpret_cast<const char *>(bj2), sizeof(bj2) / sizeof(UInt8));
    non_null_str_col->insertData(reinterpret_cast<const char *>(bj2), sizeof(bj2) / sizeof(UInt8));
    non_null_str_col->insertData(reinterpret_cast<const char *>(bj9), sizeof(bj9) / sizeof(UInt8));
    auto non_null_input_col = ColumnWithTypeAndName(std::move(non_null_str_col), string_type_ptr, "input0");
    res = executeCastJsonAsStringFunction(non_null_input_col, field_type);
    output_col = createColumn<Nullable<String>>(
        {R"([{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true])",
         R"([{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true])",
         "[[0, 1], [2, 3], [4, [5, 6]]]"});
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnConst(non-null)
    non_null_str_col = ColumnString::create();
    non_null_str_col->insertData(reinterpret_cast<const char *>(bj2), sizeof(bj2) / sizeof(UInt8));
    auto const_non_null_input_col = ColumnConst::create(std::move(non_null_str_col), 3);
    res = executeCastJsonAsStringFunction({std::move(const_non_null_input_col), string_type_ptr, ""}, field_type);
    output_col
        = createConstColumn<Nullable<String>>(3, {R"([{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true])"});
    ASSERT_COLUMN_EQ(res, output_col);

    /// ColumnConst(nullable)
    auto nested_str_col = ColumnString::create();
    nested_str_col->insertData(reinterpret_cast<const char *>(bj9), sizeof(bj9) / sizeof(UInt8));
    col_null_map = ColumnUInt8::create(1, 0);
    json_col = ColumnNullable::create(std::move(nested_str_col), std::move(col_null_map));
    auto const_json_col = ColumnConst::create(std::move(json_col), 3);
    auto const_nullable_input_col
        = ColumnWithTypeAndName(std::move(const_json_col), nullable_string_type_ptr, "input0");
    res = executeCastJsonAsStringFunction(const_nullable_input_col, field_type);
    output_col = createConstColumn<Nullable<String>>(3, {"[[0, 1], [2, 3], [4, [5, 6]]]"});
    ASSERT_COLUMN_EQ(res, output_col);

    /// Limit string length
    context->getDAGContext()->addFlag(TiDBSQLFlags::IGNORE_TRUNCATE);
    str_col = ColumnString::create();
    str_col->insertData(reinterpret_cast<const char *>(bj2), sizeof(bj2) / sizeof(UInt8));
    str_col->insertData("", 0);
    str_col->insertData(reinterpret_cast<const char *>(bj9), sizeof(bj9) / sizeof(UInt8));
    col_null_map = ColumnUInt8::create(3, 0);
    json_col = ColumnNullable::create(std::move(str_col), std::move(col_null_map));
    input_col = ColumnWithTypeAndName(std::move(json_col), nullable_string_type_ptr, "input0");

    output_col = createColumn<Nullable<String>>({R"([{"a")", {}, "[[0, "});
    field_type.set_flen(5);
    res = executeCastJsonAsStringFunction(input_col, field_type);
    ASSERT_COLUMN_EQ(res, output_col);
    ASSERT_TRUE(context->getDAGContext()->getWarningCount() == 2);

    // multiple-bytes utf characters "你好"
    // clang-format off
    UInt8 bj3[] = {0xc, 0x6, 0xe4, 0xbd, 0xa0, 0xe5, 0xa5, 0xbd};
    // clang-format on
    str_col = ColumnString::create();
    str_col->insertData(reinterpret_cast<const char *>(bj3), sizeof(bj3) / sizeof(UInt8));
    col_null_map = ColumnUInt8::create(1, 0);
    json_col = ColumnNullable::create(std::move(str_col), std::move(col_null_map));
    input_col = ColumnWithTypeAndName(std::move(json_col), nullable_string_type_ptr, "input0");

    output_col = createColumn<Nullable<String>>({R"("你)"});
    field_type.set_flen(2);
    context->getDAGContext()->clearWarnings();
    res = executeCastJsonAsStringFunction(input_col, field_type);
    ASSERT_TRUE(context->getDAGContext()->getWarningCount() == 1);
    ASSERT_COLUMN_EQ(res, output_col);

    output_col = createColumn<Nullable<String>>({R"()"});
    field_type.set_flen(0);
    context->getDAGContext()->clearWarnings();
    res = executeCastJsonAsStringFunction(input_col, field_type);
    ASSERT_TRUE(context->getDAGContext()->getWarningCount() == 1);
    ASSERT_COLUMN_EQ(res, output_col);

    output_col = createColumn<Nullable<String>>({R"("你好")"});
    field_type.set_flen(-1);
    context->getDAGContext()->clearWarnings();
    res = executeCastJsonAsStringFunction(input_col, field_type);
    ASSERT_COLUMN_EQ(res, output_col);
    ASSERT_TRUE(context->getDAGContext()->getWarningCount() == 0);

    output_col = createColumn<Nullable<String>>({R"("你好")"});
    field_type.set_flen(4);
    context->getDAGContext()->clearWarnings();
    res = executeCastJsonAsStringFunction(input_col, field_type);
    ASSERT_COLUMN_EQ(res, output_col);
    ASSERT_TRUE(context->getDAGContext()->getWarningCount() == 0);

    output_col = createColumn<Nullable<String>>({R"("你好")"});
    field_type.set_flen(10);
    context->getDAGContext()->clearWarnings();
    res = executeCastJsonAsStringFunction(input_col, field_type);
    ASSERT_COLUMN_EQ(res, output_col);
    ASSERT_TRUE(context->getDAGContext()->getWarningCount() == 0);
}
CATCH

} // namespace DB::tests