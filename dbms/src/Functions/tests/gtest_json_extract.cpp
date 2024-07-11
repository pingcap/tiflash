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
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/JsonBinary.h>

#include <string>
#include <vector>

namespace DB::tests
{
class TestJsonExtract : public DB::tests::FunctionTest
{
public:
    static constexpr auto func_name = "json_extract";

    ColumnWithTypeAndName castStringToJson(const ColumnWithTypeAndName & column)
    {
        assert(removeNullable(column.type)->isString());
        ColumnsWithTypeAndName origin_inputs{column};
        return executeFunction("cast_string_as_json", origin_inputs, nullptr, true);
    }

    static void checkResult(ColumnPtr column, std::vector<UInt8> & null_vec, std::vector<String> & data)
    {
        bool is_const_res = false;
        if (column->isColumnConst())
        {
            column = static_cast<const ColumnConst *>(column.get())->getDataColumnPtr();
            is_const_res = true;
        }
        ASSERT_TRUE(column->isColumnNullable());
        auto result_not_nullable = static_cast<const ColumnNullable &>(*column).getNestedColumnPtr();
        const auto * result_col = checkAndGetColumn<ColumnString>(result_not_nullable.get());
        ASSERT_TRUE(result_col);
        const ColumnPtr & null_map_column = static_cast<const ColumnNullable &>(*column).getNullMapColumnPtr();
        const NullMap & result_null_map = static_cast<const ColumnUInt8 &>(*null_map_column).getData();
        for (size_t i = 0; i < null_vec.size(); ++i)
        {
            auto result_data_index = i;
            if (is_const_res)
                result_data_index = 0;

            ASSERT_TRUE(result_null_map[result_data_index] == null_vec[i]);
            if (!null_vec[i])
            {
                const auto & str_ref = result_col->getDataAt(result_data_index);
                JsonBinary json(str_ref.data[0], StringRef(str_ref.data + 1, str_ref.size - 1));
                ASSERT_TRUE(json.toString() == data[i]);
            }
        }
    }
};

TEST_F(TestJsonExtract, TestAllPathConst)
try
{
    /// Normal case: ColumnVector(nullable)
    static auto const nullable_string_type_ptr = makeNullable(std::make_shared<DataTypeString>());
    static auto const string_type_ptr = std::make_shared<DataTypeString>();
    auto str_col = ColumnString::create();
    /// `[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`
    /// Avoid expanding const arrays one element per line
    // clang-format off
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
    vec_null_map[1] = 1;
    auto json_col = ColumnNullable::create(std::move(str_col), std::move(col_null_map));
    auto input_col = ColumnWithTypeAndName(std::move(json_col), nullable_string_type_ptr, "input0");

    auto path_col = createConstColumn<Nullable<String>>(3, {"$[1]"});
    auto res = executeFunction(func_name, {input_col, path_col});
    std::vector<UInt8> expect_null_vec{0, 1, 0};
    std::vector<String> expect_string_vec{"3", "", "[2, 3]"};
    checkResult(res.column, expect_null_vec, expect_string_vec);

    /// ColumnVector(null)
    str_col = ColumnString::create();
    str_col->insertData("", 0);
    str_col->insertData("", 0);
    str_col->insertData("", 0);
    col_null_map = ColumnUInt8::create(3, 1);
    json_col = ColumnNullable::create(std::move(str_col), std::move(col_null_map));
    input_col = ColumnWithTypeAndName(std::move(json_col), nullable_string_type_ptr, "input0");
    path_col = createConstColumn<Nullable<String>>(3, {"$[1]"});
    res = executeFunction(func_name, {input_col, path_col});
    expect_null_vec = {1, 1, 1};
    expect_string_vec = {"", "", ""};
    checkResult(res.column, expect_null_vec, expect_string_vec);

    /// Path is constant null
    auto null_path_col = createConstColumn<Nullable<String>>(3, {});
    res = executeFunction(func_name, {input_col, null_path_col});
    expect_null_vec = {1, 1, 1};
    expect_string_vec = {"", "", ""};
    checkResult(res.column, expect_null_vec, expect_string_vec);

    /// JsonBinary is constant null
    auto null_json = ColumnString::create();
    null_json->insertData("", 0);
    col_null_map = ColumnUInt8::create(1, 1);
    json_col = ColumnNullable::create(std::move(null_json), std::move(col_null_map));
    auto const_null_json_col = ColumnConst::create(std::move(json_col), 3);
    auto const_null_input_col
        = ColumnWithTypeAndName(std::move(const_null_json_col), nullable_string_type_ptr, "input0");
    res = executeFunction(func_name, {const_null_input_col, path_col});
    ASSERT_TRUE(res.column->size() == 3);
    expect_string_vec = {"", "", ""};
    expect_null_vec = {1, 1, 1};
    checkResult(res.column, expect_null_vec, expect_string_vec);

    /// JsonBinary only null
    auto const_null_only_col = createOnlyNullColumnConst(3);
    res = executeFunction(func_name, {const_null_only_col, path_col});
    ASSERT_TRUE(res.column->size() == 3);
    expect_string_vec = {"", "", ""};
    expect_null_vec = {1, 1, 1};
    checkResult(res.column, expect_null_vec, expect_string_vec);

    /// ColumnVector(non-null)
    auto non_null_str_col = ColumnString::create();
    non_null_str_col->insertData(reinterpret_cast<const char *>(bj2), sizeof(bj2) / sizeof(UInt8));
    non_null_str_col->insertData(reinterpret_cast<const char *>(bj2), sizeof(bj2) / sizeof(UInt8));
    non_null_str_col->insertData(reinterpret_cast<const char *>(bj9), sizeof(bj9) / sizeof(UInt8));
    auto non_null_input_col = ColumnWithTypeAndName(std::move(non_null_str_col), string_type_ptr, "input0");
    auto non_null_path_col = createConstColumn<String>(3, "$[1]");
    res = executeFunction(func_name, {non_null_input_col, non_null_path_col});
    ASSERT_TRUE(res.column->size() == 3);
    expect_string_vec = {"3", "3", "[2, 3]"};
    expect_null_vec = {0, 0, 0};
    checkResult(res.column, expect_null_vec, expect_string_vec);

    /// One of Paths is only null
    res = executeFunction(func_name, {non_null_input_col, non_null_path_col, const_null_only_col});
    ASSERT_TRUE(res.column->size() == 3);
    expect_string_vec = {"", "", ""};
    expect_null_vec = {1, 1, 1};
    checkResult(res.column, expect_null_vec, expect_string_vec);

    /// ColumnConst(non-null)
    non_null_str_col = ColumnString::create();
    non_null_str_col->insertData(reinterpret_cast<const char *>(bj2), sizeof(bj2) / sizeof(UInt8));
    auto const_non_null_input_col = ColumnConst::create(std::move(non_null_str_col), 3);
    res = executeFunction(func_name, {{std::move(const_non_null_input_col), string_type_ptr, ""}, non_null_path_col});
    ASSERT_TRUE(res.column->size() == 3);
    expect_string_vec = {"3", "3", "3"};
    expect_null_vec = {0, 0, 0};
    checkResult(res.column, expect_null_vec, expect_string_vec);

    /// ColumnConst(nullable)
    auto nested_str_col = ColumnString::create();
    nested_str_col->insertData(reinterpret_cast<const char *>(bj9), sizeof(bj9) / sizeof(UInt8));
    col_null_map = ColumnUInt8::create(1, 0);
    json_col = ColumnNullable::create(std::move(nested_str_col), std::move(col_null_map));
    auto const_json_col = ColumnConst::create(std::move(json_col), 3);
    auto const_nullable_input_col
        = ColumnWithTypeAndName(std::move(const_json_col), nullable_string_type_ptr, "input0");
    res = executeFunction(func_name, {const_nullable_input_col, path_col});
    ASSERT_TRUE(res.column->size() == 3);
    expect_string_vec = {"[2, 3]", "[2, 3]", "[2, 3]"};
    expect_null_vec = {0, 0, 0};
    checkResult(res.column, expect_null_vec, expect_string_vec);
}
CATCH

TEST_F(TestJsonExtract, TestOnlyNull)
try
{
    size_t rows_count = 2;
    ColumnWithTypeAndName json_column = castStringToJson(createColumn<Nullable<String>>({"[]", "[]"}));
    ColumnWithTypeAndName path_column = createColumn<Nullable<String>>({"$", "$"});
    ColumnWithTypeAndName path_column2 = createColumn<Nullable<String>>({"$.a", "$.a"});
    ColumnWithTypeAndName null_string_const = createConstColumn<Nullable<String>>(rows_count, {});
    ColumnWithTypeAndName only_null_const = createOnlyNullColumnConst(rows_count);

    ASSERT_COLUMN_EQ(json_column, executeFunction(func_name, json_column, path_column));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, only_null_const, path_column));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, null_string_const, path_column));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, json_column, only_null_const, path_column));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, json_column, null_string_const, path_column));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, json_column, only_null_const));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, json_column, null_string_const));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, json_column, path_column, only_null_const));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, json_column, path_column, null_string_const));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, json_column, path_column2, only_null_const));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, json_column, path_column2, null_string_const));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, json_column, only_null_const, path_column));
    ASSERT_COLUMN_EQ(null_string_const, executeFunction(func_name, json_column, null_string_const, path_column));

    // path const.
    ASSERT_COLUMN_EQ(
        null_string_const,
        executeFunction(func_name, json_column, createConstColumn<String>(2, "$"), null_string_const));
    ASSERT_COLUMN_EQ(
        null_string_const,
        executeFunction(func_name, json_column, createConstColumn<String>(2, "$"), only_null_const));
    ASSERT_COLUMN_EQ(
        null_string_const,
        executeFunction(func_name, json_column, null_string_const, createConstColumn<String>(2, "$")));
    ASSERT_COLUMN_EQ(
        null_string_const,
        executeFunction(func_name, json_column, only_null_const, createConstColumn<String>(2, "$")));
}
CATCH

TEST_F(TestJsonExtract, TestHasColumnPath)
try
{
    auto exec_assert = [&](const std::optional<String> & json,
                           const std::optional<String> & path,
                           const std::optional<String> & expect) {
        ASSERT_COLUMN_EQ(
            castStringToJson(createColumn<Nullable<String>>({expect, expect})),
            executeFunction(
                func_name,
                {castStringToJson(createColumn<Nullable<String>>({json, json})),
                 createColumn<Nullable<String>>({path, path})}));

        auto const_json_expect = json ? castStringToJson(createColumn<Nullable<String>>({expect, expect}))
                                      : createConstColumn<Nullable<String>>(2, {});
        ASSERT_COLUMN_EQ(
            const_json_expect,
            executeFunction(
                func_name,
                {castStringToJson(createConstColumn<Nullable<String>>(2, json)),
                 createColumn<Nullable<String>>({path, path})}));
    };

    exec_assert("{}", "$", "{}");
    exec_assert("{}", "$.a", {});
    exec_assert("{}", {}, {});
    exec_assert({}, "$", {});
}
CATCH

} // namespace DB::tests
