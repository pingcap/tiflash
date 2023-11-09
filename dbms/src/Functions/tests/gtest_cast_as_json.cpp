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

#include <string>
#include <vector>

namespace DB::tests
{
/**
  * Because all the functions that cast xx as json have A, 
  * bool useDefaultImplementationForNulls() const override { return true; }
  * bool useDefaultImplementationForConstants() const override { return true; }
  * there is no need to test const, null_value, and only value.
  */
class TestCastAsJson : public DB::tests::FunctionTest
{
public:
    ColumnWithTypeAndName executeFunctionWithCast(
        const String & func_name,
        const ColumnsWithTypeAndName & columns)
    {
        auto json_column = executeFunction(func_name, columns);
        static auto json_return_type = std::make_shared<DataTypeString>();
        assert(json_return_type->equals(*json_column.type));
        // The `json_binary` should be cast as a string to improve readability.
        return executeFunction("cast_json_as_string", {json_column});
    }
};

TEST_F(TestCastAsJson, CastJsonAsJson)
try
{
    /// prepare
    // []
    // clang-format off
    const UInt8 empty_array[] = {
        JsonBinary::TYPE_CODE_ARRAY, // array_type
        0x0, 0x0, 0x0, 0x0, // element_count
        0x8, 0x0, 0x0, 0x0}; // total_size
    // clang-format on
    ColumnWithTypeAndName json_column;
    {
        auto empty_array_json = ColumnString::create();
        empty_array_json->insertData(reinterpret_cast<const char *>(empty_array), sizeof(empty_array) / sizeof(UInt8));
        empty_array_json->insertData(reinterpret_cast<const char *>(empty_array), sizeof(empty_array) / sizeof(UInt8));
        json_column = ColumnWithTypeAndName(std::move(empty_array_json), std::make_shared<DataTypeString>());
    }

    auto gen_column_expect = [](const String & value) {
        // double for rows_count 2.
        return createColumn<Nullable<String>>({value, value});
    };

    auto res = executeFunctionWithCast("cast_json_as_json", {json_column});
    auto expect = gen_column_expect("[]");
    ASSERT_COLUMN_EQ(expect, res);
}
CATCH

} // namespace DB::tests
