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
#include <TiDB/Schema/TiDBTypes.h>
#include <gtest/gtest.h>

#include <limits>

namespace DB::tests
{
class TestJsonObject : public DB::tests::FunctionTest
{
public:
    ColumnWithTypeAndName castStringToJson(const ColumnWithTypeAndName & column)
    {
        assert(removeNullable(column.type)->isString());
        ColumnsWithTypeAndName inputs{column};
        return executeFunction("cast_string_as_json", inputs, nullptr, true);
    }

    ColumnWithTypeAndName executeFunctionWithCast(
        const ColumnNumbers & argument_column_numbers,
        const ColumnsWithTypeAndName & columns)
    {
        auto json_column = executeFunction("json_object", argument_column_numbers, columns);
        tipb::FieldType field_type;
        field_type.set_flen(-1);
        field_type.set_collate(TiDB::ITiDBCollator::BINARY);
        field_type.set_tp(TiDB::TypeString);
        return executeCastJsonAsStringFunction(json_column, field_type);
    }
};

TEST_F(TestJsonObject, TestBasicSemantics)
try
{
    constexpr size_t rows_count = 2;

    {
        ColumnsWithTypeAndName inputs{createColumn<String>({"placeholder", "placeholder"})};
        auto res = executeFunctionWithCast({}, inputs);
        ASSERT_COLUMN_EQ(createConstColumn<Nullable<String>>(rows_count, "{}"), res);
    }

    {
        ColumnsWithTypeAndName inputs{
            createColumn<String>({"b", "b"}),
            castStringToJson(createColumn<String>({"1", "1"})),
            createColumn<String>({"a", "a"}),
            castStringToJson(createColumn<Nullable<String>>({{}, "\"x\""})),
        };
        auto res = executeFunctionWithCast({0, 1, 2, 3}, inputs);
        auto expect = createColumn<Nullable<String>>(
            {R"({"a": null, "b": 1})", R"({"a": "x", "b": 1})"});
        ASSERT_COLUMN_EQ(expect, res);
    }

    {
        ColumnsWithTypeAndName inputs{
            createConstColumn<String>(rows_count, "dup"),
            castStringToJson(createConstColumn<String>(rows_count, "1")),
            createConstColumn<String>(rows_count, "dup"),
            castStringToJson(createColumn<String>({"2", "3"})),
        };
        auto res = executeFunctionWithCast({0, 1, 2, 3}, inputs);
        auto expect = createColumn<Nullable<String>>({R"({"dup": 2})", R"({"dup": 3})"});
        ASSERT_COLUMN_EQ(expect, res);
    }
}
CATCH

TEST_F(TestJsonObject, TestErrors)
try
{
    ASSERT_THROW(executeFunction("json_object", {createColumn<String>({"a"})}), Exception);

    auto value = castStringToJson(createColumn<String>({"1"}));
    ASSERT_THROW(executeFunction("json_object", {createColumn<Nullable<String>>({{}}), value}), Exception);

    String too_long_key(std::numeric_limits<UInt16>::max() + 1, 'a');
    ASSERT_THROW(executeFunction("json_object", {createColumn<String>({too_long_key}), value}), Exception);
}
CATCH

} // namespace DB::tests
