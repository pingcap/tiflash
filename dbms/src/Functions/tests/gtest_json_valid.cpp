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
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <Interpreters/ExpressionActions.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Decode/JsonBinary.h>
#include <TiDB/Schema/TiDB.h>

#include <string>
#include <vector>

namespace DB::tests
{
class TestJsonValid : public DB::tests::FunctionTest
{
};

TEST_F(TestJsonValid, TestJsonValidOthers)
try
{
    static constexpr auto func_name = "json_valid_others";
    {
        auto input = createColumn<Int64>({10, 100, 29, 22});
        ASSERT_COLUMN_EQ(createConstColumn<UInt8>(input.column->size(), false), executeFunction(func_name, input));
    }
    {
        auto input = createColumn<UInt64>({0, 10, 100, 29, 22});
        ASSERT_COLUMN_EQ(createConstColumn<UInt8>(input.column->size(), false), executeFunction(func_name, input));
    }
    {
        auto input = createColumn<UInt8>({1, 1, 0, 0, 1, 0});
        ASSERT_COLUMN_EQ(createConstColumn<UInt8>(input.column->size(), false), executeFunction(func_name, input));
    }
    {
        // Although it is stated in https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-valid that returns NULL if the argument is NULL,
        // both MySQL and TiDB will directly return false instead of NULL.
        auto input = createOnlyNullColumn(10);
        ASSERT_COLUMN_EQ(createConstColumn<UInt8>(input.column->size(), false), executeFunction(func_name, input));
    }
}
CATCH

TEST_F(TestJsonValid, TestJsonValidJson)
try
{
    static constexpr auto func_name = "json_valid_json";
    auto input = createColumn<String>({"{}", "[]", "1", "\"fdfd\""});
    // Use string as input column to improve readability.
    ColumnsWithTypeAndName origin_inputs{input};
    auto json_column = executeFunction("cast_string_as_json", origin_inputs, nullptr, true);
    ASSERT_COLUMN_EQ(createConstColumn<UInt8>(input.column->size(), true), executeFunction(func_name, json_column));
}
CATCH

TEST_F(TestJsonValid, TestJsonValidString)
try
{
    auto execute_and_assert = [&](const String & input, bool expect) {
        ASSERT_COLUMN_EQ(
            createColumn<UInt8>({expect, expect}),
            executeFunction("json_valid_string", {createColumn<String>({input, input})}));
    };

    execute_and_assert("", false);
    execute_and_assert("[]", true);
    execute_and_assert("{}", true);
    execute_and_assert("1", true);
    execute_and_assert("-991", true);
    execute_and_assert("1.111", true);
    execute_and_assert("-991.111", true);
    execute_and_assert("-991gdfgf.111", false);
    execute_and_assert("gsdfgsdf", false);
    execute_and_assert("\"gsdfgsdf\"", true);
    execute_and_assert(R"({"a":[]})", true);
    execute_and_assert(R"({"a":[], "b" :1, "c" :{}, "d":"d"})", true);
}
CATCH

TEST_F(TestJsonValid, GuardStringToJsonParsingInFilter)
try
{
    getDAGContext().log = Logger::get("TestJsonValid");

    auto make_field_type = [](Int32 tp, UInt32 flag = 0) {
        tipb::FieldType field_type;
        field_type.set_tp(tp);
        field_type.set_flag(flag);
        return field_type;
    };
    auto make_column_ref = [&] {
        tipb::Expr expr;
        expr.set_tp(tipb::ExprType::ColumnRef);
        WriteBufferFromOwnString ss;
        encodeDAGInt64(0, ss);
        expr.set_val(ss.releaseStr());
        *expr.mutable_field_type() = make_field_type(TiDB::TypeString);
        return expr;
    };
    auto make_scalar = [](tipb::ScalarFuncSig sig, const tipb::FieldType & field_type) {
        tipb::Expr expr;
        expr.set_tp(tipb::ExprType::ScalarFunc);
        expr.set_sig(sig);
        *expr.mutable_field_type() = field_type;
        return expr;
    };

    const auto column_ref = make_column_ref();
    auto json_valid = make_scalar(
        tipb::ScalarFuncSig::JsonValidStringSig,
        make_field_type(TiDB::TypeLongLong, TiDB::ColumnFlagIsBooleanFlag));
    *json_valid.add_children() = column_ref;

    auto cast_json = make_scalar(
        tipb::ScalarFuncSig::CastStringAsJson,
        make_field_type(TiDB::TypeJSON, TiDB::ColumnFlagParseToJSON));
    *cast_json.add_children() = column_ref;

    auto is_null = make_scalar(
        tipb::ScalarFuncSig::StringIsNull,
        make_field_type(TiDB::TypeLongLong, TiDB::ColumnFlagIsBooleanFlag));
    *is_null.add_children() = cast_json;

    auto is_not_null = make_scalar(
        tipb::ScalarFuncSig::UnaryNotInt,
        make_field_type(TiDB::TypeLongLong, TiDB::ColumnFlagIsBooleanFlag));
    *is_not_null.add_children() = is_null;

    auto execute_filter = [&](const google::protobuf::RepeatedPtrField<tipb::Expr> & conditions) {
        Block block({createColumn<String>({"", "invalid json", R"({"a": 1})"}, "json")});
        auto actions = std::make_shared<ExpressionActions>(block.getColumnsWithTypeAndName());
        DAGExpressionAnalyzer analyzer(block, *context);
        const auto filter_column = analyzer.buildFilterColumn(actions, conditions, true);
        actions->execute(block);
        return block.getByName(filter_column);
    };

    google::protobuf::RepeatedPtrField<tipb::Expr> guarded_conditions;
    *guarded_conditions.Add() = json_valid;
    *guarded_conditions.Add() = is_not_null;
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 0, 1}), execute_filter(guarded_conditions));

    google::protobuf::RepeatedPtrField<tipb::Expr> reversed_conditions;
    *reversed_conditions.Add() = is_not_null;
    *reversed_conditions.Add() = json_valid;
    ASSERT_THROW(execute_filter(reversed_conditions), Exception);

    google::protobuf::RepeatedPtrField<tipb::Expr> unguarded_conditions;
    *unguarded_conditions.Add() = is_not_null;
    ASSERT_THROW(execute_filter(unguarded_conditions), Exception);

    auto nested_and = make_scalar(
        tipb::ScalarFuncSig::LogicalAnd,
        make_field_type(TiDB::TypeLongLong, TiDB::ColumnFlagIsBooleanFlag));
    *nested_and.add_children() = json_valid;
    *nested_and.add_children() = json_valid;
    auto guarded_nested_and = make_scalar(
        tipb::ScalarFuncSig::LogicalAnd,
        make_field_type(TiDB::TypeLongLong, TiDB::ColumnFlagIsBooleanFlag));
    *guarded_nested_and.add_children() = nested_and;
    *guarded_nested_and.add_children() = is_not_null;
    google::protobuf::RepeatedPtrField<tipb::Expr> nested_conditions;
    *nested_conditions.Add() = guarded_nested_and;
    ASSERT_COLUMN_EQ(createColumn<UInt8>({0, 0, 1}), execute_filter(nested_conditions));

    auto guarded_and = make_scalar(
        tipb::ScalarFuncSig::LogicalAnd,
        make_field_type(TiDB::TypeLongLong, TiDB::ColumnFlagIsBooleanFlag));
    *guarded_and.add_children() = json_valid;
    *guarded_and.add_children() = is_not_null;
    auto unguarded_or = make_scalar(
        tipb::ScalarFuncSig::LogicalOr,
        make_field_type(TiDB::TypeLongLong, TiDB::ColumnFlagIsBooleanFlag));
    *unguarded_or.add_children() = guarded_and;
    *unguarded_or.add_children() = is_not_null;
    google::protobuf::RepeatedPtrField<tipb::Expr> or_conditions;
    *or_conditions.Add() = unguarded_or;
    ASSERT_THROW(execute_filter(or_conditions), Exception);

    auto non_and_wrapper
        = make_scalar(tipb::ScalarFuncSig::EQInt, make_field_type(TiDB::TypeLongLong, TiDB::ColumnFlagIsBooleanFlag));
    *non_and_wrapper.add_children() = guarded_and;
    *non_and_wrapper.add_children() = is_not_null;
    google::protobuf::RepeatedPtrField<tipb::Expr> wrapped_conditions;
    *wrapped_conditions.Add() = non_and_wrapper;
    ASSERT_THROW(execute_filter(wrapped_conditions), Exception);
}
CATCH

} // namespace DB::tests
