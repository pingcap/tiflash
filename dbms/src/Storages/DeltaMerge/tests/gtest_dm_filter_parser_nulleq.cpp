// Copyright 2026 PingCAP, Inc.
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

#include <Common/Logger.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/TiDBTypes.h>

namespace DB::DM::tests
{

namespace
{
tipb::Expr buildColumnRefExpr(Int64 column_index, Int32 field_type)
{
    tipb::Expr col;
    col.set_tp(tipb::ExprType::ColumnRef);
    {
        WriteBufferFromOwnString ss;
        encodeDAGInt64(column_index, ss);
        col.set_val(ss.releaseStr());
    }
    auto * field_type_pb = col.mutable_field_type();
    field_type_pb->set_tp(field_type);
    field_type_pb->set_flag(0);
    return col;
}

tipb::Expr buildInt64LiteralExpr(Int64 value)
{
    tipb::Expr lit;
    lit.set_tp(tipb::ExprType::Int64);
    {
        WriteBufferFromOwnString ss;
        encodeDAGInt64(value, ss);
        lit.set_val(ss.releaseStr());
    }
    return lit;
}

tipb::Expr buildNullLiteralExpr()
{
    tipb::Expr lit;
    lit.set_tp(tipb::ExprType::Null);
    return lit;
}

tipb::Expr buildLogicalNotExpr(const tipb::Expr & child)
{
    tipb::Expr expr;
    expr.set_sig(tipb::ScalarFuncSig::UnaryNotInt);
    expr.set_tp(tipb::ExprType::ScalarFunc);
    expr.add_children()->CopyFrom(child);
    return expr;
}

String parseToDebugString(Context & context, const tipb::Expr & filter_expr)
{
    google::protobuf::RepeatedPtrField<tipb::Expr> filters;
    filters.Add()->CopyFrom(filter_expr);

    const google::protobuf::RepeatedPtrField<tipb::Expr> pushed_down_filters{};

    TiDB::ColumnInfo col;
    col.id = 1;
    TiDB::ColumnInfos column_infos = {col};

    const ColumnDefines columns_to_read = {ColumnDefine{1, "a", std::make_shared<DataTypeInt64>()}};
    FilterParser::ColumnIDToAttrMap column_id_to_attr;
    for (const auto & cd : columns_to_read)
    {
        column_id_to_attr[cd.id] = Attr{.col_name = cd.name, .col_id = cd.id, .type = cd.type};
    }

    const auto ann_query_info = tipb::ANNQueryInfo{};
    const auto fts_query_info = tipb::FTSQueryInfo{};
    static const google::protobuf::RepeatedPtrField<tipb::ColumnarIndexInfo> empty_used_indexes{};
    auto dag_query = std::make_unique<DAGQueryInfo>(
        filters,
        ann_query_info,
        fts_query_info,
        pushed_down_filters,
        empty_used_indexes,
        column_infos,
        std::vector<int>{},
        0,
        context.getTimezoneInfo());

    const auto op = DB::DM::FilterParser::parseDAGQuery(*dag_query, column_infos, column_id_to_attr, Logger::get());
    return op->toDebugString();
}
} // namespace

TEST(DMFilterParserTest, ParseNullEQ)
try
{
    auto context = DMTestEnv::getContext();

    {
        // a <=> 1 -> null_equal(a, 1)
        tipb::Expr expr;
        expr.set_sig(tipb::ScalarFuncSig::NullEQInt);
        expr.set_tp(tipb::ExprType::ScalarFunc);
        expr.add_children()->CopyFrom(buildColumnRefExpr(/*column_index*/ 0, TiDB::TypeLongLong));
        expr.add_children()->CopyFrom(buildInt64LiteralExpr(1));
        EXPECT_EQ(parseToDebugString(*context, expr), R"raw({"op":"null_equal","col":"a","value":"1"})raw");
    }

    {
        // a <=> NULL -> isnull(a)
        tipb::Expr expr;
        expr.set_sig(tipb::ScalarFuncSig::NullEQInt);
        expr.set_tp(tipb::ExprType::ScalarFunc);
        expr.add_children()->CopyFrom(buildColumnRefExpr(/*column_index*/ 0, TiDB::TypeLongLong));
        expr.add_children()->CopyFrom(buildNullLiteralExpr());
        EXPECT_EQ(parseToDebugString(*context, expr), R"raw({"op":"isnull","col":"a"})raw");
    }

    {
        // NULL <=> a -> isnull(a)
        tipb::Expr expr;
        expr.set_sig(tipb::ScalarFuncSig::NullEQInt);
        expr.set_tp(tipb::ExprType::ScalarFunc);
        expr.add_children()->CopyFrom(buildNullLiteralExpr());
        expr.add_children()->CopyFrom(buildColumnRefExpr(/*column_index*/ 0, TiDB::TypeLongLong));
        EXPECT_EQ(parseToDebugString(*context, expr), R"raw({"op":"isnull","col":"a"})raw");
    }

    {
        // 1 <=> a -> null_equal(a, 1)
        tipb::Expr expr;
        expr.set_sig(tipb::ScalarFuncSig::NullEQInt);
        expr.set_tp(tipb::ExprType::ScalarFunc);
        expr.add_children()->CopyFrom(buildInt64LiteralExpr(1));
        expr.add_children()->CopyFrom(buildColumnRefExpr(/*column_index*/ 0, TiDB::TypeLongLong));
        EXPECT_EQ(parseToDebugString(*context, expr), R"raw({"op":"null_equal","col":"a","value":"1"})raw");
    }

    {
        // not(a <=> 1) keeps the dedicated null_equal node under logical not.
        tipb::Expr expr;
        expr.set_sig(tipb::ScalarFuncSig::NullEQInt);
        expr.set_tp(tipb::ExprType::ScalarFunc);
        expr.add_children()->CopyFrom(buildColumnRefExpr(/*column_index*/ 0, TiDB::TypeLongLong));
        expr.add_children()->CopyFrom(buildInt64LiteralExpr(1));
        EXPECT_EQ(
            parseToDebugString(*context, buildLogicalNotExpr(expr)),
            R"raw({"op":"not","children":[{"op":"null_equal","col":"a","value":"1"}]})raw");
    }

    {
        // White-box regression for the NullEQJson signature.
        // DM rough set filter does not currently support JSON ColumnRef directly,
        // so use a supported ColumnRef type here and verify the NullEQJson sig still
        // lowers `<=> NULL` to `isnull(col)` once it reaches parseTiCompareExpr.
        tipb::Expr expr;
        expr.set_sig(tipb::ScalarFuncSig::NullEQJson);
        expr.set_tp(tipb::ExprType::ScalarFunc);
        expr.add_children()->CopyFrom(buildColumnRefExpr(/*column_index*/ 0, TiDB::TypeLongLong));
        expr.add_children()->CopyFrom(buildNullLiteralExpr());
        EXPECT_EQ(parseToDebugString(*context, expr), R"raw({"op":"isnull","col":"a"})raw");
    }
}
CATCH

} // namespace DB::DM::tests
