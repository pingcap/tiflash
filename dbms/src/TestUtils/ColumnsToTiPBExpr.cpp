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
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Debug/MockExecutor/AstToPB.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <TestUtils/ColumnsToTiPBExpr.h>
#include <TiDB/Decode/TypeMapping.h>

namespace DB
{
namespace tests
{
namespace
{
void columnToTiPBExpr(tipb::Expr * expr, const ColumnWithTypeAndName column, size_t index)
{
    auto ci = reverseGetColumnInfo({column.name, column.type}, 0, Field(), true);
    bool is_const = false;
    if (column.column != nullptr)
    {
        is_const = column.column->isColumnConst();
        if (!is_const)
        {
            if (column.column->isColumnNullable())
            {
                auto [col, null_map] = removeNullable(column.column.get());
                (void)null_map;
                is_const = col->isColumnConst();
            }
        }
    }
    if (is_const)
    {
        Field val_field;
        column.column->get(0, val_field);
        literalFieldToTiPBExpr(ci, val_field, expr, 0);
    }
    else
    {
        *(expr->mutable_field_type()) = columnInfoToFieldType(ci);
        expr->set_tp(tipb::ExprType::ColumnRef);
        WriteBufferFromOwnString ss;
        encodeDAGInt64(index, ss);
        expr->set_val(ss.releaseStr());
    }
}
void columnsToTiPBExprForRegExp(
    tipb::Expr * expr,
    const String &,
    const ColumnNumbers & argument_column_number,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator)
{
    expr->set_tp(tipb::ExprType::ScalarFunc);
    if (collator == nullptr || !collator->isBinary())
        expr->set_sig(tipb::ScalarFuncSig::RegexpUTF8Sig);
    else
        expr->set_sig(tipb::ScalarFuncSig::RegexpSig);
    for (size_t i = 0; i < argument_column_number.size(); ++i)
    {
        auto * argument_expr = expr->add_children();
        columnToTiPBExpr(argument_expr, columns[argument_column_number[i]], i);
    }
    /// since we don't know the type, just set a fake one
    expr->mutable_field_type()->set_tp(TiDB::TypeLongLong);
    if (collator != nullptr)
        expr->mutable_field_type()->set_collate(-collator->getCollatorId());
}
void columnsToTiPBExprForTiDBCast(
    tipb::Expr * expr,
    const String & func_name,
    const ColumnNumbers & argument_column_number,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator)
{
    expr->set_tp(tipb::ExprType::ScalarFunc);
    expr->set_sig(reverseGetFuncSigByFuncName(func_name));
    assert(argument_column_number.size() == 2);
    const auto & type_column = columns[argument_column_number[1]];
    bool is_const = false;
    if (type_column.column != nullptr)
    {
        is_const = type_column.column->isColumnConst();
        if (!is_const)
        {
            if (type_column.column->isColumnNullable())
            {
                auto [col, null_map] = removeNullable(type_column.column.get());
                (void)null_map;
                is_const = col->isColumnConst();
            }
        }
    }
    assert(is_const && removeNullable(type_column.type)->isString());
    Field val;
    type_column.column->get(0, val);
    String type_string = val.safeGet<String>();
    DataTypePtr target_type = DataTypeFactory::instance().get(type_string);
    auto * argument_expr = expr->add_children();
    columnToTiPBExpr(argument_expr, columns[argument_column_number[0]], 0);
    auto ci = reverseGetColumnInfo({type_string, target_type}, 0, Field(), true);
    if (ci.tp == TiDB::TypeString)
    {
        ci.flen = -1;
    }
    *(expr->mutable_field_type()) = columnInfoToFieldType(ci);
    if (collator != nullptr)
        expr->mutable_field_type()->set_collate(-collator->getCollatorId());
}

const std::unordered_map<String, String> date_add_sub_map({
    {"addDays", "DAY"},
    {"addWeeks", "WEEK"},
    {"addMonths", "MONTH"},
    {"addYears", "YEAR"},
    {"addHours", "HOUR"},
    {"addMinutes", "MINUTE"},
    {"addSeconds", "SECOND"},
    {"subtractDays", "DAY"},
    {"subtractWeeks", "WEEK"},
    {"subtractMonths", "MONTH"},
    {"subtractYears", "YEAR"},
    {"subtractHours", "HOUR"},
    {"subtractMinutes", "MINUTE"},
    {"subtractSeconds", "SECOND"},
});

void columnsToTiPBExprForDateAddSub(
    tipb::Expr * expr,
    const String & func_name,
    const ColumnNumbers & argument_column_number,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator)
{
    String name = func_name.substr(0, 3) == "add" ? "date_add" : "date_sub";
    expr->set_tp(tipb::ExprType::ScalarFunc);
    expr->set_sig(reverseGetFuncSigByFuncName(name));
    for (size_t i = 0; i < argument_column_number.size(); ++i)
    {
        auto * argument_expr = expr->add_children();
        columnToTiPBExpr(argument_expr, columns[argument_column_number[i]], i);
    }
    String unit = date_add_sub_map.find(func_name)->second;
    *(expr->add_children()) = constructStringLiteralTiExpr(unit);
    /// since we don't know the type, just set a fake one
    expr->mutable_field_type()->set_tp(TiDB::TypeLongLong);
    if (collator != nullptr)
        expr->mutable_field_type()->set_collate(-collator->getCollatorId());
}

void columnsToTiPBExpr(
    tipb::Expr * expr,
    const String & func_name,
    const ColumnNumbers & argument_column_number,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator,
    const String & val)
{
    if (func_name == "tidb_cast")
    {
        columnsToTiPBExprForTiDBCast(expr, func_name, argument_column_number, columns, collator);
    }
    else if (func_name == "regexp")
    {
        columnsToTiPBExprForRegExp(expr, func_name, argument_column_number, columns, collator);
    }
    else if (date_add_sub_map.find(func_name) != date_add_sub_map.end())
    {
        columnsToTiPBExprForDateAddSub(expr, func_name, argument_column_number, columns, collator);
    }
    else
    {
        expr->set_val(val);
        expr->set_tp(tipb::ExprType::ScalarFunc);
        expr->set_sig(reverseGetFuncSigByFuncName(func_name));
        for (size_t i = 0; i < argument_column_number.size(); ++i)
        {
            auto * argument_expr = expr->add_children();
            columnToTiPBExpr(argument_expr, columns[argument_column_number[i]], i);
        }
        /// since we don't know the type, just set a fake one
        expr->mutable_field_type()->set_tp(TiDB::TypeLongLong);
        if (collator != nullptr)
            expr->mutable_field_type()->set_collate(-collator->getCollatorId());
    }
}
} // namespace

tipb::Expr columnToTiPBExpr(const ColumnWithTypeAndName & column, size_t index)
{
    tipb::Expr ret;
    columnToTiPBExpr(&ret, column, index);
    return ret;
}

tipb::Expr columnsToTiPBExpr(
    const String & func_name,
    const ColumnNumbers & argument_column_number,
    const ColumnsWithTypeAndName & columns,
    const TiDB::TiDBCollatorPtr & collator,
    const String & val)
{
    tipb::Expr ret;
    columnsToTiPBExpr(&ret, func_name, argument_column_number, columns, collator, val);
    return ret;
}
} // namespace tests
} // namespace DB
