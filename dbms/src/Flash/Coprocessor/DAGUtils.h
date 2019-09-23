#pragma once

#include <unordered_map>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

bool isLiteralExpr(const tipb::Expr & expr);
Field decodeLiteral(const tipb::Expr & expr);
bool isFunctionExpr(const tipb::Expr & expr);
bool isAggFunctionExpr(const tipb::Expr & expr);
const String & getFunctionName(const tipb::Expr & expr);
const String & getAggFunctionName(const tipb::Expr & expr);
bool isColumnExpr(const tipb::Expr & expr);
ColumnID getColumnID(const tipb::Expr & expr);
String getName(const tipb::Expr & expr, const NamesAndTypesList & current_input_columns);
const String & getTypeName(const tipb::Expr & expr);
String exprToString(const tipb::Expr & expr, const NamesAndTypesList & input_col, bool for_parser = true);
bool isInOrGlobalInOperator(const String & name);
bool exprHasValidFieldType(const tipb::Expr & expr);
extern std::unordered_map<tipb::ExprType, String> agg_func_map;
extern std::unordered_map<tipb::ScalarFuncSig, String> scalar_func_map;

tipb::FieldType columnInfoToFieldType(const TiDB::ColumnInfo & ci);
TiDB::ColumnInfo fieldTypeToColumnInfo(const tipb::FieldType & field_type);

} // namespace DB
