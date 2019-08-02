#pragma once

#include <unordered_map>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Storages/Transaction/Types.h>

namespace DB
{

bool isLiteralExpr(const tipb::Expr & expr);
Field decodeLiteral(const tipb::Expr & expr);
bool isFunctionExpr(const tipb::Expr & expr);
bool isAggFunctionExpr(const tipb::Expr & expr);
const String & getFunctionName(const tipb::Expr & expr);
bool isColumnExpr(const tipb::Expr & expr);
ColumnID getColumnID(const tipb::Expr & expr);
String getName(const tipb::Expr & expr, const NamesAndTypesList & current_input_columns);
const String & getTypeName(const tipb::Expr & expr);
String exprToString(const tipb::Expr & expr, const NamesAndTypesList & input_col);
extern std::unordered_map<tipb::ExprType, String> aggFunMap;
extern std::unordered_map<tipb::ScalarFuncSig, String> scalarFunMap;

} // namespace DB
