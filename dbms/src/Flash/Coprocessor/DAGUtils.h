#pragma once

#include <unordered_map>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Storages/Transaction/Collator.h>
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
String getColumnNameForColumnExpr(const tipb::Expr & expr, const std::vector<NameAndTypePair> & input_col);
const String & getTypeName(const tipb::Expr & expr);
String exprToString(const tipb::Expr & expr, const std::vector<NameAndTypePair> & input_col);
bool exprHasValidFieldType(const tipb::Expr & expr);
void constructStringLiteralTiExpr(tipb::Expr & expr, const String & value);
void constructInt64LiteralTiExpr(tipb::Expr & expr, Int64 value);
void constructDateTimeLiteralTiExpr(tipb::Expr & expr, UInt64 packed_value);
void constructNULLLiteralTiExpr(tipb::Expr & expr);
extern std::unordered_map<tipb::ExprType, String> agg_func_map;
extern std::unordered_map<tipb::ScalarFuncSig, String> scalar_func_map;
extern const Int8 VAR_SIZE;

tipb::FieldType columnInfoToFieldType(const TiDB::ColumnInfo & ci);
TiDB::ColumnInfo fieldTypeToColumnInfo(const tipb::FieldType & field_type);
UInt8 getFieldLengthForArrowEncode(Int32 tp);
bool isUnsupportedEncodeType(const std::vector<tipb::FieldType> & types, tipb::EncodeType encode_type);
std::shared_ptr<TiDB::ITiDBCollator> getCollatorFromExpr(const tipb::Expr & expr);
std::shared_ptr<TiDB::ITiDBCollator> getCollatorFromFieldType(const tipb::FieldType & field_type);
bool hasUnsignedFlag(const tipb::FieldType & tp);

} // namespace DB
