#pragma once

#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Core/SortDescription.h>
#include <Storages/Transaction/Collator.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>
#include <grpcpp/impl/codegen/status_code_enum.h>
#include <tipb/select.pb.h>

#include <unordered_map>

namespace DB
{
class DAGContext;

bool isLiteralExpr(const tipb::Expr & expr);
Field decodeLiteral(const tipb::Expr & expr);
bool isFunctionExpr(const tipb::Expr & expr);
bool isScalarFunctionExpr(const tipb::Expr & expr);
bool isAggFunctionExpr(const tipb::Expr & expr);
bool isWindowFunctionExpr(const tipb::Expr & expr);
bool isWindowLagOrLeadFunctionExpr(const tipb::Expr & expr);
const String & getFunctionName(const tipb::Expr & expr);
const String & getAggFunctionName(const tipb::Expr & expr);
const String & getWindowFunctionName(const tipb::Expr & expr);
bool isColumnExpr(const tipb::Expr & expr);
String getColumnNameForColumnExpr(const tipb::Expr & expr, const std::vector<NameAndTypePair> & input_col);
const String & getTypeName(const tipb::Expr & expr);
String exprToString(const tipb::Expr & expr, const std::vector<NameAndTypePair> & input_col);
bool exprHasValidFieldType(const tipb::Expr & expr);
tipb::Expr constructStringLiteralTiExpr(const String & value);
tipb::Expr constructInt64LiteralTiExpr(Int64 value);
tipb::Expr constructDateTimeLiteralTiExpr(UInt64 packed_value);
tipb::Expr constructNULLLiteralTiExpr();
DataTypePtr inferDataType4Literal(const tipb::Expr & expr);
SortDescription getSortDescription(
    const std::vector<NameAndTypePair> & order_columns,
    const google::protobuf::RepeatedPtrField<tipb::ByItem> & by_items);
String genFuncString(
    const String & func_name,
    const Names & argument_names,
    const TiDB::TiDBCollators & collators);

extern const Int8 VAR_SIZE;

UInt8 getFieldLengthForArrowEncode(Int32 tp);
bool isUnsupportedEncodeType(const std::vector<tipb::FieldType> & types, tipb::EncodeType encode_type);
TiDB::TiDBCollatorPtr getCollatorFromExpr(const tipb::Expr & expr);
TiDB::TiDBCollatorPtr getCollatorFromFieldType(const tipb::FieldType & field_type);
bool hasUnsignedFlag(const tipb::FieldType & tp);
grpc::StatusCode tiflashErrorCodeToGrpcStatusCode(int error_code);

void assertBlockSchema(
    const DataTypes & expected_types,
    const Block & block,
    const String & context_description);

void assertBlockSchema(
    const Block & header,
    const Block & block,
    const String & context_description);

class UniqueNameGenerator
{
private:
    std::unordered_map<String, Int32> existing_name_map;

public:
    String toUniqueName(const String & orig_name)
    {
        String ret_name = orig_name;
        auto it = existing_name_map.find(ret_name);
        while (it != existing_name_map.end())
        {
            ret_name.append("_").append(std::to_string(it->second));
            it->second++;
            it = existing_name_map.find(ret_name);
        }
        existing_name_map.try_emplace(ret_name, 1);
        return ret_name;
    }
};

tipb::DAGRequest getDAGRequestFromStringWithRetry(const String & s);
tipb::EncodeType analyzeDAGEncodeType(DAGContext & dag_context);

} // namespace DB
