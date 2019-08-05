
#include <unordered_map>

#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Interpreters/DAGUtils.h>
#include <Storages/Transaction/Codec.h>

namespace DB
{

bool isFunctionExpr(const tipb::Expr & expr)
{
    switch (expr.tp())
    {
        case tipb::ExprType::ScalarFunc:
        case tipb::ExprType::Count:
        case tipb::ExprType::Sum:
        case tipb::ExprType::Avg:
        case tipb::ExprType::Min:
        case tipb::ExprType::Max:
        case tipb::ExprType::First:
        case tipb::ExprType::GroupConcat:
        case tipb::ExprType::Agg_BitAnd:
        case tipb::ExprType::Agg_BitOr:
        case tipb::ExprType::Agg_BitXor:
        case tipb::ExprType::Std:
        case tipb::ExprType::Stddev:
        case tipb::ExprType::StddevPop:
        case tipb::ExprType::StddevSamp:
        case tipb::ExprType::VarPop:
        case tipb::ExprType::VarSamp:
        case tipb::ExprType::Variance:
        case tipb::ExprType::JsonArrayAgg:
        case tipb::ExprType::JsonObjectAgg:
            return true;
        default:
            return false;
    }
}

const String & getAggFunctionName(const tipb::Expr & expr)
{
    if (!aggFunMap.count(expr.tp()))
    {
        throw Exception(tipb::ExprType_Name(expr.tp()) + " is not supported.");
    }
    return aggFunMap[expr.tp()];
}

const String & getFunctionName(const tipb::Expr & expr)
{
    if (isAggFunctionExpr(expr))
    {
        if (!aggFunMap.count(expr.tp()))
        {
            throw Exception(tipb::ExprType_Name(expr.tp()) + " is not supported.");
        }
        return aggFunMap[expr.tp()];
    }
    else
    {
        if (!scalarFunMap.count(expr.sig()))
        {
            throw Exception(tipb::ScalarFuncSig_Name(expr.sig()) + " is not supported.");
        }
        return scalarFunMap[expr.sig()];
    }
}

String exprToString(const tipb::Expr & expr, const NamesAndTypesList & input_col)
{
    std::stringstream ss;
    size_t cursor = 1;
    Int64 columnId = 0;
    String func_name;
    Field f;
    switch (expr.tp())
    {
        case tipb::ExprType::Null:
            return "NULL";
        case tipb::ExprType::Int64:
            return std::to_string(DecodeInt<Int64>(cursor, expr.val()));
        case tipb::ExprType::Uint64:
            return std::to_string(DecodeInt<UInt64>(cursor, expr.val()));
        case tipb::ExprType::Float32:
        case tipb::ExprType::Float64:
            return std::to_string(DecodeFloat64(cursor, expr.val()));
        case tipb::ExprType::String:
            return DecodeCompactBytes(cursor, expr.val());
        case tipb::ExprType::Bytes:
            return DecodeBytes(cursor, expr.val());
        case tipb::ExprType::ColumnRef:
            columnId = DecodeInt<Int64>(cursor, expr.val());
            if (columnId < 1 || columnId > (ColumnID)input_col.size())
            {
                throw Exception("out of bound");
            }
            return input_col.getNames()[columnId - 1];
        case tipb::ExprType::Count:
        case tipb::ExprType::Sum:
        case tipb::ExprType::Avg:
        case tipb::ExprType::Min:
        case tipb::ExprType::Max:
        case tipb::ExprType::First:
            if (!aggFunMap.count(expr.tp()))
            {
                throw Exception("not supported");
            }
            func_name = aggFunMap.find(expr.tp())->second;
            break;
        case tipb::ExprType::ScalarFunc:
            if (!scalarFunMap.count(expr.sig()))
            {
                throw Exception("not supported");
            }
            func_name = scalarFunMap.find(expr.sig())->second;
            break;
        default:
            throw Exception("not supported");
    }
    // build function expr
    if (func_name == "in")
    {
        // for in, we could not represent the function expr using func_name(param1, param2, ...)
        throw Exception("not supported");
    }
    else
    {
        ss << func_name << "(";
        bool first = true;
        for (const tipb::Expr & child : expr.children())
        {
            String s = exprToString(child, input_col);
            if (first)
            {
                first = false;
            }
            else
            {
                ss << ", ";
            }
            ss << s;
        }
        ss << ") ";
        return ss.str();
    }
}

const String & getTypeName(const tipb::Expr & expr) { return tipb::ExprType_Name(expr.tp()); }

String getName(const tipb::Expr & expr, const NamesAndTypesList & current_input_columns)
{
    return exprToString(expr, current_input_columns);
}

bool isAggFunctionExpr(const tipb::Expr & expr)
{
    switch (expr.tp())
    {
        case tipb::ExprType::Count:
        case tipb::ExprType::Sum:
        case tipb::ExprType::Avg:
        case tipb::ExprType::Min:
        case tipb::ExprType::Max:
        case tipb::ExprType::First:
        case tipb::ExprType::GroupConcat:
        case tipb::ExprType::Agg_BitAnd:
        case tipb::ExprType::Agg_BitOr:
        case tipb::ExprType::Agg_BitXor:
        case tipb::ExprType::Std:
        case tipb::ExprType::Stddev:
        case tipb::ExprType::StddevPop:
        case tipb::ExprType::StddevSamp:
        case tipb::ExprType::VarPop:
        case tipb::ExprType::VarSamp:
        case tipb::ExprType::Variance:
        case tipb::ExprType::JsonArrayAgg:
        case tipb::ExprType::JsonObjectAgg:
            return true;
        default:
            return false;
    }
}

bool isLiteralExpr(const tipb::Expr & expr)
{
    switch (expr.tp())
    {
        case tipb::ExprType::Null:
        case tipb::ExprType::Int64:
        case tipb::ExprType::Uint64:
        case tipb::ExprType::Float32:
        case tipb::ExprType::Float64:
        case tipb::ExprType::String:
        case tipb::ExprType::Bytes:
        case tipb::ExprType::MysqlBit:
        case tipb::ExprType::MysqlDecimal:
        case tipb::ExprType::MysqlDuration:
        case tipb::ExprType::MysqlEnum:
        case tipb::ExprType::MysqlHex:
        case tipb::ExprType::MysqlSet:
        case tipb::ExprType::MysqlTime:
        case tipb::ExprType::MysqlJson:
        case tipb::ExprType::ValueList:
            return true;
        default:
            return false;
    }
}

bool isColumnExpr(const tipb::Expr & expr) { return expr.tp() == tipb::ExprType::ColumnRef; }

Field decodeLiteral(const tipb::Expr & expr)
{
    size_t cursor = 0;
    switch (expr.tp())
    {
        case tipb::ExprType::MysqlBit:
        case tipb::ExprType::MysqlDecimal:
        case tipb::ExprType::MysqlDuration:
        case tipb::ExprType::MysqlEnum:
        case tipb::ExprType::MysqlHex:
        case tipb::ExprType::MysqlSet:
        case tipb::ExprType::MysqlTime:
        case tipb::ExprType::MysqlJson:
        case tipb::ExprType::ValueList:
            throw Exception("mysql type literal is not supported yet");
        default:
            return DecodeDatum(cursor, expr.val());
    }
}

ColumnID getColumnID(const tipb::Expr & expr)
{
    size_t cursor = 1;
    return DecodeInt<Int64>(cursor, expr.val());
}

std::unordered_map<tipb::ExprType, String> aggFunMap({
    {tipb::ExprType::Count, "count"}, {tipb::ExprType::Sum, "sum"}, {tipb::ExprType::Avg, "avg"}, {tipb::ExprType::Min, "min"},
    {tipb::ExprType::Max, "max"}, {tipb::ExprType::First, "any"},
    //{tipb::ExprType::GroupConcat, ""},
    //{tipb::ExprType::Agg_BitAnd, ""},
    //{tipb::ExprType::Agg_BitOr, ""},
    //{tipb::ExprType::Agg_BitXor, ""},
    //{tipb::ExprType::Std, ""},
    //{tipb::ExprType::Stddev, ""},
    //{tipb::ExprType::StddevPop, ""},
    //{tipb::ExprType::StddevSamp, ""},
    //{tipb::ExprType::VarPop, ""},
    //{tipb::ExprType::VarSamp, ""},
    //{tipb::ExprType::Variance, ""},
    //{tipb::ExprType::JsonArrayAgg, ""},
    //{tipb::ExprType::JsonObjectAgg, ""},
});

std::unordered_map<tipb::ScalarFuncSig, String> scalarFunMap({
    {tipb::ScalarFuncSig::CastIntAsInt, "cast"},
    {tipb::ScalarFuncSig::CastIntAsReal, "cast"},
    {tipb::ScalarFuncSig::CastIntAsString, "cast"},
    {tipb::ScalarFuncSig::CastIntAsDecimal, "cast"},
    {tipb::ScalarFuncSig::CastIntAsTime, "cast"},
    {tipb::ScalarFuncSig::CastIntAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastIntAsJson, "cast"},

    {tipb::ScalarFuncSig::CastRealAsInt, "cast"},
    {tipb::ScalarFuncSig::CastRealAsReal, "cast"},
    {tipb::ScalarFuncSig::CastRealAsString, "cast"},
    {tipb::ScalarFuncSig::CastRealAsDecimal, "cast"},
    {tipb::ScalarFuncSig::CastRealAsTime, "cast"},
    {tipb::ScalarFuncSig::CastRealAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastRealAsJson, "cast"},

    {tipb::ScalarFuncSig::CastDecimalAsInt, "cast"},
    {tipb::ScalarFuncSig::CastDecimalAsReal, "cast"},
    {tipb::ScalarFuncSig::CastDecimalAsString, "cast"},
    {tipb::ScalarFuncSig::CastDecimalAsDecimal, "cast"},
    {tipb::ScalarFuncSig::CastDecimalAsTime, "cast"},
    {tipb::ScalarFuncSig::CastDecimalAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastDecimalAsJson, "cast"},

    {tipb::ScalarFuncSig::CastStringAsInt, "cast"},
    {tipb::ScalarFuncSig::CastStringAsReal, "cast"},
    {tipb::ScalarFuncSig::CastStringAsString, "cast"},
    {tipb::ScalarFuncSig::CastStringAsDecimal, "cast"},
    {tipb::ScalarFuncSig::CastStringAsTime, "cast"},
    {tipb::ScalarFuncSig::CastStringAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastStringAsJson, "cast"},

    {tipb::ScalarFuncSig::CastTimeAsInt, "cast"},
    {tipb::ScalarFuncSig::CastTimeAsReal, "cast"},
    {tipb::ScalarFuncSig::CastTimeAsString, "cast"},
    {tipb::ScalarFuncSig::CastTimeAsDecimal, "cast"},
    {tipb::ScalarFuncSig::CastTimeAsTime, "cast"},
    {tipb::ScalarFuncSig::CastTimeAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastTimeAsJson, "cast"},

    {tipb::ScalarFuncSig::CastDurationAsInt, "cast"},
    {tipb::ScalarFuncSig::CastDurationAsReal, "cast"},
    {tipb::ScalarFuncSig::CastDurationAsString, "cast"},
    {tipb::ScalarFuncSig::CastDurationAsDecimal, "cast"},
    {tipb::ScalarFuncSig::CastDurationAsTime, "cast"},
    {tipb::ScalarFuncSig::CastDurationAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastDurationAsJson, "cast"},

    {tipb::ScalarFuncSig::CastJsonAsInt, "cast"},
    {tipb::ScalarFuncSig::CastJsonAsReal, "cast"},
    {tipb::ScalarFuncSig::CastJsonAsString, "cast"},
    {tipb::ScalarFuncSig::CastJsonAsDecimal, "cast"},
    {tipb::ScalarFuncSig::CastJsonAsTime, "cast"},
    {tipb::ScalarFuncSig::CastJsonAsDuration, "cast"},
    {tipb::ScalarFuncSig::CastJsonAsJson, "cast"},

    {tipb::ScalarFuncSig::CoalesceInt, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceReal, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceString, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceDecimal, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceTime, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceDuration, "coalesce"},
    {tipb::ScalarFuncSig::CoalesceJson, "coalesce"},

    {tipb::ScalarFuncSig::LTInt, "less"},
    {tipb::ScalarFuncSig::LTReal, "less"},
    {tipb::ScalarFuncSig::LTString, "less"},
    {tipb::ScalarFuncSig::LTDecimal, "less"},
    {tipb::ScalarFuncSig::LTTime, "less"},
    {tipb::ScalarFuncSig::LTDuration, "less"},
    {tipb::ScalarFuncSig::LTJson, "less"},

    {tipb::ScalarFuncSig::LEInt, "lessOrEquals"},
    {tipb::ScalarFuncSig::LEReal, "lessOrEquals"},
    {tipb::ScalarFuncSig::LEString, "lessOrEquals"},
    {tipb::ScalarFuncSig::LEDecimal, "lessOrEquals"},
    {tipb::ScalarFuncSig::LETime, "lessOrEquals"},
    {tipb::ScalarFuncSig::LEDuration, "lessOrEquals"},
    {tipb::ScalarFuncSig::LEJson, "lessOrEquals"},

    {tipb::ScalarFuncSig::GTInt, "greater"},
    {tipb::ScalarFuncSig::GTReal, "greater"},
    {tipb::ScalarFuncSig::GTString, "greater"},
    {tipb::ScalarFuncSig::GTDecimal, "greater"},
    {tipb::ScalarFuncSig::GTTime, "greater"},
    {tipb::ScalarFuncSig::GTDuration, "greater"},
    {tipb::ScalarFuncSig::GTJson, "greater"},

    {tipb::ScalarFuncSig::GreatestInt, "greatest"},
    {tipb::ScalarFuncSig::GreatestReal, "greatest"},
    {tipb::ScalarFuncSig::GreatestString, "greatest"},
    {tipb::ScalarFuncSig::GreatestDecimal, "greatest"},
    {tipb::ScalarFuncSig::GreatestTime, "greatest"},

    {tipb::ScalarFuncSig::LeastInt, "least"},
    {tipb::ScalarFuncSig::LeastReal, "least"},
    {tipb::ScalarFuncSig::LeastString, "least"},
    {tipb::ScalarFuncSig::LeastDecimal, "least"},
    {tipb::ScalarFuncSig::LeastTime, "least"},

    //{tipb::ScalarFuncSig::IntervalInt, "cast"},
    //{tipb::ScalarFuncSig::IntervalReal, "cast"},

    {tipb::ScalarFuncSig::GEInt, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GEReal, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GEString, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GEDecimal, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GETime, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GEDuration, "greaterOrEquals"},
    {tipb::ScalarFuncSig::GEJson, "greaterOrEquals"},

    {tipb::ScalarFuncSig::EQInt, "equals"},
    {tipb::ScalarFuncSig::EQReal, "equals"},
    {tipb::ScalarFuncSig::EQString, "equals"},
    {tipb::ScalarFuncSig::EQDecimal, "equals"},
    {tipb::ScalarFuncSig::EQTime, "equals"},
    {tipb::ScalarFuncSig::EQDuration, "equals"},
    {tipb::ScalarFuncSig::EQJson, "equals"},

    {tipb::ScalarFuncSig::NEInt, "notEquals"},
    {tipb::ScalarFuncSig::NEReal, "notEquals"},
    {tipb::ScalarFuncSig::NEString, "notEquals"},
    {tipb::ScalarFuncSig::NEDecimal, "notEquals"},
    {tipb::ScalarFuncSig::NETime, "notEquals"},
    {tipb::ScalarFuncSig::NEDuration, "notEquals"},
    {tipb::ScalarFuncSig::NEJson, "notEquals"},

    //{tipb::ScalarFuncSig::NullEQInt, "cast"},
    //{tipb::ScalarFuncSig::NullEQReal, "cast"},
    //{tipb::ScalarFuncSig::NullEQString, "cast"},
    //{tipb::ScalarFuncSig::NullEQDecimal, "cast"},
    //{tipb::ScalarFuncSig::NullEQTime, "cast"},
    //{tipb::ScalarFuncSig::NullEQDuration, "cast"},
    //{tipb::ScalarFuncSig::NullEQJson, "cast"},

    {tipb::ScalarFuncSig::PlusReal, "plus"},
    {tipb::ScalarFuncSig::PlusDecimal, "plus"},
    {tipb::ScalarFuncSig::PlusInt, "plus"},

    {tipb::ScalarFuncSig::MinusReal, "minus"},
    {tipb::ScalarFuncSig::MinusDecimal, "minus"},
    {tipb::ScalarFuncSig::MinusInt, "minus"},

    {tipb::ScalarFuncSig::MultiplyReal, "multiply"},
    {tipb::ScalarFuncSig::MultiplyDecimal, "multiply"},
    {tipb::ScalarFuncSig::MultiplyInt, "multiply"},

    {tipb::ScalarFuncSig::DivideReal, "divide"},
    {tipb::ScalarFuncSig::DivideDecimal, "divide"},
    {tipb::ScalarFuncSig::IntDivideInt, "intDiv"},
    {tipb::ScalarFuncSig::IntDivideDecimal, "divide"},

    {tipb::ScalarFuncSig::ModReal, "modulo"},
    {tipb::ScalarFuncSig::ModDecimal, "modulo"},
    {tipb::ScalarFuncSig::ModInt, "modulo"},

    {tipb::ScalarFuncSig::MultiplyIntUnsigned, "multiply"},

    {tipb::ScalarFuncSig::AbsInt, "abs"},
    {tipb::ScalarFuncSig::AbsUInt, "abs"},
    {tipb::ScalarFuncSig::AbsReal, "abs"},
    {tipb::ScalarFuncSig::AbsDecimal, "abs"},

    {tipb::ScalarFuncSig::CeilIntToDec, "ceil"},
    {tipb::ScalarFuncSig::CeilIntToInt, "ceil"},
    {tipb::ScalarFuncSig::CeilDecToInt, "ceil"},
    {tipb::ScalarFuncSig::CeilDecToDec, "ceil"},
    {tipb::ScalarFuncSig::CeilReal, "ceil"},

    {tipb::ScalarFuncSig::FloorIntToDec, "floor"},
    {tipb::ScalarFuncSig::FloorIntToInt, "floor"},
    {tipb::ScalarFuncSig::FloorDecToInt, "floor"},
    {tipb::ScalarFuncSig::FloorDecToDec, "floor"},
    {tipb::ScalarFuncSig::FloorReal, "floor"},

    {tipb::ScalarFuncSig::RoundReal, "round"},
    {tipb::ScalarFuncSig::RoundInt, "round"},
    {tipb::ScalarFuncSig::RoundDec, "round"},
    //{tipb::ScalarFuncSig::RoundWithFracReal, "cast"},
    //{tipb::ScalarFuncSig::RoundWithFracInt, "cast"},
    //{tipb::ScalarFuncSig::RoundWithFracDec, "cast"},

    {tipb::ScalarFuncSig::Log1Arg, "log"},
    //{tipb::ScalarFuncSig::Log2Args, "cast"},
    {tipb::ScalarFuncSig::Log2, "log2"},
    {tipb::ScalarFuncSig::Log10, "log10"},

    {tipb::ScalarFuncSig::Rand, "rand"},
    //{tipb::ScalarFuncSig::RandWithSeed, "cast"},

    {tipb::ScalarFuncSig::Pow, "pow"},
    //{tipb::ScalarFuncSig::Conv, "cast"},
    //{tipb::ScalarFuncSig::CRC32, "cast"},
    //{tipb::ScalarFuncSig::Sign, "cast"},

    {tipb::ScalarFuncSig::Sqrt, "sqrt"},
    {tipb::ScalarFuncSig::Acos, "acos"},
    {tipb::ScalarFuncSig::Asin, "asin"},
    {tipb::ScalarFuncSig::Atan1Arg, "atan"},
    //{tipb::ScalarFuncSig::Atan2Args, "cast"},
    {tipb::ScalarFuncSig::Cos, "cos"},
    //{tipb::ScalarFuncSig::Cot, "cast"},
    //{tipb::ScalarFuncSig::Degrees, "cast"},
    {tipb::ScalarFuncSig::Exp, "exp"},
    //{tipb::ScalarFuncSig::PI, "cast"},
    //{tipb::ScalarFuncSig::Radians, "cast"},
    {tipb::ScalarFuncSig::Sin, "sin"},
    {tipb::ScalarFuncSig::Tan, "tan"},
    {tipb::ScalarFuncSig::TruncateInt, "trunc"},
    {tipb::ScalarFuncSig::TruncateReal, "trunc"},
    //{tipb::ScalarFuncSig::TruncateDecimal, "cast"},

    {tipb::ScalarFuncSig::LogicalAnd, "and"},
    {tipb::ScalarFuncSig::LogicalOr, "or"},
    {tipb::ScalarFuncSig::LogicalXor, "xor"},
    {tipb::ScalarFuncSig::UnaryNot, "not"},
    {tipb::ScalarFuncSig::UnaryMinusInt, "negate"},
    {tipb::ScalarFuncSig::UnaryMinusReal, "negate"},
    {tipb::ScalarFuncSig::UnaryMinusDecimal, "negate"},
    {tipb::ScalarFuncSig::DecimalIsNull, "isNull"},
    {tipb::ScalarFuncSig::DurationIsNull, "isNull"},
    {tipb::ScalarFuncSig::RealIsNull, "isNull"},
    {tipb::ScalarFuncSig::StringIsNull, "isNull"},
    {tipb::ScalarFuncSig::TimeIsNull, "isNull"},
    {tipb::ScalarFuncSig::IntIsNull, "isNull"},
    {tipb::ScalarFuncSig::JsonIsNull, "isNull"},

    //{tipb::ScalarFuncSig::BitAndSig, "cast"},
    //{tipb::ScalarFuncSig::BitOrSig, "cast"},
    //{tipb::ScalarFuncSig::BitXorSig, "cast"},
    //{tipb::ScalarFuncSig::BitNegSig, "cast"},
    //{tipb::ScalarFuncSig::IntIsTrue, "cast"},
    //{tipb::ScalarFuncSig::RealIsTrue, "cast"},
    //{tipb::ScalarFuncSig::DecimalIsTrue, "cast"},
    //{tipb::ScalarFuncSig::IntIsFalse, "cast"},
    //{tipb::ScalarFuncSig::RealIsFalse, "cast"},
    //{tipb::ScalarFuncSig::DecimalIsFalse, "cast"},

    //{tipb::ScalarFuncSig::LeftShift, "cast"},
    //{tipb::ScalarFuncSig::RightShift, "cast"},

    //{tipb::ScalarFuncSig::BitCount, "cast"},
    //{tipb::ScalarFuncSig::GetParamString, "cast"},
    //{tipb::ScalarFuncSig::GetVar, "cast"},
    //{tipb::ScalarFuncSig::RowSig, "cast"},
    //{tipb::ScalarFuncSig::SetVar, "cast"},
    //{tipb::ScalarFuncSig::ValuesDecimal, "cast"},
    //{tipb::ScalarFuncSig::ValuesDuration, "cast"},
    //{tipb::ScalarFuncSig::ValuesInt, "cast"},
    //{tipb::ScalarFuncSig::ValuesJSON, "cast"},
    //{tipb::ScalarFuncSig::ValuesReal, "cast"},
    //{tipb::ScalarFuncSig::ValuesString, "cast"},
    //{tipb::ScalarFuncSig::ValuesTime, "cast"},

    {tipb::ScalarFuncSig::InInt, "in"},
    {tipb::ScalarFuncSig::InReal, "in"},
    {tipb::ScalarFuncSig::InString, "in"},
    {tipb::ScalarFuncSig::InDecimal, "in"},
    {tipb::ScalarFuncSig::InTime, "in"},
    {tipb::ScalarFuncSig::InDuration, "in"},
    {tipb::ScalarFuncSig::InJson, "in"},

    {tipb::ScalarFuncSig::IfNullInt, "ifNull"},
    {tipb::ScalarFuncSig::IfNullReal, "ifNull"},
    {tipb::ScalarFuncSig::IfNullString, "ifNull"},
    {tipb::ScalarFuncSig::IfNullDecimal, "ifNull"},
    {tipb::ScalarFuncSig::IfNullTime, "ifNull"},
    {tipb::ScalarFuncSig::IfNullDuration, "ifNull"},
    {tipb::ScalarFuncSig::IfNullJson, "ifNull"},

    {tipb::ScalarFuncSig::IfInt, "if"},
    {tipb::ScalarFuncSig::IfReal, "if"},
    {tipb::ScalarFuncSig::IfString, "if"},
    {tipb::ScalarFuncSig::IfDecimal, "if"},
    {tipb::ScalarFuncSig::IfTime, "if"},
    {tipb::ScalarFuncSig::IfDuration, "if"},
    {tipb::ScalarFuncSig::IfJson, "if"},

    //todo need further check for caseWithExpression and multiIf
    {tipb::ScalarFuncSig::CaseWhenInt, "caseWithExpression"},
    {tipb::ScalarFuncSig::CaseWhenReal, "caseWithExpression"},
    {tipb::ScalarFuncSig::CaseWhenString, "caseWithExpression"},
    {tipb::ScalarFuncSig::CaseWhenDecimal, "caseWithExpression"},
    {tipb::ScalarFuncSig::CaseWhenTime, "caseWithExpression"},
    {tipb::ScalarFuncSig::CaseWhenDuration, "caseWithExpression"},
    {tipb::ScalarFuncSig::CaseWhenJson, "caseWithExpression"},

    //{tipb::ScalarFuncSig::AesDecrypt, "cast"},
    //{tipb::ScalarFuncSig::AesEncrypt, "cast"},
    //{tipb::ScalarFuncSig::Compress, "cast"},
    //{tipb::ScalarFuncSig::MD5, "cast"},
    //{tipb::ScalarFuncSig::Password, "cast"},
    //{tipb::ScalarFuncSig::RandomBytes, "cast"},
    //{tipb::ScalarFuncSig::SHA1, "cast"},
    //{tipb::ScalarFuncSig::SHA2, "cast"},
    //{tipb::ScalarFuncSig::Uncompress, "cast"},
    //{tipb::ScalarFuncSig::UncompressedLength, "cast"},

    //{tipb::ScalarFuncSig::Database, "cast"},
    //{tipb::ScalarFuncSig::FoundRows, "cast"},
    //{tipb::ScalarFuncSig::CurrentUser, "cast"},
    //{tipb::ScalarFuncSig::User, "cast"},
    //{tipb::ScalarFuncSig::ConnectionID, "cast"},
    //{tipb::ScalarFuncSig::LastInsertID, "cast"},
    //{tipb::ScalarFuncSig::LastInsertIDWithID, "cast"},
    //{tipb::ScalarFuncSig::Version, "cast"},
    //{tipb::ScalarFuncSig::TiDBVersion, "cast"},
    //{tipb::ScalarFuncSig::RowCount, "cast"},

    //{tipb::ScalarFuncSig::Sleep, "cast"},
    //{tipb::ScalarFuncSig::Lock, "cast"},
    //{tipb::ScalarFuncSig::ReleaseLock, "cast"},
    //{tipb::ScalarFuncSig::DecimalAnyValue, "cast"},
    //{tipb::ScalarFuncSig::DurationAnyValue, "cast"},
    //{tipb::ScalarFuncSig::IntAnyValue, "cast"},
    //{tipb::ScalarFuncSig::JSONAnyValue, "cast"},
    //{tipb::ScalarFuncSig::RealAnyValue, "cast"},
    //{tipb::ScalarFuncSig::StringAnyValue, "cast"},
    //{tipb::ScalarFuncSig::TimeAnyValue, "cast"},
    //{tipb::ScalarFuncSig::InetAton, "cast"},
    //{tipb::ScalarFuncSig::InetNtoa, "cast"},
    //{tipb::ScalarFuncSig::Inet6Aton, "cast"},
    //{tipb::ScalarFuncSig::Inet6Ntoa, "cast"},
    //{tipb::ScalarFuncSig::IsIPv4, "cast"},
    //{tipb::ScalarFuncSig::IsIPv4Compat, "cast"},
    //{tipb::ScalarFuncSig::IsIPv4Mapped, "cast"},
    //{tipb::ScalarFuncSig::IsIPv6, "cast"},
    //{tipb::ScalarFuncSig::UUID, "cast"},

    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
    {tipb::ScalarFuncSig::Uncompress, "cast"},
});
} // namespace DB
