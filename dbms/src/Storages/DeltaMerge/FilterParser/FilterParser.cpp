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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Poco/Logger.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/Transaction/TiDB.h>
#include <common/logger_useful.h>

#include <cassert>


namespace DB
{
namespace ErrorCodes
{
extern const int COP_BAD_DAG_REQUEST;
} // namespace ErrorCodes

namespace DM
{
namespace cop
{
// This is a pre-check for rough set filter support type.
inline bool isRoughSetFilterSupportType(const Int32 field_type)
{
    switch (field_type)
    {
    case TiDB::TypeTiny:
    case TiDB::TypeShort:
    case TiDB::TypeLong:
    case TiDB::TypeLongLong:
    case TiDB::TypeInt24:
    case TiDB::TypeYear:
        return true;
    // For these date-like types, they store UTC time and ignore time_zone
    case TiDB::TypeNewDate:
    case TiDB::TypeDate:
    case TiDB::TypeTime:
    case TiDB::TypeDatetime:
    case TiDB::TypeTimestamp: // For timestamp, should take time_zone into consideration while parsing `literal`
        return true;
    // For these types, should take collation into consideration. Disable them.
    case TiDB::TypeVarchar:
    case TiDB::TypeJSON:
    case TiDB::TypeTinyBlob:
    case TiDB::TypeMediumBlob:
    case TiDB::TypeLongBlob:
    case TiDB::TypeBlob:
    case TiDB::TypeVarString:
    case TiDB::TypeString:
        return false;
    // Unknown.
    case TiDB::TypeDecimal:
    case TiDB::TypeNewDecimal:
    case TiDB::TypeFloat:
    case TiDB::TypeDouble:
    case TiDB::TypeNull:
    case TiDB::TypeBit:
    case TiDB::TypeEnum:
    case TiDB::TypeSet:
    case TiDB::TypeGeometry:
        return false;
    }
    return false;
}

ColumnID getColumnIDForColumnExpr(const tipb::Expr & expr, const ColumnInfos & scan_column_infos)
{
    assert(isColumnExpr(expr));
    auto column_index = decodeDAGInt64(expr.val());
    if (column_index < 0 || column_index >= static_cast<Int64>(scan_column_infos.size()))
    {
        throw TiFlashException("Column index out of bound: " + DB::toString(column_index) + ", should in [0,"
                                   + DB::toString(scan_column_infos.size()) + ")",
                               Errors::Coprocessor::BadRequest);
    }
    return scan_column_infos[column_index].id;
}

enum class OperandType
{
    Unknown = 0,
    Column,
    Literal,
};

inline RSOperatorPtr parseTiCompareExpr( //
    const tipb::Expr & expr,
    const FilterParser::RSFilterType filter_type,
    const ColumnInfos & scan_column_infos,
    const FilterParser::AttrCreatorByColumnID & creator,
    const TimezoneInfo & timezone_info,
    const LoggerPtr & /*log*/)
{
    if (unlikely(expr.children_size() != 2))
        return createUnsupported(expr.ShortDebugString(),
                                 tipb::ScalarFuncSig_Name(expr.sig()) + " with " + DB::toString(expr.children_size())
                                     + " children is not supported",
                                 false);

    /// Only support `column` `op` `literal` now.

    Attr attr;
    Field value;
    OperandType left = OperandType::Unknown;
    OperandType right = OperandType::Unknown;
    bool is_timestamp_column = false;
    for (const auto & child : expr.children())
    {
        if (isColumnExpr(child))
            is_timestamp_column = (child.field_type().tp() == TiDB::TypeTimestamp);
    }
    for (int32_t child_idx = 0; child_idx < expr.children_size(); child_idx++)
    {
        const auto & child = expr.children(child_idx);
        if (isColumnExpr(child))
        {
            if (unlikely(!child.has_field_type()))
                return createUnsupported(expr.ShortDebugString(), "ColumnRef with no field type is not supported", false);

            auto field_type = child.field_type().tp();
            if (!isRoughSetFilterSupportType(field_type))
                return createUnsupported(
                    expr.ShortDebugString(),
                    "ColumnRef with field type(" + DB::toString(field_type) + ") is not supported",
                    false);

            ColumnID id = getColumnIDForColumnExpr(child, scan_column_infos);
            attr = creator(id);
            if (child_idx == 0)
                left = OperandType::Column;
            else if (child_idx == 1)
                right = OperandType::Column;
        }
        else if (isLiteralExpr(child))
        {
            value = decodeLiteral(child);
            if (child_idx == 0)
                left = OperandType::Literal;
            else if (child_idx == 1)
                right = OperandType::Literal;

            if (is_timestamp_column)
            {
                auto literal_type = child.field_type().tp();
                if (unlikely(literal_type != TiDB::TypeTimestamp && literal_type != TiDB::TypeDatetime))
                    return createUnsupported(expr.ShortDebugString(),
                                             "Compare timestamp column with literal type(" + DB::toString(literal_type)
                                                 + ") is not supported",
                                             false);
                // convert literal value from timezone specified in cop request to UTC
                if (literal_type == TiDB::TypeDatetime && !timezone_info.is_utc_timezone)
                {
                    static const auto & time_zone_utc = DateLUT::instance("UTC");
                    UInt64 from_time = value.get<UInt64>();
                    UInt64 result_time = from_time;
                    if (timezone_info.is_name_based)
                        convertTimeZone(from_time, result_time, *timezone_info.timezone, time_zone_utc);
                    else if (timezone_info.timezone_offset != 0)
                        convertTimeZoneByOffset(from_time, result_time, false, timezone_info.timezone_offset);
                    value = Field(result_time);
                }
            }
        }
    }

    bool normal_cmp = (left == OperandType::Column && right == OperandType::Literal);
    bool inverse_cmp = (left == OperandType::Literal && right == OperandType::Column);
    if (!(normal_cmp || inverse_cmp))
        return createUnsupported(expr.ShortDebugString(),
                                 tipb::ScalarFuncSig_Name(expr.sig()) + " is not supported [left=" + DB::toString(static_cast<int>(left))
                                     + "] [right=" + DB::toString(static_cast<int>(right)) + "]",
                                 false);

    // Correct the filter type by the direction of operands
    auto filter_type_with_direction = filter_type;
    if (inverse_cmp)
    {
        switch (filter_type)
        {
        case FilterParser::RSFilterType::Greater:
            filter_type_with_direction = FilterParser::RSFilterType::Less;
            break;
        case FilterParser::RSFilterType::GreaterEqual:
            filter_type_with_direction = FilterParser::RSFilterType::LessEqual;
            break;
        case FilterParser::RSFilterType::Less:
            filter_type_with_direction = FilterParser::RSFilterType::Greater;
            break;
        case FilterParser::RSFilterType::LessEqual:
            filter_type_with_direction = FilterParser::RSFilterType::GreaterEqual;
            break;
            // Commutative operators, ignored.
            // case FilterParser::RSFilterType::Equal:
            // case FilterParser::RSFilterType::NotEqual:
        default:
            break;
        }
    }

    // TODO: null_direction
    RSOperatorPtr op;
    switch (filter_type_with_direction)
    {
    case FilterParser::RSFilterType::Equal:
        op = createEqual(attr, value);
        break;
    case FilterParser::RSFilterType::NotEqual:
        op = createNotEqual(attr, value);
        break;
    case FilterParser::RSFilterType::Greater:
        op = createGreater(attr, value, -1);
        break;
    case FilterParser::RSFilterType::GreaterEqual:
        op = createGreaterEqual(attr, value, -1);
        break;
    case FilterParser::RSFilterType::Less:
        op = createLess(attr, value, -1);
        break;
    case FilterParser::RSFilterType::LessEqual:
        op = createLessEqual(attr, value, -1);
        break;
    default:
        op = createUnsupported(expr.ShortDebugString(), "Unknown compare type: " + tipb::ExprType_Name(expr.tp()), false);
        break;
    }
    return op;
}

RSOperatorPtr parseTiExpr(const tipb::Expr & expr,
                          const ColumnInfos & scan_column_infos,
                          const FilterParser::AttrCreatorByColumnID & creator,
                          const TimezoneInfo & timezone_info,
                          const LoggerPtr & log)
{
    assert(isFunctionExpr(expr));

    RSOperatorPtr op = EMPTY_RS_OPERATOR;
    if (unlikely(isAggFunctionExpr(expr)))
    {
        op = createUnsupported(expr.ShortDebugString(), "agg function: " + tipb::ExprType_Name(expr.tp()), false);
        return op;
    }

    if (auto iter = FilterParser::scalar_func_rs_filter_map.find(expr.sig()); iter != FilterParser::scalar_func_rs_filter_map.end())
    {
        FilterParser::RSFilterType filter_type = iter->second;
        switch (filter_type)
        {
        // Not/And/Or only support when the child is FunctionExpr, thus expr like `a and null` can not do filter here.
        // If later we want to support Not/And/Or do filter not only FunctionExpr but also ColumnExpr and Literal,
        // we must take a special consideration about null value, due to we just ignore the null value in other filter(such as equal, greater, etc.)
        // Therefore, null value may bring some extra correctness problem if we expand Not/And/Or filtering areas.
        case FilterParser::RSFilterType::Not:
        {
            if (unlikely(expr.children_size() != 1))
                op = createUnsupported(
                    expr.ShortDebugString(),
                    "logical not with " + DB::toString(expr.children_size()) + " children",
                    false);
            else
            {
                const auto & child = expr.children(0);
                if (likely(isFunctionExpr(child)))
                    op = createNot(parseTiExpr(child, scan_column_infos, creator, timezone_info, log));
                else
                    op = createUnsupported(child.ShortDebugString(), "child of logical not is not function", false);
            }
        }
        break;

        case FilterParser::RSFilterType::And:
        case FilterParser::RSFilterType::Or:
        {
            RSOperators children;
            for (Int32 i = 0; i < expr.children_size(); ++i)
            {
                const auto & child = expr.children(i);
                if (likely(isFunctionExpr(child)))
                    children.emplace_back(parseTiExpr(child, scan_column_infos, creator, timezone_info, log));
                else
                    children.emplace_back(createUnsupported(child.ShortDebugString(), "child of logical operator is not function", false));
            }
            if (expr.sig() == tipb::ScalarFuncSig::LogicalAnd)
                op = createAnd(children);
            else
                op = createOr(children);
        }
        break;

        case FilterParser::RSFilterType::Equal:
        case FilterParser::RSFilterType::NotEqual:
        case FilterParser::RSFilterType::Greater:
        case FilterParser::RSFilterType::GreaterEqual:
        case FilterParser::RSFilterType::Less:
        case FilterParser::RSFilterType::LessEqual:
            op = parseTiCompareExpr(expr, filter_type, scan_column_infos, creator, timezone_info, log);
            break;

        case FilterParser::RSFilterType::IsNull:
        {
            // for IsNULL filter, we only support do filter when the child is ColumnExpr.
            // That is, we only do filter when the statement likes `where a is null`, but not `where (a > 1) is null`
            // because in other filter calculation(like Equal/Less/LessEqual/Greater/GreateEqual/NotEqual), we just make filter ignoring the null value.
            // Therefore, if we support IsNull with sub expr, there could be correctness problem.
            // For example, we have a table t(a int, b int), and the data is: (1, 2), (0, null), (null, 1)
            // and then we execute `select * from t where (a > 1) is null`, we want to get (null, 1)
            // but in RSResult (a > 1), we will get the result RSResult::None, and then we think the result is the empty set.
            if (unlikely(expr.children_size() != 1))
            {
                op = createUnsupported(
                    expr.ShortDebugString(),
                    "filter IsNull with " + DB::toString(expr.children_size()) + " children",
                    false);
            }
            else
            {
                const auto & child = expr.children(0);
                if (likely(isColumnExpr(child)))
                {
                    auto field_type = child.field_type().tp();
                    if (!isRoughSetFilterSupportType(field_type))
                        op = createUnsupported(
                            expr.ShortDebugString(),
                            "ColumnRef with field type(" + DB::toString(field_type) + ") is not supported",
                            false);
                    else
                    {
                        ColumnID id = getColumnIDForColumnExpr(child, scan_column_infos);
                        Attr attr = creator(id);
                        op = createIsNull(attr);
                    }
                }
                else
                {
                    op = createUnsupported(child.ShortDebugString(), "child of is null is not column", false);
                }
            }
        }
        break;

        case FilterParser::RSFilterType::In:
        case FilterParser::RSFilterType::NotIn:
        case FilterParser::RSFilterType::Like:
        case FilterParser::RSFilterType::NotLike:
        case FilterParser::RSFilterType::Unsupported:
            op = createUnsupported(expr.ShortDebugString(), tipb::ScalarFuncSig_Name(expr.sig()) + " is not supported", false);
            break;
        }
    }
    else
    {
        op = createUnsupported(expr.ShortDebugString(), tipb::ScalarFuncSig_Name(expr.sig()) + " is not supported", false);
    }

    return op;
}

inline RSOperatorPtr tryParse(const tipb::Expr & filter,
                              const ColumnInfos & scan_column_infos,
                              const FilterParser::AttrCreatorByColumnID & creator,
                              const TimezoneInfo & timezone_info,
                              const LoggerPtr & log)
{
    if (isFunctionExpr(filter))
        return cop::parseTiExpr(filter, scan_column_infos, creator, timezone_info, log);
    else
        return createUnsupported(filter.ShortDebugString(), "child of logical and is not function", false);
}

} // namespace cop


RSOperatorPtr FilterParser::parseDAGQuery(const DAGQueryInfo & dag_info,
                                          const ColumnInfos & scan_column_infos,
                                          FilterParser::AttrCreatorByColumnID && creator,
                                          const LoggerPtr & log)
{
    RSOperatorPtr op = EMPTY_RS_OPERATOR;
    if (dag_info.filters.empty() && dag_info.pushed_down_filters.empty())
        return op;

    if (dag_info.filters.size() == 1 && dag_info.pushed_down_filters.empty())
    {
        op = cop::tryParse(dag_info.filters[0], scan_column_infos, creator, dag_info.timezone_info, log);
    }
    else if (dag_info.pushed_down_filters.size() == 1 && dag_info.filters.empty())
    {
        op = cop::tryParse(dag_info.pushed_down_filters[0], scan_column_infos, creator, dag_info.timezone_info, log);
    }
    else
    {
        /// By default, multiple conditions with operator "and"
        RSOperators children;
        children.reserve(dag_info.filters.size() + dag_info.pushed_down_filters.size());
        for (const auto & filter : dag_info.filters)
        {
            children.emplace_back(cop::tryParse(filter, scan_column_infos, creator, dag_info.timezone_info, log));
        }
        for (const auto & filter : dag_info.pushed_down_filters)
        {
            children.emplace_back(cop::tryParse(filter, scan_column_infos, creator, dag_info.timezone_info, log));
        }
        op = createAnd(children);
    }
    return op;
}

std::unordered_map<tipb::ScalarFuncSig, FilterParser::RSFilterType> FilterParser::scalar_func_rs_filter_map{
    /*
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
    */

    {tipb::ScalarFuncSig::LTInt, FilterParser::RSFilterType::Less},
    {tipb::ScalarFuncSig::LTReal, FilterParser::RSFilterType::Less},
    {tipb::ScalarFuncSig::LTString, FilterParser::RSFilterType::Less},
    {tipb::ScalarFuncSig::LTDecimal, FilterParser::RSFilterType::Less},
    {tipb::ScalarFuncSig::LTTime, FilterParser::RSFilterType::Less},
    {tipb::ScalarFuncSig::LTDuration, FilterParser::RSFilterType::Less},
    {tipb::ScalarFuncSig::LTJson, FilterParser::RSFilterType::Less},

    {tipb::ScalarFuncSig::LEInt, FilterParser::RSFilterType::LessEqual},
    {tipb::ScalarFuncSig::LEReal, FilterParser::RSFilterType::LessEqual},
    {tipb::ScalarFuncSig::LEString, FilterParser::RSFilterType::LessEqual},
    {tipb::ScalarFuncSig::LEDecimal, FilterParser::RSFilterType::LessEqual},
    {tipb::ScalarFuncSig::LETime, FilterParser::RSFilterType::LessEqual},
    {tipb::ScalarFuncSig::LEDuration, FilterParser::RSFilterType::LessEqual},
    {tipb::ScalarFuncSig::LEJson, FilterParser::RSFilterType::LessEqual},

    {tipb::ScalarFuncSig::GTInt, FilterParser::RSFilterType::Greater},
    {tipb::ScalarFuncSig::GTReal, FilterParser::RSFilterType::Greater},
    {tipb::ScalarFuncSig::GTString, FilterParser::RSFilterType::Greater},
    {tipb::ScalarFuncSig::GTDecimal, FilterParser::RSFilterType::Greater},
    {tipb::ScalarFuncSig::GTTime, FilterParser::RSFilterType::Greater},
    {tipb::ScalarFuncSig::GTDuration, FilterParser::RSFilterType::Greater},
    {tipb::ScalarFuncSig::GTJson, FilterParser::RSFilterType::Greater},

    // {tipb::ScalarFuncSig::GreatestInt, "greatest"},
    // {tipb::ScalarFuncSig::GreatestReal, "greatest"},
    // {tipb::ScalarFuncSig::GreatestString, "greatest"},
    // {tipb::ScalarFuncSig::GreatestDecimal, "greatest"},
    // {tipb::ScalarFuncSig::GreatestTime, "greatest"},

    // {tipb::ScalarFuncSig::LeastInt, "least"},
    // {tipb::ScalarFuncSig::LeastReal, "least"},
    // {tipb::ScalarFuncSig::LeastString, "least"},
    // {tipb::ScalarFuncSig::LeastDecimal, "least"},
    // {tipb::ScalarFuncSig::LeastTime, "least"},

    //{tipb::ScalarFuncSig::IntervalInt, "cast"},
    //{tipb::ScalarFuncSig::IntervalReal, "cast"},

    {tipb::ScalarFuncSig::GEInt, FilterParser::RSFilterType::GreaterEqual},
    {tipb::ScalarFuncSig::GEReal, FilterParser::RSFilterType::GreaterEqual},
    {tipb::ScalarFuncSig::GEString, FilterParser::RSFilterType::GreaterEqual},
    {tipb::ScalarFuncSig::GEDecimal, FilterParser::RSFilterType::GreaterEqual},
    {tipb::ScalarFuncSig::GETime, FilterParser::RSFilterType::GreaterEqual},
    {tipb::ScalarFuncSig::GEDuration, FilterParser::RSFilterType::GreaterEqual},
    {tipb::ScalarFuncSig::GEJson, FilterParser::RSFilterType::GreaterEqual},

    {tipb::ScalarFuncSig::EQInt, FilterParser::RSFilterType::Equal},
    {tipb::ScalarFuncSig::EQReal, FilterParser::RSFilterType::Equal},
    {tipb::ScalarFuncSig::EQString, FilterParser::RSFilterType::Equal},
    {tipb::ScalarFuncSig::EQDecimal, FilterParser::RSFilterType::Equal},
    {tipb::ScalarFuncSig::EQTime, FilterParser::RSFilterType::Equal},
    {tipb::ScalarFuncSig::EQDuration, FilterParser::RSFilterType::Equal},
    {tipb::ScalarFuncSig::EQJson, FilterParser::RSFilterType::Equal},

    {tipb::ScalarFuncSig::NEInt, FilterParser::RSFilterType::NotEqual},
    {tipb::ScalarFuncSig::NEReal, FilterParser::RSFilterType::NotEqual},
    {tipb::ScalarFuncSig::NEString, FilterParser::RSFilterType::NotEqual},
    {tipb::ScalarFuncSig::NEDecimal, FilterParser::RSFilterType::NotEqual},
    {tipb::ScalarFuncSig::NETime, FilterParser::RSFilterType::NotEqual},
    {tipb::ScalarFuncSig::NEDuration, FilterParser::RSFilterType::NotEqual},
    {tipb::ScalarFuncSig::NEJson, FilterParser::RSFilterType::NotEqual},

    //{tipb::ScalarFuncSig::NullEQInt, "cast"},
    //{tipb::ScalarFuncSig::NullEQReal, "cast"},
    //{tipb::ScalarFuncSig::NullEQString, "cast"},
    //{tipb::ScalarFuncSig::NullEQDecimal, "cast"},
    //{tipb::ScalarFuncSig::NullEQTime, "cast"},
    //{tipb::ScalarFuncSig::NullEQDuration, "cast"},
    //{tipb::ScalarFuncSig::NullEQJson, "cast"},

    // {tipb::ScalarFuncSig::PlusReal, "plus"},
    // {tipb::ScalarFuncSig::PlusDecimal, "plus"},
    // {tipb::ScalarFuncSig::PlusInt, "plus"},

    // {tipb::ScalarFuncSig::MinusReal, "minus"},
    // {tipb::ScalarFuncSig::MinusDecimal, "minus"},
    // {tipb::ScalarFuncSig::MinusInt, "minus"},

    // {tipb::ScalarFuncSig::MultiplyReal, "multiply"},
    // {tipb::ScalarFuncSig::MultiplyDecimal, "multiply"},
    // {tipb::ScalarFuncSig::MultiplyInt, "multiply"},

    // {tipb::ScalarFuncSig::DivideReal, "divide"},
    // {tipb::ScalarFuncSig::DivideDecimal, "divide"},
    // {tipb::ScalarFuncSig::IntDivideInt, "intDiv"},
    // {tipb::ScalarFuncSig::IntDivideDecimal, "divide"},

    // {tipb::ScalarFuncSig::ModReal, "modulo"},
    // {tipb::ScalarFuncSig::ModDecimal, "modulo"},
    // {tipb::ScalarFuncSig::ModInt, "modulo"},

    // {tipb::ScalarFuncSig::MultiplyIntUnsigned, "multiply"},

    // {tipb::ScalarFuncSig::AbsInt, "abs"},
    // {tipb::ScalarFuncSig::AbsUInt, "abs"},
    // {tipb::ScalarFuncSig::AbsReal, "abs"},
    // {tipb::ScalarFuncSig::AbsDecimal, "abs"},

    // {tipb::ScalarFuncSig::CeilIntToDec, "ceil"},
    // {tipb::ScalarFuncSig::CeilIntToInt, "ceil"},
    // {tipb::ScalarFuncSig::CeilDecToInt, "ceil"},
    // {tipb::ScalarFuncSig::CeilDecToDec, "ceil"},
    // {tipb::ScalarFuncSig::CeilReal, "ceil"},

    // {tipb::ScalarFuncSig::FloorIntToDec, "floor"},
    // {tipb::ScalarFuncSig::FloorIntToInt, "floor"},
    // {tipb::ScalarFuncSig::FloorDecToInt, "floor"},
    // {tipb::ScalarFuncSig::FloorDecToDec, "floor"},
    // {tipb::ScalarFuncSig::FloorReal, "floor"},

    //{tipb::ScalarFuncSig::RoundReal, "round"},
    //{tipb::ScalarFuncSig::RoundInt, "round"},
    //{tipb::ScalarFuncSig::RoundDec, "round"},
    //{tipb::ScalarFuncSig::RoundWithFracReal, "cast"},
    //{tipb::ScalarFuncSig::RoundWithFracInt, "cast"},
    //{tipb::ScalarFuncSig::RoundWithFracDec, "cast"},

    //{tipb::ScalarFuncSig::Log1Arg, "log"},
    //{tipb::ScalarFuncSig::Log2Args, "cast"},
    //{tipb::ScalarFuncSig::Log2, "log2"},
    //{tipb::ScalarFuncSig::Log10, "log10"},

    //{tipb::ScalarFuncSig::Rand, "rand"},
    //{tipb::ScalarFuncSig::RandWithSeed, "cast"},

    //{tipb::ScalarFuncSig::Pow, "pow"},
    //{tipb::ScalarFuncSig::Conv, "cast"},
    //{tipb::ScalarFuncSig::CRC32, "cast"},
    //{tipb::ScalarFuncSig::Sign, "cast"},

    //{tipb::ScalarFuncSig::Sqrt, "sqrt"},
    //{tipb::ScalarFuncSig::Acos, "acos"},
    //{tipb::ScalarFuncSig::Asin, "asin"},
    //{tipb::ScalarFuncSig::Atan1Arg, "atan"},
    //{tipb::ScalarFuncSig::Atan2Args, "cast"},
    //{tipb::ScalarFuncSig::Cos, "cos"},
    //{tipb::ScalarFuncSig::Cot, "cast"},
    //{tipb::ScalarFuncSig::Degrees, "cast"},
    //{tipb::ScalarFuncSig::Exp, "exp"},
    //{tipb::ScalarFuncSig::PI, "cast"},
    //{tipb::ScalarFuncSig::Radians, "cast"},
    // {tipb::ScalarFuncSig::Sin, "sin"},
    // {tipb::ScalarFuncSig::Tan, "tan"},
    // {tipb::ScalarFuncSig::TruncateInt, "trunc"},
    // {tipb::ScalarFuncSig::TruncateReal, "trunc"},
    //{tipb::ScalarFuncSig::TruncateDecimal, "cast"},

    {tipb::ScalarFuncSig::LogicalAnd, FilterParser::RSFilterType::And},
    {tipb::ScalarFuncSig::LogicalOr, FilterParser::RSFilterType::Or},
    // {tipb::ScalarFuncSig::LogicalXor, "xor"},
    {tipb::ScalarFuncSig::UnaryNotDecimal, FilterParser::RSFilterType::Not},
    {tipb::ScalarFuncSig::UnaryNotInt, FilterParser::RSFilterType::Not},
    {tipb::ScalarFuncSig::UnaryNotReal, FilterParser::RSFilterType::Not},

    // {tipb::ScalarFuncSig::UnaryMinusInt, "negate"},
    // {tipb::ScalarFuncSig::UnaryMinusReal, "negate"},
    // {tipb::ScalarFuncSig::UnaryMinusDecimal, "negate"},

    {tipb::ScalarFuncSig::DecimalIsNull, FilterParser::RSFilterType::IsNull},
    {tipb::ScalarFuncSig::DurationIsNull, FilterParser::RSFilterType::IsNull},
    {tipb::ScalarFuncSig::RealIsNull, FilterParser::RSFilterType::IsNull},
    {tipb::ScalarFuncSig::StringIsNull, FilterParser::RSFilterType::IsNull},
    {tipb::ScalarFuncSig::TimeIsNull, FilterParser::RSFilterType::IsNull},
    {tipb::ScalarFuncSig::IntIsNull, FilterParser::RSFilterType::IsNull},
    {tipb::ScalarFuncSig::JsonIsNull, FilterParser::RSFilterType::IsNull},

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

    // {tipb::ScalarFuncSig::InInt, "in"},
    // {tipb::ScalarFuncSig::InReal, "in"},
    // {tipb::ScalarFuncSig::InString, "in"},
    // {tipb::ScalarFuncSig::InDecimal, "in"},
    // {tipb::ScalarFuncSig::InTime, "in"},
    // {tipb::ScalarFuncSig::InDuration, "in"},
    // {tipb::ScalarFuncSig::InJson, "in"},

    // {tipb::ScalarFuncSig::IfNullInt, "ifNull"},
    // {tipb::ScalarFuncSig::IfNullReal, "ifNull"},
    // {tipb::ScalarFuncSig::IfNullString, "ifNull"},
    // {tipb::ScalarFuncSig::IfNullDecimal, "ifNull"},
    // {tipb::ScalarFuncSig::IfNullTime, "ifNull"},
    // {tipb::ScalarFuncSig::IfNullDuration, "ifNull"},
    // {tipb::ScalarFuncSig::IfNullJson, "ifNull"},

    // {tipb::ScalarFuncSig::IfInt, "if"},
    // {tipb::ScalarFuncSig::IfReal, "if"},
    // {tipb::ScalarFuncSig::IfString, "if"},
    // {tipb::ScalarFuncSig::IfDecimal, "if"},
    // {tipb::ScalarFuncSig::IfTime, "if"},
    // {tipb::ScalarFuncSig::IfDuration, "if"},
    // {tipb::ScalarFuncSig::IfJson, "if"},

    //todo need further check for caseWithExpression and multiIf
    //{tipb::ScalarFuncSig::CaseWhenInt, "caseWithExpression"},
    //{tipb::ScalarFuncSig::CaseWhenReal, "caseWithExpression"},
    //{tipb::ScalarFuncSig::CaseWhenString, "caseWithExpression"},
    //{tipb::ScalarFuncSig::CaseWhenDecimal, "caseWithExpression"},
    //{tipb::ScalarFuncSig::CaseWhenTime, "caseWithExpression"},
    //{tipb::ScalarFuncSig::CaseWhenDuration, "caseWithExpression"},
    //{tipb::ScalarFuncSig::CaseWhenJson, "caseWithExpression"},

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

    // {tipb::ScalarFuncSig::LikeSig, "like3Args"},
    //{tipb::ScalarFuncSig::RegexpBinarySig, "cast"},
    //{tipb::ScalarFuncSig::RegexpSig, "cast"},

    //{tipb::ScalarFuncSig::JsonExtractSig, "cast"},
    //{tipb::ScalarFuncSig::JsonUnquoteSig, "cast"},
    //{tipb::ScalarFuncSig::JsonTypeSig, "cast"},
    //{tipb::ScalarFuncSig::JsonSetSig, "cast"},
    //{tipb::ScalarFuncSig::JsonInsertSig, "cast"},
    //{tipb::ScalarFuncSig::JsonReplaceSig, "cast"},
    //{tipb::ScalarFuncSig::JsonRemoveSig, "cast"},
    //{tipb::ScalarFuncSig::JsonMergeSig, "cast"},
    //{tipb::ScalarFuncSig::JsonObjectSig, "cast"},
    //{tipb::ScalarFuncSig::JsonArraySig, "cast"},
    //{tipb::ScalarFuncSig::JsonValidJsonSig, "cast"},
    //{tipb::ScalarFuncSig::JsonContainsSig, "cast"},
    //{tipb::ScalarFuncSig::JsonArrayAppendSig, "cast"},
    //{tipb::ScalarFuncSig::JsonArrayInsertSig, "cast"},
    //{tipb::ScalarFuncSig::JsonMergePatchSig, "cast"},
    //{tipb::ScalarFuncSig::JsonMergePreserveSig, "cast"},
    //{tipb::ScalarFuncSig::JsonContainsPathSig, "cast"},
    //{tipb::ScalarFuncSig::JsonPrettySig, "cast"},
    //{tipb::ScalarFuncSig::JsonQuoteSig, "cast"},
    //{tipb::ScalarFuncSig::JsonSearchSig, "cast"},
    //{tipb::ScalarFuncSig::JsonStorageSizeSig, "cast"},
    //{tipb::ScalarFuncSig::JsonDepthSig, "cast"},
    //{tipb::ScalarFuncSig::JsonKeysSig, "cast"},
    //{tipb::ScalarFuncSig::JsonLengthSig, "cast"},
    //{tipb::ScalarFuncSig::JsonKeys2ArgsSig, "cast"},
    //{tipb::ScalarFuncSig::JsonValidStringSig, "cast"},

    //{tipb::ScalarFuncSig::DateFormatSig, "cast"},
    //{tipb::ScalarFuncSig::DateLiteral, "cast"},
    //{tipb::ScalarFuncSig::DateDiff, "cast"},
    //{tipb::ScalarFuncSig::NullTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::TimeStringTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::DurationDurationTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::DurationDurationTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::StringTimeTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::StringDurationTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::StringStringTimeDiff, "cast"},
    //{tipb::ScalarFuncSig::TimeTimeTimeDiff, "cast"},

    //{tipb::ScalarFuncSig::Date, "cast"},
    //{tipb::ScalarFuncSig::Hour, "cast"},
    //{tipb::ScalarFuncSig::Minute, "cast"},
    //{tipb::ScalarFuncSig::Second, "cast"},
    //{tipb::ScalarFuncSig::MicroSecond, "cast"},
    //{tipb::ScalarFuncSig::Month, "cast"},
    //{tipb::ScalarFuncSig::MonthName, "cast"},

    //{tipb::ScalarFuncSig::NowWithArg, "cast"},
    //{tipb::ScalarFuncSig::NowWithoutArg, "cast"},

    //{tipb::ScalarFuncSig::DayName, "cast"},
    //{tipb::ScalarFuncSig::DayOfMonth, "cast"},
    //{tipb::ScalarFuncSig::DayOfWeek, "cast"},
    //{tipb::ScalarFuncSig::DayOfYear, "cast"},

    //{tipb::ScalarFuncSig::WeekWithMode, "cast"},
    //{tipb::ScalarFuncSig::WeekWithoutMode, "cast"},
    //{tipb::ScalarFuncSig::WeekDay, "cast"},
    //{tipb::ScalarFuncSig::WeekOfYear, "cast"},

    //{tipb::ScalarFuncSig::Year, "cast"},
    //{tipb::ScalarFuncSig::YearWeekWithMode, "cast"},
    //{tipb::ScalarFuncSig::YearWeekWithoutMode, "cast"},

    //{tipb::ScalarFuncSig::GetFormat, "cast"},
    //{tipb::ScalarFuncSig::SysDateWithFsp, "cast"},
    //{tipb::ScalarFuncSig::SysDateWithoutFsp, "cast"},
    //{tipb::ScalarFuncSig::CurrentDate, "cast"},
    //{tipb::ScalarFuncSig::CurrentTime0Arg, "cast"},
    //{tipb::ScalarFuncSig::CurrentTime1Arg, "cast"},

    //{tipb::ScalarFuncSig::Time, "cast"},
    //{tipb::ScalarFuncSig::TimeLiteral, "cast"},
    //{tipb::ScalarFuncSig::UTCDate, "cast"},
    //{tipb::ScalarFuncSig::UTCTimestampWithArg, "cast"},
    //{tipb::ScalarFuncSig::UTCTimestampWithoutArg, "cast"},

    //{tipb::ScalarFuncSig::AddDatetimeAndDuration, "cast"},
    //{tipb::ScalarFuncSig::AddDatetimeAndString, "cast"},
    //{tipb::ScalarFuncSig::AddTimeDateTimeNull, "cast"},
    //{tipb::ScalarFuncSig::AddStringAndDuration, "cast"},
    //{tipb::ScalarFuncSig::AddStringAndString, "cast"},
    //{tipb::ScalarFuncSig::AddTimeStringNull, "cast"},
    //{tipb::ScalarFuncSig::AddDurationAndDuration, "cast"},
    //{tipb::ScalarFuncSig::AddDurationAndString, "cast"},
    //{tipb::ScalarFuncSig::AddTimeDurationNull, "cast"},
    //{tipb::ScalarFuncSig::AddDateAndDuration, "cast"},
    //{tipb::ScalarFuncSig::AddDateAndString, "cast"},

    //{tipb::ScalarFuncSig::SubDateAndDuration, "cast"},
    //{tipb::ScalarFuncSig::SubDateAndString, "cast"},
    //{tipb::ScalarFuncSig::SubTimeDateTimeNull, "cast"},
    //{tipb::ScalarFuncSig::SubStringAndDuration, "cast"},
    //{tipb::ScalarFuncSig::SubStringAndString, "cast"},
    //{tipb::ScalarFuncSig::SubTimeStringNull, "cast"},
    //{tipb::ScalarFuncSig::SubDurationAndDuration, "cast"},
    //{tipb::ScalarFuncSig::SubDurationAndString, "cast"},
    //{tipb::ScalarFuncSig::SubDateAndDuration, "cast"},
    //{tipb::ScalarFuncSig::SubDateAndString, "cast"},

    //{tipb::ScalarFuncSig::UnixTimestampCurrent, "cast"},
    //{tipb::ScalarFuncSig::UnixTimestampInt, "cast"},
    //{tipb::ScalarFuncSig::UnixTimestampDec, "cast"},

    //{tipb::ScalarFuncSig::ConvertTz, "cast"},
    //{tipb::ScalarFuncSig::MakeDate, "cast"},
    //{tipb::ScalarFuncSig::MakeTime, "cast"},
    //{tipb::ScalarFuncSig::PeriodAdd, "cast"},
    //{tipb::ScalarFuncSig::PeriodDiff, "cast"},
    //{tipb::ScalarFuncSig::Quarter, "cast"},

    //{tipb::ScalarFuncSig::SecToTime, "cast"},
    //{tipb::ScalarFuncSig::TimeToSec, "cast"},
    //{tipb::ScalarFuncSig::TimestampAdd, "cast"},
    //{tipb::ScalarFuncSig::ToDays, "cast"},
    //{tipb::ScalarFuncSig::ToSeconds, "cast"},
    //{tipb::ScalarFuncSig::UTCTimeWithArg, "cast"},
    //{tipb::ScalarFuncSig::UTCTimestampWithoutArg, "cast"},
    //{tipb::ScalarFuncSig::Timestamp1Arg, "cast"},
    //{tipb::ScalarFuncSig::Timestamp2Args, "cast"},
    //{tipb::ScalarFuncSig::TimestampLiteral, "cast"},

    //{tipb::ScalarFuncSig::LastDay, "cast"},
    //{tipb::ScalarFuncSig::StrToDateDate, "cast"},
    //{tipb::ScalarFuncSig::StrToDateDatetime, "cast"},
    //{tipb::ScalarFuncSig::StrToDateDuration, "cast"},
    //{tipb::ScalarFuncSig::FromUnixTime1Arg, "cast"},
    //{tipb::ScalarFuncSig::FromUnixTime2Arg, "cast"},
    //{tipb::ScalarFuncSig::ExtractDatetime, "cast"},
    //{tipb::ScalarFuncSig::ExtractDuration, "cast"},

    //{tipb::ScalarFuncSig::AddDateStringString, "cast"},
    //{tipb::ScalarFuncSig::AddDateStringInt, "cast"},
    //{tipb::ScalarFuncSig::AddDateStringDecimal, "cast"},
    //{tipb::ScalarFuncSig::AddDateIntString, "cast"},
    //{tipb::ScalarFuncSig::AddDateIntInt, "cast"},
    //{tipb::ScalarFuncSig::AddDateDatetimeString, "cast"},
    //{tipb::ScalarFuncSig::AddDateDatetimeInt, "cast"},

    //{tipb::ScalarFuncSig::SubDateStringString, "cast"},
    //{tipb::ScalarFuncSig::SubDateStringInt, "cast"},
    //{tipb::ScalarFuncSig::SubDateStringDecimal, "cast"},
    //{tipb::ScalarFuncSig::SubDateIntString, "cast"},
    //{tipb::ScalarFuncSig::SubDateIntInt, "cast"},
    //{tipb::ScalarFuncSig::SubDateDatetimeString, "cast"},
    //{tipb::ScalarFuncSig::SubDateDatetimeInt, "cast"},

    //{tipb::ScalarFuncSig::FromDays, "cast"},
    //{tipb::ScalarFuncSig::TimeFormat, "cast"},
    //{tipb::ScalarFuncSig::TimestampDiff, "cast"},

    //{tipb::ScalarFuncSig::BitLength, "cast"},
    //{tipb::ScalarFuncSig::Bin, "cast"},
    //{tipb::ScalarFuncSig::ASCII, "cast"},
    //{tipb::ScalarFuncSig::Char, "cast"},
    // {tipb::ScalarFuncSig::CharLength, "lengthUTF8"},
    //{tipb::ScalarFuncSig::Concat, "cast"},
    //{tipb::ScalarFuncSig::ConcatWS, "cast"},
    //{tipb::ScalarFuncSig::Convert, "cast"},
    //{tipb::ScalarFuncSig::Elt, "cast"},
    //{tipb::ScalarFuncSig::ExportSet3Arg, "cast"},
    //{tipb::ScalarFuncSig::ExportSet4Arg, "cast"},
    //{tipb::ScalarFuncSig::ExportSet5Arg, "cast"},
    //{tipb::ScalarFuncSig::FieldInt, "cast"},
    //{tipb::ScalarFuncSig::FieldReal, "cast"},
    //{tipb::ScalarFuncSig::FieldString, "cast"},

    //{tipb::ScalarFuncSig::FindInSet, "cast"},
    //{tipb::ScalarFuncSig::Format, "cast"},
    //{tipb::ScalarFuncSig::FormatWithLocale, "cast"},
    //{tipb::ScalarFuncSig::FromBase64, "cast"},
    //{tipb::ScalarFuncSig::HexIntArg, "cast"},
    //{tipb::ScalarFuncSig::HexStrArg, "cast"},
    //{tipb::ScalarFuncSig::Insert, "cast"},
    //{tipb::ScalarFuncSig::InsertBinary, "cast"},
    //{tipb::ScalarFuncSig::Instr, "cast"},
    //{tipb::ScalarFuncSig::InstrBinary, "cast"},

    // {tipb::ScalarFuncSig::LTrim, "ltrim"},
    //{tipb::ScalarFuncSig::Left, "cast"},
    //{tipb::ScalarFuncSig::LeftBinary, "cast"},
    // {tipb::ScalarFuncSig::Length, "length"},
    //{tipb::ScalarFuncSig::Locate2Args, "cast"},
    //{tipb::ScalarFuncSig::Locate3Args, "cast"},
    //{tipb::ScalarFuncSig::LocateBinary2Args, "cast"},
    //{tipb::ScalarFuncSig::LocateBinary3Args, "cast"},

    // {tipb::ScalarFuncSig::Lower, "lower"},
    //{tipb::ScalarFuncSig::Lpad, "cast"},
    //{tipb::ScalarFuncSig::LpadBinary, "cast"},
    //{tipb::ScalarFuncSig::MakeSet, "cast"},
    //{tipb::ScalarFuncSig::OctInt, "cast"},
    //{tipb::ScalarFuncSig::OctString, "cast"},
    //{tipb::ScalarFuncSig::Ord, "cast"},
    //{tipb::ScalarFuncSig::Quote, "cast"},
    // {tipb::ScalarFuncSig::RTrim, "rtrim"},
    //{tipb::ScalarFuncSig::Repeat, "cast"},
    //{tipb::ScalarFuncSig::Replace, "cast"},
    //{tipb::ScalarFuncSig::Reverse, "cast"},
    //{tipb::ScalarFuncSig::ReverseBinary, "cast"},
    //{tipb::ScalarFuncSig::Right, "cast"},
    //{tipb::ScalarFuncSig::RightBinary, "cast"},
    //{tipb::ScalarFuncSig::Rpad, "cast"},
    //{tipb::ScalarFuncSig::RpadBinary, "cast"},
    //{tipb::ScalarFuncSig::Space, "cast"},
    //{tipb::ScalarFuncSig::Strcmp, "cast"},
    //{tipb::ScalarFuncSig::Substring2Args, "cast"},
    //{tipb::ScalarFuncSig::Substring3Args, "cast"},
    //{tipb::ScalarFuncSig::SubstringBinary2Args, "cast"},
    //{tipb::ScalarFuncSig::SubstringBinary3Args, "cast"},
    //{tipb::ScalarFuncSig::SubstringIndex, "cast"},

    //{tipb::ScalarFuncSig::ToBase64, "cast"},
    //{tipb::ScalarFuncSig::Trim1Arg, "cast"},
    //{tipb::ScalarFuncSig::Trim2Args, "cast"},
    //{tipb::ScalarFuncSig::Trim3Args, "cast"},
    //{tipb::ScalarFuncSig::UnHex, "cast"},
    // {tipb::ScalarFuncSig::Upper, "upper"},
};

} // namespace DM

} // namespace DB
