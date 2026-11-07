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

#pragma once

#include <Common/MyTime.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/ShardInfo.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <Interpreters/TimezoneInfo.h>
#include <TiDB/Schema/TiDBTypes.h>
#include <fmt/format.h>
#include <google/protobuf/repeated_field.h>
#include <tici-search-lib/src/lib.rs.h>
#include <tipb/executor.pb.h>

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <stdexcept>
#include <tuple>
#include <utility>
#include <vector>

namespace DB::TS
{

// Convert literal value from timezone specified in cop request to UTC in-place.
inline UInt64 convertPackedU64WithTimezone(UInt64 from_time, const TimezoneInfo & timezone_info)
{
    static const auto & time_zone_utc = DateLUT::instance("UTC");
    UInt64 result_time = from_time;
    if (timezone_info.is_name_based)
        convertTimeZone(from_time, result_time, *timezone_info.timezone, time_zone_utc);
    else if (timezone_info.timezone_offset != 0)
        convertTimeZoneByOffset(from_time, result_time, false, timezone_info.timezone_offset);
    return result_time;
}

inline rust::Vec<::Range> getKeyRanges(const ShardInfo::KeyRanges & key_ranges)
{
    rust::Vec<::Range> res;
    for (const auto & range : key_ranges)
    {
        rust::Slice<::std::uint8_t const> start(
            reinterpret_cast<const unsigned char *>(range.start().c_str()),
            range.start().size());
        rust::Slice<::std::uint8_t const> end(
            reinterpret_cast<const unsigned char *>(range.end().c_str()),
            range.end().size());
        res.push_back({
            .start = std::move(start),
            .end = std::move(end),
        });
    }
    return res;
}

inline std::tuple<::Expr, std::vector<ColumnID>> tipbToTiCIExpr(
    const tipb::Expr & expr,
    const TimezoneInfo & timezone_info)
{
    ::Expr ret;
    switch (expr.tp())
    {
    case tipb::ExprType::ScalarFunc:
    {
        std::vector<ColumnID> children_cids;
        ret.tp = tipb::ExprType::ScalarFunc;
        for (const auto & child : expr.children())
        {
            auto [child_expr, child_cids] = tipbToTiCIExpr(child, timezone_info);
            ret.children.push_back(child_expr);
            children_cids.insert(children_cids.end(), child_cids.begin(), child_cids.end());
        }
        switch (expr.sig())
        {
        case tipb::ScalarFuncSig::FTSMatchWord:
        case tipb::ScalarFuncSig::FTSMatchPrefix:
        case tipb::ScalarFuncSig::FTSMatchPhrase:
        case tipb::ScalarFuncSig::LogicalAnd:
        case tipb::ScalarFuncSig::LogicalOr:
        case tipb::ScalarFuncSig::UnaryNotInt:
        case tipb::ScalarFuncSig::UnaryNotReal:
        case tipb::ScalarFuncSig::EQInt:
        case tipb::ScalarFuncSig::NEInt:
        case tipb::ScalarFuncSig::LTInt:
        case tipb::ScalarFuncSig::LEInt:
        case tipb::ScalarFuncSig::GTInt:
        case tipb::ScalarFuncSig::GEInt:
        case tipb::ScalarFuncSig::EQString:
        case tipb::ScalarFuncSig::NEString:
        case tipb::ScalarFuncSig::LTString:
        case tipb::ScalarFuncSig::LEString:
        case tipb::ScalarFuncSig::GTString:
        case tipb::ScalarFuncSig::GEString:
        case tipb::ScalarFuncSig::EQReal:
        case tipb::ScalarFuncSig::NEReal:
        case tipb::ScalarFuncSig::LTReal:
        case tipb::ScalarFuncSig::LEReal:
        case tipb::ScalarFuncSig::GTReal:
        case tipb::ScalarFuncSig::GEReal:
        case tipb::ScalarFuncSig::EQDecimal:
        case tipb::ScalarFuncSig::NEDecimal:
        case tipb::ScalarFuncSig::LTDecimal:
        case tipb::ScalarFuncSig::LEDecimal:
        case tipb::ScalarFuncSig::GTDecimal:
        case tipb::ScalarFuncSig::GEDecimal:
        case tipb::ScalarFuncSig::InInt:
        case tipb::ScalarFuncSig::InString:
        case tipb::ScalarFuncSig::InReal:
        case tipb::ScalarFuncSig::InDecimal:
            ret.sig = expr.sig();
            break;
        case tipb::ScalarFuncSig::EQTime:
        case tipb::ScalarFuncSig::NETime:
        case tipb::ScalarFuncSig::LTTime:
        case tipb::ScalarFuncSig::LETime:
        case tipb::ScalarFuncSig::GTTime:
        case tipb::ScalarFuncSig::GETime:
        {
            ret.sig = expr.sig();
            size_t col_idx = 0, val_idx = 1;
            if (isColumnExpr(expr.children(1)))
                std::swap(col_idx, val_idx);
            if (expr.children(col_idx).field_type().tp() == TiDB::TypeTimestamp)
            {
                const auto & child_expr = expr.children(val_idx);
                if (isLiteralExpr(child_expr))
                {
                    UInt64 val = decodeDAGUInt64(child_expr.val());
                    val = convertPackedU64WithTimezone(val, timezone_info);
                    WriteBufferFromOwnString ss;
                    encodeDAGUInt64(val, ss);
                    ret.children[val_idx].val.clear();
                    auto str = ss.releaseStr();
                    std::copy(str.begin(), str.end(), std::back_inserter(ret.children[val_idx].val));
                }
            }
            break;
        }
        case tipb::ScalarFuncSig::InTime:
        {
            ret.sig = expr.sig();
            if (expr.children(0).field_type().tp() == TiDB::TypeTimestamp)
            {
                for (int val_idx = 1; val_idx < expr.children_size(); ++val_idx)
                {
                    const auto & child_expr = expr.children(val_idx);
                    if (isLiteralExpr(child_expr))
                    {
                        UInt64 val = decodeDAGUInt64(child_expr.val());
                        val = convertPackedU64WithTimezone(val, timezone_info);
                        WriteBufferFromOwnString ss;
                        encodeDAGUInt64(val, ss);
                        ret.children[val_idx].val.clear();
                        auto str = ss.releaseStr();
                        std::copy(str.begin(), str.end(), std::back_inserter(ret.children[val_idx].val));
                    }
                    else
                    {
                        throw TiFlashException("InTime only support literal values", Errors::Coprocessor::BadRequest);
                    }
                }
            }
            break;
        }
        default:
            throw std::runtime_error(fmt::format(
                "Unsupported expression sig: tp={}, sig={}, expr={}",
                static_cast<int>(expr.tp()),
                static_cast<int>(expr.sig()),
                expr.DebugString()));
        }

        return {ret, children_cids};
    }
    case tipb::ExprType::ColumnRef:
    {
        ret.tp = expr.tp();
        auto id = decodeDAGInt64(expr.val());
        auto str = fmt::format("column_{}", id);
        std::copy(str.begin(), str.end(), std::back_inserter(ret.val));
        return {ret, {id}};
    }
    case tipb::ExprType::String:
    case tipb::ExprType::Int64:
    case tipb::ExprType::Uint64:
    case tipb::ExprType::Float32:
    case tipb::ExprType::Float64:
    case tipb::ExprType::MysqlTime:
    {
        ret.tp = expr.tp();
        std::copy(expr.val().begin(), expr.val().end(), std::back_inserter(ret.val));
        return {ret, {}};
    }
    case tipb::ExprType::MysqlDecimal:
    {
        ret.tp = expr.tp();
        auto field = decodeDAGDecimal(expr.val());
        String str;
        if (field.getType() == Field::Types::Decimal32)
            str = field.get<DecimalField<Decimal32>>().toString();
        else if (field.getType() == Field::Types::Decimal64)
            str = field.get<DecimalField<Decimal64>>().toString();
        else if (field.getType() == Field::Types::Decimal128)
            str = field.get<DecimalField<Decimal128>>().toString();
        else if (field.getType() == Field::Types::Decimal256)
            str = field.get<DecimalField<Decimal256>>().toString();
        else
            throw TiFlashException("Not decimal literal: " + expr.DebugString(), Errors::Coprocessor::BadRequest);
        std::copy(str.begin(), str.end(), std::back_inserter(ret.val));
        return {ret, {}};
    }
    default:
        throw std::runtime_error(fmt::format(
            "Unsupported expression type: tp={}, expr={}",
            static_cast<int>(expr.tp()),
            expr.DebugString()));
    }
}

inline std::tuple<::Expr, std::vector<ColumnID>> tipbToTiCIExpr(
    const google::protobuf::RepeatedPtrField<tipb::Expr> & exprs,
    const TimezoneInfo & tz)
{
    if (exprs.empty())
        throw std::runtime_error("Empty match expression");
    auto [ret, cids] = tipbToTiCIExpr(exprs[0], tz);
    for (auto i = 1; i < exprs.size(); ++i)
    {
        auto [child_expr, child_cids] = tipbToTiCIExpr(exprs[i], tz);
        ret = {
            .tp = tipb::ExprType::ScalarFunc,
            .children = {ret, child_expr},
            .sig = tipb::ScalarFuncSig::LogicalAnd,
        };
        cids.insert(cids.end(), child_cids.begin(), child_cids.end());
    }
    return {ret, cids};
}

} // namespace DB::TS
