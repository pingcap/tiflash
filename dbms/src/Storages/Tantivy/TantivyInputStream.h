// Copyright 2025 PingCAP, Ltd.
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

#include <Common/Logger.h>
#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/ShardInfo.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <fcntl.h>
#include <fmt/os.h>
#include <tici-search-lib/src/lib.rs.h>

namespace DB::TS
{
class TantivyInputStream : public IProfilingBlockInputStream
{
    static constexpr auto NAME = "TantivyInputStream";

public:
    TantivyInputStream(
        LoggerPtr log_,
        UInt32 keyspace_id_,
        Int64 table_id_,
        Int64 index_id_,
        ShardInfoList query_shard_infos_,
        NamesAndTypes return_columns_,
        UInt64 limit_,
        UInt64 read_ts_,
        google::protobuf::RepeatedPtrField<tipb::Expr> match_expr_,
        bool is_count)
        : log(log_)
        , keyspace_id(keyspace_id_)
        , table_id(table_id_)
        , index_id(index_id_)
        , query_shard_infos(query_shard_infos_)
        , return_columns(return_columns_)
        , limit(limit_)
        , read_ts(read_ts_)
        , is_count(is_count)
    {
        FmtBuffer buf;
        buf.joinStr(
            return_columns.begin(),
            return_columns.end(),
            [](const auto & nt, FmtBuffer & fb) { fb.fmtAppend("{}:{}", nt.name, nt.type->getName()); },
            ", ");
        auto [expr, cids] = tipbToTiCIExpr(match_expr_);
        match_expr = std::move(expr);
        LOG_DEBUG(log, "columns: [{}], match columns: {}", buf.toString(), cids);
    }

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

    Block readImpl() override
    {
        if (done)
        {
            return {};
        }
        if (processed_shard >= query_shard_infos.size())
        {
            LOG_DEBUG(log, "empty shard info, returning empty block");
            done = true;
            return {};
        }

        Block ret = readFromS3(processed_shard, is_count);
        processed_shard++;
        done = processed_shard >= query_shard_infos.size();
        return ret;
    }

protected:
    Block readFromS3(size_t processing, bool is_count)
    {
        auto return_fields = getFields(return_columns);
        auto & shard_info = query_shard_infos[processing];
        LOG_DEBUG(log, "Processing shard: {}, shard info: {}", processing, shard_info.toString());
        auto key_ranges = getKeyRanges(shard_info.key_ranges);

        auto search_param = SearchParam{static_cast<size_t>(limit)};
        if (is_count)
            return_fields = {};

        SearchResult search_result = search(
            {
                .keyspace_id = keyspace_id,
                .table_id = table_id,
                .index_id = index_id,
                .shard_id = shard_info.shard_id,
                .shard_epoch = shard_info.shard_epoch,
            },
            key_ranges,
            return_fields,
            match_expr,
            search_param,
            read_ts);

        Block res(return_columns);
        if (is_count)
        {
            if (search_result.count == 0)
            {
                return res;
            }
            // Construct const columns with search_result.count items, only used by count(*)
            for (auto & name_and_type : return_columns)
            {
                auto type_default_value = name_and_type.type->getDefault();
                auto col = name_and_type.type->createColumnConst(
                    static_cast<size_t>(search_result.count),
                    type_default_value);
                res.getByName(name_and_type.name).column = std::move(col);
            }
            return res;
        }


        auto documents = search_result.rows;
        if (documents.empty())
        {
            return res;
        }
        for (auto & name_and_type : return_columns)
        {
            int idx = -1;
            for (size_t j = 0; j < documents[0].fieldValues.size(); j++)
            {
                if (documents[0].fieldValues[j].field_name == name_and_type.name)
                {
                    idx = j;
                    break;
                }
            }
            if (idx == -1)
            {
                for (size_t j = 0; j < documents.size(); j++)
                {
                    // Insert default value for missing fields
                    res.getByName(name_and_type.name).column->assumeMutable()->insertDefault();
                }
                continue;
            }

            auto col = res.getByName(name_and_type.name).column->assumeMutable();
            if (removeNullable(name_and_type.type)->isStringOrFixedString())
            {
                for (auto & doc : documents)
                {
                    const auto & v = doc.fieldValues[idx].string_value;
                    col->insert(Field(String(v.begin(), v.end())));
                }
            }
            if (removeNullable(name_and_type.type)->isInteger())
            {
                for (auto & doc : documents)
                {
                    col->insert(Field(doc.fieldValues[idx].int_value));
                }
            }
        }
        return res;
    }

private:
    Block header;
    bool done = false;
    LoggerPtr log;
    UInt32 keyspace_id;
    Int64 table_id;
    Int64 index_id;
    ShardInfoList query_shard_infos;
    NamesAndTypes return_columns;
    UInt64 limit;
    UInt64 read_ts;
    ::Expr match_expr;
    bool is_count;

    size_t processed_shard = 0;

    static rust::Vec<rust::String> getFields(NamesAndTypes & columns)
    {
        rust::Vec<rust::String> fields;
        for (auto & name_and_type : columns)
        {
            fields.push_back(name_and_type.name);
        }
        return fields;
    }

    static rust::Vec<::Range> getKeyRanges(ShardInfo::KeyRanges & key_ranges)
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

    std::tuple<::Expr, std::vector<ColumnID>> tipbToTiCIExpr(google::protobuf::RepeatedPtrField<tipb::Expr> exprs)
    {
        if (exprs.empty())
        {
            throw std::runtime_error("Empty match expression");
        }
        auto [ret, cids] = tipbToTiCIExpr(exprs[0]);
        for (auto i = 1; i < exprs.size(); ++i)
        {
            auto [child_expr, child_cids] = tipbToTiCIExpr(exprs[i]);
            ret = {
                .tp = tipb::ExprType::ScalarFunc,
                .children = {ret, child_expr},
                .sig = tipb::ScalarFuncSig::LogicalAnd,
            };
            cids.insert(cids.end(), child_cids.begin(), child_cids.end());
        }
        return {ret, cids};
    }

    std::tuple<::Expr, std::vector<ColumnID>> tipbToTiCIExpr(const tipb::Expr & expr)
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
                auto [child_expr, child_cids] = tipbToTiCIExpr(child);
                ret.children.push_back(child_expr);
                children_cids.insert(children_cids.end(), child_cids.begin(), child_cids.end());
            }
            switch (expr.sig())
            {
            case tipb::ScalarFuncSig::FTSMatchWord:
            case tipb::ScalarFuncSig::FTSMatchPrefix:
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
            case tipb::ScalarFuncSig::EQTime:
            case tipb::ScalarFuncSig::NETime:
            case tipb::ScalarFuncSig::LTTime:
            case tipb::ScalarFuncSig::LETime:
            case tipb::ScalarFuncSig::GTTime:
            case tipb::ScalarFuncSig::GETime:
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
            case tipb::ScalarFuncSig::InTime:
                ret.sig = expr.sig();
                break;
            default:
                throw std::runtime_error("Unsupported expression sig");
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
                throw TiFlashException("Not decimal literal" + expr.DebugString(), Errors::Coprocessor::BadRequest);
            std::copy(str.begin(), str.end(), std::back_inserter(ret.val));
            return {ret, {}};
        }
        default:
            throw std::runtime_error("Unsupported expression type");
        }
    }
};

} // namespace DB::TS
