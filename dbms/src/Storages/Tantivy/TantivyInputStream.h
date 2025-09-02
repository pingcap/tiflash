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
        Int64 table_id_,
        Int64 index_id_,
        ShardInfoList query_shard_infos_,
        NamesAndTypes return_columns_,
        UInt64 limit_,
        UInt64 read_ts_,
        google::protobuf::RepeatedPtrField<tipb::Expr> match_expr_)
        : log(log_)
        , table_id(table_id_)
        , index_id(index_id_)
        , query_shard_infos(query_shard_infos_)
        , return_columns(return_columns_)
        , limit(limit_)
        , read_ts(read_ts_)
    {
        FmtBuffer buf;
        buf.joinStr(
            return_columns.begin(),
            return_columns.end(),
            [](const auto & nt, FmtBuffer & fb) { fb.fmtAppend("{}:{}", nt.name, nt.type->getName()); },
            ", ");
        auto [expr, cids] = tipbToTiCIExpr(match_expr_);
        match_expr = std::move(expr);
        LOG_INFO(log, "columns: [{}], match columns: {}", buf.toString(), cids);
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
            LOG_INFO(log, "empty shard info, returning empty block");
            done = true;
            return {};
        }

        Block ret = readFromS3(processed_shard);
        processed_shard++;
        done = processed_shard >= query_shard_infos.size();
        return ret;
    }

protected:
    Block readFromS3(size_t processing)
    {
        auto return_fields = getFields(return_columns);
        auto & shard_info = query_shard_infos[processing];
        LOG_INFO(log, "Processing shard: {}, shard info: {}", processing, shard_info.toString());
        auto key_ranges = getKeyRanges(shard_info.key_ranges);

        auto search_param = SearchParam{static_cast<size_t>(limit)};
        rust::Vec<IdDocument> documents = search(
            {
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
                    col->insert(Field(String(doc.fieldValues[idx].string_value.c_str())));
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
    Int64 table_id;
    Int64 index_id;
    ShardInfoList query_shard_infos;
    NamesAndTypes return_columns;
    UInt64 limit;
    UInt64 read_ts;
    ::Expr match_expr;

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
                ret.sig = expr.sig();
                break;
            default:
                throw std::runtime_error("Unsupported expression sig");
            }

            return {ret, children_cids};
        }
        case tipb::ExprType::ColumnRef:
        {
            ret.tp = tipb::ExprType::ColumnRef;
            auto id = decodeDAGInt64(expr.val());
            ret.val = fmt::format("column_{}", id);
            return {ret, {id}};
        }
        case tipb::ExprType::String:
        {
            ret.tp = tipb::ExprType::String;
            ret.val = expr.val();
            return {ret, {}};
        }
        default:
            throw std::runtime_error("Unsupported expression type");
        }
    }
};

} // namespace DB::TS
