// Copyright 2025 PingCAP, Inc.
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

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/ShardInfo.h>
#include <Storages/Tantivy/TantivyInputStream.h>

namespace DB::TS
{
struct TiCIReadTask
{
public:
    explicit TiCIReadTask(const ShardInfo & shard_info_)
        : shard_info(shard_info_)
    {}

    void initInputStream(
        LoggerPtr log_,
        UInt32 keyspace_id_,
        Int64 table_id_,
        Int64 index_id_,
        ShardInfo query_shard_info_,
        NamesAndTypes return_columns_,
        UInt64 limit_,
        UInt64 read_ts_,
        ::Expr match_expr_,
        bool is_count)
    {
        input_stream = std::make_shared<TantivyInputStream>(
            log_,
            keyspace_id_,
            table_id_,
            index_id_,
            query_shard_info_,
            return_columns_,
            limit_,
            read_ts_,
            match_expr_,
            is_count);
    }

    bool isInitialized() const { return input_stream != nullptr; }

    BlockInputStreamPtr getInputStream() const
    {
        RUNTIME_CHECK(input_stream != nullptr);
        return input_stream;
    }
    ShardInfo getShardInfo() const { return shard_info; }

private:
    ShardInfo shard_info;
    BlockInputStreamPtr input_stream;
};

using TiCIReadTaskPtr = std::shared_ptr<TiCIReadTask>;

struct TiCIReadTaskPool
{
public:
    using TiCIReadTasks = std::vector<std::shared_ptr<TiCIReadTask>>;

    TiCIReadTaskPool(
        LoggerPtr log_,
        UInt32 keyspace_id_,
        Int64 table_id_,
        Int64 index_id_,
        const ShardInfoList & shard_infos,
        NamesAndTypes return_columns_,
        UInt64 limit_,
        UInt64 read_ts_,
        google::protobuf::RepeatedPtrField<tipb::Expr> match_expr_,
        bool is_count,
        const TimezoneInfo & timezone_info_)
        : log(log_)
        , keyspace_id(keyspace_id_)
        , table_id(table_id_)
        , index_id(index_id_)
        , return_columns(return_columns_)
        , limit(limit_)
        , read_ts(read_ts_)
        , is_count(is_count)
    {
        for (const auto & shard_info : shard_infos)
        {
            tasks.emplace_back(std::make_shared<TiCIReadTask>(shard_info));
        }
        FmtBuffer buf;
        buf.joinStr(
            return_columns.begin(),
            return_columns.end(),
            [](const auto & nt, FmtBuffer & fb) { fb.fmtAppend("{}:{}", nt.name, nt.type->getName()); },
            ", ");
        auto [expr, cids] = tipbToTiCIExpr(match_expr_, timezone_info_);
        match_expr = std::move(expr);
        LOG_DEBUG(log, "columns: [{}], match columns: {}", buf.toString(), cids);
    }

    TiCIReadTaskPtr getNextTask()
    {
        std::lock_guard lock(mutex);
        if (tasks.empty())
            return nullptr;
        TiCIReadTaskPtr task = tasks.back();
        tasks.pop_back();
        return task;
    }

    BlockInputStreamPtr buildInputStream(TiCIReadTaskPtr & task)
    {
        RUNTIME_CHECK(task != nullptr);
        if (!task->isInitialized())
        {
            task->initInputStream(
                log,
                keyspace_id,
                table_id,
                index_id,
                task->getShardInfo(),
                return_columns,
                limit,
                read_ts,
                match_expr,
                is_count);
        }
        return task->getInputStream();
    }

private:
    mutable std::mutex mutex;
    TiCIReadTasks tasks;

    LoggerPtr log;
    UInt32 keyspace_id;
    Int64 table_id;
    Int64 index_id;
    NamesAndTypes return_columns;
    UInt64 limit;
    UInt64 read_ts;
    ::Expr match_expr;
    bool is_count;

    static std::tuple<::Expr, std::vector<ColumnID>> tipbToTiCIExpr(
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
                            throw TiFlashException(
                                "InTime only support literal values",
                                Errors::Coprocessor::BadRequest);
                        }
                    }
                }
                break;
            }
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

    static std::tuple<::Expr, std::vector<ColumnID>> tipbToTiCIExpr(
        google::protobuf::RepeatedPtrField<tipb::Expr> exprs,
        const TimezoneInfo & tz)
    {
        if (exprs.empty())
        {
            throw std::runtime_error("Empty match expression");
        }
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
};

using TiCIReadTaskPoolPtr = std::shared_ptr<TiCIReadTaskPool>;

} // namespace DB::TS
