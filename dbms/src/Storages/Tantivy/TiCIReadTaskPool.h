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

#include <cmath>
#include <cstring>

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

    // FTS init
    void initInputStream(
        LoggerPtr log_,
        UInt32 keyspace_id_,
        Int64 table_id_,
        Int64 index_id_,
        ShardInfo query_shard_info_,
        NamesAndTypes return_columns_,
        UInt64 limit_,
        std::vector<Int64> sort_column_ids_,
        std::vector<bool> sort_column_asc_,
        UInt64 read_ts_,
        ::Expr match_expr_,
        bool is_count,
        const std::shared_ptr<rust::Box<ShardsSnapshot>> & shards_snapshot_)
    {
        input_stream = std::make_shared<TantivyInputStream>(
            log_,
            keyspace_id_,
            table_id_,
            index_id_,
            query_shard_info_,
            return_columns_,
            limit_,
            sort_column_ids_,
            sort_column_asc_,
            read_ts_,
            match_expr_,
            is_count,
            shards_snapshot_);
    }

    // Vector init
    void initInputStreamVector(
        LoggerPtr log_,
        UInt32 keyspace_id_,
        Int64 table_id_,
        Int64 index_id_,
        ShardInfo query_shard_info_,
        NamesAndTypes return_columns_,
        UInt64 read_ts_,
        VectorQueryState vector_state_,
        const std::shared_ptr<rust::Box<ShardsSnapshot>> & shards_snapshot_)
    {
        input_stream = std::make_shared<TantivyInputStream>(
            log_,
            keyspace_id_,
            table_id_,
            index_id_,
            query_shard_info_,
            return_columns_,
            read_ts_,
            std::move(vector_state_),
            shards_snapshot_);
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

    // FTS constructor
    TiCIReadTaskPool(
        LoggerPtr log_,
        UInt32 keyspace_id_,
        Int64 table_id_,
        Int64 index_id_,
        const ShardInfoList & shard_infos,
        NamesAndTypes return_columns_,
        UInt64 limit_,
        std::vector<Int64> sort_column_ids_,
        std::vector<bool> sort_column_asc_,
        UInt64 read_ts_,
        google::protobuf::RepeatedPtrField<tipb::Expr> match_expr_,
        bool is_count,
        const TimezoneInfo & timezone_info_,
        rust::Box<ShardsSnapshot> shards_snapshot_)
        : log(log_)
        , keyspace_id(keyspace_id_)
        , table_id(table_id_)
        , index_id(index_id_)
        , return_columns(return_columns_)
        , limit(limit_)
        , sort_column_ids(sort_column_ids_)
        , sort_column_asc(sort_column_asc_)
        , read_ts(read_ts_)
        , query_mode(TiCIQueryMode::FTS)
        , is_count(is_count)
        , shards_snapshot(std::make_shared<rust::Box<ShardsSnapshot>>(std::move(shards_snapshot_)))
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

    // Vector constructor
    TiCIReadTaskPool(
        LoggerPtr log_,
        UInt32 keyspace_id_,
        Int64 table_id_,
        Int64 index_id_,
        const ShardInfoList & shard_infos,
        NamesAndTypes return_columns_,
        UInt64 read_ts_,
        VectorQueryState vector_state_,
        rust::Box<ShardsSnapshot> shards_snapshot_)
        : log(log_)
        , keyspace_id(keyspace_id_)
        , table_id(table_id_)
        , index_id(index_id_)
        , return_columns(return_columns_)
        , limit(vector_state_.top_k)
        , read_ts(read_ts_)
        , query_mode(TiCIQueryMode::Vector)
        , vector_state(std::move(vector_state_))
        , is_count(false)
        , shards_snapshot(std::make_shared<rust::Box<ShardsSnapshot>>(std::move(shards_snapshot_)))
    {
        for (const auto & shard_info : shard_infos)
        {
            tasks.emplace_back(std::make_shared<TiCIReadTask>(shard_info));
        }
        LOG_DEBUG(
            log,
            "vector query: col_id={} distance_metric={} top_k={} dim={} has_filter={}",
            vector_state.col_id,
            vector_state.distance_metric,
            vector_state.top_k,
            vector_state.query_vector.size(),
            vector_state.has_filter);
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
            if (query_mode == TiCIQueryMode::Vector)
            {
                task->initInputStreamVector(
                    log,
                    keyspace_id,
                    table_id,
                    index_id,
                    task->getShardInfo(),
                    return_columns,
                    read_ts,
                    vector_state,
                    shards_snapshot);
            }
            else
            {
                task->initInputStream(
                    log,
                    keyspace_id,
                    table_id,
                    index_id,
                    task->getShardInfo(),
                    return_columns,
                    limit,
                    sort_column_ids,
                    sort_column_asc,
                    read_ts,
                    match_expr,
                    is_count,
                    shards_snapshot);
            }
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
    std::vector<Int64> sort_column_ids;
    std::vector<bool> sort_column_asc;
    UInt64 read_ts;
    TiCIQueryMode query_mode;
    ::Expr match_expr; // FTS mode
    VectorQueryState vector_state; // Vector mode
    bool is_count;
    std::shared_ptr<rust::Box<ShardsSnapshot>> shards_snapshot;

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

public:
    /// Convert tipb filter expressions from TiCIVectorQueryInfo to a TiCI Expr.
    /// Returns an empty Expr if there are no filter expressions.
    static VectorQueryState buildVectorState(
        const tipb::TiCIVectorQueryInfo & info,
        const TimezoneInfo & timezone_info)
    {
        VectorQueryState state;
        RUNTIME_CHECK_MSG(info.top_k() > 0, "TiCI vector query top_k must be greater than 0");
        RUNTIME_CHECK_MSG(info.column_id() != 0, "TiCI vector query column_id must not be 0");
        RUNTIME_CHECK_MSG(info.dimension() > 0, "TiCI vector query dimension must be greater than 0");
        RUNTIME_CHECK_MSG(
            info.distance_metric() == tipb::VectorDistanceMetric::L2
                || info.distance_metric() == tipb::VectorDistanceMetric::COSINE,
            "Unsupported TiCI vector distance metric: {}",
            static_cast<Int32>(info.distance_metric()));

        state.col_id = info.column_id();
        state.distance_metric = static_cast<Int32>(info.distance_metric());
        state.top_k = info.top_k();

        // TiDB currently serializes VectorFloat32 with a 4-byte little-endian
        // dimension prefix. Keep accepting the original raw-float payload too
        // so TiFlash stays compatible with both callers during rollout.
        const auto & qv = info.query_vector();
        RUNTIME_CHECK_MSG(!qv.empty(), "TiCI vector query_vector must not be empty");
        RUNTIME_CHECK_MSG(
            qv.size() % sizeof(float) == 0,
            "Malformed TiCI query_vector payload: {} bytes is not a multiple of {}",
            qv.size(),
            sizeof(float));
        const auto expected_bytes = static_cast<size_t>(info.dimension()) * sizeof(float);
        size_t qv_offset = 0;
        size_t qv_bytes = qv.size();
        if (qv.size() == expected_bytes + sizeof(UInt32))
        {
            UInt32 encoded_dimension = 0;
            std::memcpy(&encoded_dimension, qv.data(), sizeof(UInt32));
            if (encoded_dimension == info.dimension())
            {
                qv_offset = sizeof(UInt32);
                qv_bytes = expected_bytes;
            }
        }
        RUNTIME_CHECK_MSG(
            qv_bytes == expected_bytes,
            "TiCI query_vector length mismatch: expected {} bytes for dimension {}, got {} bytes",
            expected_bytes,
            info.dimension(),
            qv.size());
        state.query_vector.reserve(info.dimension());
        for (size_t offset = qv_offset; offset < qv.size(); offset += sizeof(float))
        {
            float value = 0;
            std::memcpy(&value, qv.data() + offset, sizeof(float));
            RUNTIME_CHECK_MSG(
                std::isfinite(value),
                "TiCI query_vector contains non-finite value at offset {}",
                offset / sizeof(float));
            state.query_vector.push_back(value);
        }

        // Convert filter expressions.
        state.has_filter = (info.filter_expr_size() > 0);
        if (state.has_filter)
        {
            auto [expr, _cids] = tipbToTiCIExpr(info.filter_expr(), timezone_info);
            state.filter_expr = std::move(expr);
        }
        return state;
    }
};

using TiCIReadTaskPoolPtr = std::shared_ptr<TiCIReadTaskPool>;

} // namespace DB::TS
