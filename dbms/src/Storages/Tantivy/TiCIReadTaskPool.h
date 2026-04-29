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
#include <Storages/Tantivy/TiCIRequestUtils.h>

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
        auto [expr, cids] = TS::tipbToTiCIExpr(match_expr_, timezone_info_);
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
                sort_column_ids,
                sort_column_asc,
                read_ts,
                match_expr,
                is_count,
                shards_snapshot);
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
    ::Expr match_expr;
    bool is_count;
    std::shared_ptr<rust::Box<ShardsSnapshot>> shards_snapshot;
};

using TiCIReadTaskPoolPtr = std::shared_ptr<TiCIReadTaskPool>;

} // namespace DB::TS
