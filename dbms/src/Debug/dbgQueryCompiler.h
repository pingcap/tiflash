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

#pragma once

#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataStreams/UniqRawResReformatBlockOutputStream.h>
#include <Debug/MockExecutor/AstToPB.h>
#include <Debug/MockExecutor/ExecutorBinder.h>
#include <Debug/MockTiDB.h>
#include <Debug/dbgFuncCoprocessorUtils.h>
#include <Storages/KVStore/Types.h>

namespace DB
{
using MakeResOutputStream = std::function<BlockInputStreamPtr(BlockInputStreamPtr)>;
using ExecutorBinderPtr = mock::ExecutorBinderPtr;
using TableInfo = TiDB::TableInfo;
struct ASTTablesInSelectQueryElement;

class ASTSelectQuery;

enum class QueryTaskType
{
    DAG,
    MPP_DISPATCH
};

struct QueryTask
{
    std::shared_ptr<tipb::DAGRequest> dag_request;
    TableID table_id;
    DAGSchema result_schema;
    QueryTaskType type;
    Int64 task_id;
    Int64 partition_id;
    bool is_root_task;
    QueryTask(
        std::shared_ptr<tipb::DAGRequest> request,
        TableID table_id_,
        const DAGSchema & result_schema_,
        QueryTaskType type_,
        Int64 task_id_,
        Int64 partition_id_,
        bool is_root_task_)
        : dag_request(std::move(request))
        , table_id(table_id_)
        , result_schema(result_schema_)
        , type(type_)
        , task_id(task_id_)
        , partition_id(partition_id_)
        , is_root_task(is_root_task_)
    {}
};

using QueryTasks = std::vector<QueryTask>;

struct QueryFragment
{
    ExecutorBinderPtr root_executor;
    TableID table_id;
    bool is_top_fragment;
    std::vector<Int64> sender_target_task_ids;
    std::unordered_map<String, std::vector<Int64>> receiver_source_task_ids_map;
    std::vector<Int64> task_ids;
    QueryFragment(
        ExecutorBinderPtr root_executor_,
        TableID table_id_,
        bool is_top_fragment_,
        std::vector<Int64> && sender_target_task_ids_ = {},
        std::unordered_map<String, std::vector<Int64>> && receiver_source_task_ids_map_ = {},
        std::vector<Int64> && task_ids_ = {})
        : root_executor(std::move(root_executor_))
        , table_id(table_id_)
        , is_top_fragment(is_top_fragment_)
        , sender_target_task_ids(std::move(sender_target_task_ids_))
        , receiver_source_task_ids_map(std::move(receiver_source_task_ids_map_))
        , task_ids(std::move(task_ids_))
    {}

    QueryTask toQueryTask(const DAGProperties & properties, MPPInfo & mpp_info, const Context & context) const
    {
        std::shared_ptr<tipb::DAGRequest> dag_request_ptr = std::make_shared<tipb::DAGRequest>();
        tipb::DAGRequest & dag_request = *dag_request_ptr;
        dag_request.set_time_zone_name(properties.tz_name);
        dag_request.set_time_zone_offset(properties.tz_offset);
        dag_request.set_flags(
            dag_request.flags() | (1u << 1u /* TRUNCATE_AS_WARNING */) | (1u << 6u /* OVERFLOW_AS_WARNING */));
        if (is_top_fragment)
        {
            if (properties.encode_type == "chunk")
                dag_request.set_encode_type(tipb::EncodeType::TypeChunk);
            else if (properties.encode_type == "chblock")
                dag_request.set_encode_type(tipb::EncodeType::TypeCHBlock);
            else
                dag_request.set_encode_type(tipb::EncodeType::TypeDefault);
        }
        else
        {
            dag_request.set_encode_type(tipb::EncodeType::TypeCHBlock);
        }

        for (size_t i = 0; i < root_executor->output_schema.size(); ++i)
            dag_request.add_output_offsets(i);
        auto * root_tipb_executor = dag_request.mutable_root_executor();
        root_executor->toTiPBExecutor(root_tipb_executor, properties.collator, mpp_info, context);
        return QueryTask(
            dag_request_ptr,
            table_id,
            root_executor->output_schema,
            mpp_info.sender_target_task_ids.empty() ? QueryTaskType::DAG : QueryTaskType::MPP_DISPATCH,
            mpp_info.task_id,
            mpp_info.partition_id,
            is_top_fragment);
    }

    QueryTasks toQueryTasks(const DAGProperties & properties, const Context & context) const
    {
        QueryTasks ret;
        if (properties.is_mpp_query)
        {
            for (size_t partition_id = 0; partition_id < task_ids.size(); ++partition_id)
            {
                MPPInfo mpp_info(
                    properties.start_ts,
                    properties.gather_id,
                    properties.query_ts,
                    properties.server_id,
                    properties.local_query_id,
                    partition_id,
                    task_ids[partition_id],
                    sender_target_task_ids,
                    receiver_source_task_ids_map);
                ret.push_back(toQueryTask(properties, mpp_info, context));
            }
        }
        else
        {
            MPPInfo mpp_info(
                properties.start_ts,
                properties.gather_id,
                properties.query_ts,
                properties.server_id,
                properties.local_query_id,
                /*partition_id*/ -1,
                /*task_id*/ -1,
                /*sender_target_task_ids*/ {},
                /*receiver_source_task_ids_map*/ {});
            ret.push_back(toQueryTask(properties, mpp_info, context));
        }
        return ret;
    }
};

using QueryFragments = std::vector<QueryFragment>;
using SchemaFetcher = std::function<TiDB::TableInfo(const String &, const String &)>;

std::tuple<QueryTasks, MakeResOutputStream> compileQuery(
    Context & context,
    const String & query,
    SchemaFetcher schema_fetcher,
    const DAGProperties & properties);

QueryTasks queryPlanToQueryTasks(
    const DAGProperties & properties,
    mock::ExecutorBinderPtr root_executor,
    size_t & executor_index,
    const Context & context);

const ASTTablesInSelectQueryElement * getJoin(ASTSelectQuery & ast_query);
} // namespace DB