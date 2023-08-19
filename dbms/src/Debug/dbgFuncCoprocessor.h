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

#include <Debug/DAGProperties.h>
#include <Debug/DBGInvoker.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Parsers/IAST.h>
#include <Storages/Transaction/Types.h>

namespace DB
{
class Context;

// Coprocessor debug tools

// Run a DAG request using given query that will be compiled to DAG request, with the given (optional) region ID.
// Usage:
//   ./storages-client.sh "DBGInvoke dag(query[, region_id])"
BlockInputStreamPtr dbgFuncTiDBQuery(Context & context, const ASTs & args);

void dbgFuncTiDBQueryFromNaturalDag(Context & context, const ASTs & args, DBGInvoker::Printer output);

// Mock a DAG request using given query that will be compiled (with the metadata from MockTiDB) to DAG request, with the given region ID and (optional) start ts.
// Usage:
//   ./storages-client.sh "DBGInvoke mock_dag(query, region_id[, start_ts])"
BlockInputStreamPtr dbgFuncMockTiDBQuery(Context & context, const ASTs & args);

DAGProperties getDAGProperties(const String & prop_string);

enum QueryTaskType
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
    QueryTask(std::shared_ptr<tipb::DAGRequest> request, TableID table_id_, const DAGSchema & result_schema_, QueryTaskType type_, Int64 task_id_, Int64 partition_id_, bool is_root_task_)
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
using MakeResOutputStream = std::function<BlockInputStreamPtr(BlockInputStreamPtr)>;
using SchemaFetcher = std::function<TiDB::TableInfo(const String &, const String &)>;

std::tuple<QueryTasks, MakeResOutputStream> compileQuery(
    Context & context,
    const String & query,
    SchemaFetcher schema_fetcher,
    const DAGProperties & properties);
namespace Debug
{
void setServiceAddr(const std::string & addr);
}

} // namespace DB
