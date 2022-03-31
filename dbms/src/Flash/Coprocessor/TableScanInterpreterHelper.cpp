// Copyright 2022 PingCAP, Ltd.
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

#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/TableScanInterpreterHelper.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <kvproto/coprocessor.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <Interpreters/ExpressionActions.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>

namespace DB::TableScanInterpreterHelper
{
namespace
{
bool schemaMatch(const DAGSchema & left, const DAGSchema & right)
{
    if (left.size() != right.size())
        return false;
    for (size_t i = 0; i < left.size(); i++)
    {
        const auto & left_ci = left[i];
        const auto & right_ci = right[i];
        if (left_ci.second.tp != right_ci.second.tp)
            return false;
        if (left_ci.second.flag != right_ci.second.flag)
            return false;
    }
    return true;
}

// add timezone cast for timestamp type, this is used to support session level timezone
bool addExtraCastsAfterTs(
    DAGExpressionAnalyzer & analyzer,
    const std::vector<ExtraCastAfterTSMode> & need_cast_column,
    ExpressionActionsChain & chain,
    const tipb::TableScan & table_scan)
{
    bool has_need_cast_column = false;
    for (auto b : need_cast_column)
    {
        has_need_cast_column |= (b != ExtraCastAfterTSMode::None);
    }
    if (!has_need_cast_column)
        return false;
    return analyzer.appendExtraCastsAfterTS(chain, need_cast_column, table_scan);
}
}

void executeRemoteQueryImpl(
    const Context & context,
    DAGPipeline & pipeline,
    std::vector<RemoteRequest> & remote_requests)
{
    assert(!remote_requests.empty());
    DAGSchema & schema = remote_requests[0].schema;
#ifndef NDEBUG
    for (size_t i = 1; i < remote_requests.size(); ++i)
    {
        if (!schemaMatch(schema, remote_requests[i].schema))
            throw Exception("Schema mismatch between different partitions for partition table");
    }
#endif
    bool has_enforce_encode_type = remote_requests[0].dag_request.has_force_encode_type() && remote_requests[0].dag_request.force_encode_type();
    pingcap::kv::Cluster * cluster = context.getTMTContext().getKVCluster();
    std::vector<pingcap::coprocessor::copTask> all_tasks;
    for (const auto & remote_request : remote_requests)
    {
        pingcap::coprocessor::RequestPtr req = std::make_shared<pingcap::coprocessor::Request>();
        remote_request.dag_request.SerializeToString(&(req->data));
        req->tp = pingcap::coprocessor::ReqType::DAG;
        req->start_ts = context.getSettingsRef().read_tso;
        req->schema_version = context.getSettingsRef().schema_version;

        pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
        pingcap::kv::StoreType store_type = pingcap::kv::StoreType::TiFlash;
        auto tasks = pingcap::coprocessor::buildCopTasks(bo, cluster, remote_request.key_ranges, req, store_type, &Poco::Logger::get("pingcap/coprocessor"));
        all_tasks.insert(all_tasks.end(), tasks.begin(), tasks.end());
    }

    size_t concurrent_num = std::min<size_t>(context.getSettingsRef().max_threads, all_tasks.size());
    size_t task_per_thread = all_tasks.size() / concurrent_num;
    size_t rest_task = all_tasks.size() % concurrent_num;
    for (size_t i = 0, task_start = 0; i < concurrent_num; i++)
    {
        size_t task_end = task_start + task_per_thread;
        if (i < rest_task)
            task_end++;
        if (task_end == task_start)
            continue;
        std::vector<pingcap::coprocessor::copTask> tasks(all_tasks.begin() + task_start, all_tasks.begin() + task_end);

        auto coprocessor_reader = std::make_shared<CoprocessorReader>(schema, cluster, tasks, has_enforce_encode_type, 1);
        BlockInputStreamPtr input = std::make_shared<CoprocessorBlockInputStream>(coprocessor_reader, log->identifier(), query_block.source_name);
        pipeline.streams.push_back(input);
        task_start = task_end;
    }
}

void executeCastAfterTableScan(
    const Context & context,
    DAGExpressionAnalyzer & analyzer,
    const std::vector<ExtraCastAfterTSMode> & is_need_add_cast_column,
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline)
{
    auto original_source_columns = analyzer.getCurrentInputColumns();

    ExpressionActionsChain chain;
    analyzer.initChain(chain, original_source_columns);

    // execute timezone cast or duration cast if needed for local table scan
    if (addExtraCastsAfterTs(analyzer, is_need_add_cast_column, chain, query_block.source->tbl_scan()))
    {
        ExpressionActionsPtr extra_cast = chain.getLastActions();
        chain.finalize();
        chain.clear();

        // After `addExtraCastsAfterTs`, analyzer->getCurrentInputColumns() has been modified.
        // For remote read, `timezone cast and duration cast` had been pushed down, don't need to execute cast expressions.
        // To keep the schema of local read streams and remote read streams the same, do project action for remote read streams.
        NamesWithAliases project_for_remote_read;
        const auto & after_cast_source_columns = analyzer.getCurrentInputColumns();
        for (size_t i = 0; i < after_cast_source_columns.size(); ++i)
        {
            project_for_remote_read.emplace_back(original_source_columns[i].name, after_cast_source_columns[i].name);
        }
        assert(!project_for_remote_read.empty());
        assert(pipeline.streams_with_non_joined_data.empty());
        assert(remote_read_streams_start_index <= pipeline.streams.size());
        size_t i = 0;
        // local streams
        while (i < remote_read_streams_start_index)
        {
            auto & stream = pipeline.streams[i++];
            stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, log->identifier());
        }
        // remote streams
        if (i < pipeline.streams.size())
        {
            ExpressionActionsPtr project_for_cop_read = generateProjectExpressionActions(
                pipeline.streams[i],
                context,
                project_for_remote_read);
            while (i < pipeline.streams.size())
            {
                auto & stream = pipeline.streams[i++];
                stream = std::make_shared<ExpressionBlockInputStream>(stream, project_for_cop_read, log->identifier());
            }
        }
    }
}

void executePushedDownFilter(
    const std::vector<const tipb::Expr *> & conditions,
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline)
{

}
} // namespace DB::TableScanInterpreterHelper