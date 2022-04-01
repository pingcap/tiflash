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

#include <Common/FailPoint.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/TableScanInterpreterHelper.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Transaction/TiDB.h>
#include <kvproto/coprocessor.pb.h>
#include <pingcap/coprocessor/Client.h>

#include <memory>
#include <vector>

namespace DB
{
namespace FailPoints
{
extern const char pause_after_copr_streams_acquired[];
extern const char minimum_block_size_for_cross_join[];
} // namespace FailPoints

namespace TableScanInterpreterHelper
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
    const TiDBTableScan & table_scan)
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

void setQuotaAndLimitsOnTableScan(Context & context, DAGPipeline & pipeline)
{
    const Settings & settings = context.getSettingsRef();

    IProfilingBlockInputStream::LocalLimits limits;
    limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
    limits.size_limits = SizeLimits(settings.max_rows_to_read, settings.max_bytes_to_read, settings.read_overflow_mode);
    limits.max_execution_time = settings.max_execution_time;
    limits.timeout_overflow_mode = settings.timeout_overflow_mode;

    /** Quota and minimal speed restrictions are checked on the initiating server of the request, and not on remote servers,
          *  because the initiating server has a summary of the execution of the request on all servers.
          *
          * But limits on data size to read and maximum execution time are reasonable to check both on initiator and
          *  additionally on each remote server, because these limits are checked per block of data processed,
          *  and remote servers may process way more blocks of data than are received by initiator.
          */
    limits.min_execution_speed = settings.min_execution_speed;
    limits.timeout_before_checking_execution_speed = settings.timeout_before_checking_execution_speed;

    QuotaForIntervals & quota = context.getQuota();

    pipeline.transform([&](auto & stream) {
        if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
        {
            p_stream->setLimits(limits);
            p_stream->setQuota(quota);
        }
    });
}

ExpressionActionsPtr generateProjectExpressionActions(
    const BlockInputStreamPtr & stream,
    const Context & context,
    const NamesWithAliases & project_cols)
{
    auto columns = stream->getHeader();
    NamesAndTypesList input_column;
    for (const auto & column : columns.getColumnsWithTypeAndName())
    {
        input_column.emplace_back(column.name, column.type);
    }
    ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column, context.getSettingsRef());
    project->add(ExpressionAction::project(project_cols));
    return project;
}

void recordProfileStreams(DAGContext & dag_context, DAGPipeline & pipeline, const String & key)
{
    auto & profile_streams = dag_context.getProfileStreamsMap()[key];
    pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
}

void executeRemoteQueryImpl(
    const Context & context,
    DAGPipeline & pipeline,
    const String & table_scan_executor_id,
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
    const String & req_id = context.getDAGContext()->log->identifier();
    for (size_t i = 0, task_start = 0; i < concurrent_num; i++)
    {
        size_t task_end = task_start + task_per_thread;
        if (i < rest_task)
            task_end++;
        if (task_end == task_start)
            continue;
        std::vector<pingcap::coprocessor::copTask> tasks(all_tasks.begin() + task_start, all_tasks.begin() + task_end);

        auto coprocessor_reader = std::make_shared<CoprocessorReader>(schema, cluster, tasks, has_enforce_encode_type, 1);
        BlockInputStreamPtr input = std::make_shared<CoprocessorBlockInputStream>(coprocessor_reader, req_id, table_scan_executor_id);
        pipeline.streams.push_back(input);
        task_start = task_end;
    }
}

void executeCastAfterTableScan(
    const Context & context,
    const TiDBTableScan & table_scan,
    DAGExpressionAnalyzer & analyzer,
    const std::vector<ExtraCastAfterTSMode> & is_need_add_cast_column,
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline)
{
    auto original_source_columns = analyzer.getCurrentInputColumns();

    ExpressionActionsChain chain;
    analyzer.initChain(chain, original_source_columns);

    // execute timezone cast or duration cast if needed for local table scan
    if (addExtraCastsAfterTs(analyzer, is_need_add_cast_column, chain, table_scan))
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
        const String req_id = context.getDAGContext()->log->identifier();
        // local streams
        while (i < remote_read_streams_start_index)
        {
            auto & stream = pipeline.streams[i++];
            stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, req_id);
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
                stream = std::make_shared<ExpressionBlockInputStream>(stream, project_for_cop_read, req_id);
            }
        }
    }
}

void executePushedDownFilter(
    DAGExpressionAnalyzer & analyzer,
    const std::vector<const tipb::Expr *> & conditions,
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline,
    const String & req_id)
{
    ExpressionActionsChain chain;
    analyzer.initChain(chain, analyzer.getCurrentInputColumns());
    String filter_column_name = analyzer.appendWhere(chain, conditions);
    ExpressionActionsPtr before_where = chain.getLastActions();
    chain.addStep();

    // remove useless tmp column and keep the schema of local streams and remote streams the same.
    NamesWithAliases project_cols;
    for (const auto & col : analyzer.getCurrentInputColumns())
    {
        chain.getLastStep().required_output.push_back(col.name);
        project_cols.emplace_back(col.name, col.name);
    }
    chain.getLastActions()->add(ExpressionAction::project(project_cols));
    ExpressionActionsPtr project_after_where = chain.getLastActions();
    chain.finalize();
    chain.clear();

    assert(pipeline.streams_with_non_joined_data.empty());
    assert(remote_read_streams_start_index <= pipeline.streams.size());
    // for remote read, filter had been pushed down, don't need to execute again.
    for (size_t i = 0; i < remote_read_streams_start_index; ++i)
    {
        auto & stream = pipeline.streams[i];
        stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, req_id);
        // after filter, do project action to keep the schema of local streams and remote streams the same.
        stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, req_id);
    }
}
} // namespace

std::unique_ptr<DAGExpressionAnalyzer> handleTableScan(
    Context & context,
    const TiDBTableScan & table_scan,
    const String & filter_executor_id,
    const std::vector<const tipb::Expr *> & conditions,
    DAGPipeline & pipeline,
    size_t max_streams)
{
    DAGContext & dag_context = *context.getDAGContext();
    bool has_region_to_read = false;
    for (const auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & table_regions_info = dag_context.getTableRegionsInfoByTableID(physical_table_id);
        if (!table_regions_info.local_regions.empty() || !table_regions_info.remote_regions.empty())
        {
            has_region_to_read = true;
            break;
        }
    }
    if (!has_region_to_read)
        throw TiFlashException(
            fmt::format("Dag Request does not have region to read for table: {}", table_scan.getLogicalTableID()),
            Errors::Coprocessor::BadRequest);

    DAGStorageInterpreter storage_interpreter(context, table_scan, filter_executor_id, conditions, max_streams);
    storage_interpreter.execute(pipeline);

    std::unique_ptr<DAGExpressionAnalyzer> analyzer = std::move(storage_interpreter.analyzer);

    auto remote_requests = std::move(storage_interpreter.remote_requests);
    auto null_stream_if_empty = std::move(storage_interpreter.null_stream_if_empty);

    // It is impossible to have no joined stream.
    assert(pipeline.streams_with_non_joined_data.empty());
    // after executeRemoteQueryImpl, remote read stream will be appended in pipeline.streams.
    size_t remote_read_streams_start_index = pipeline.streams.size();

    // For those regions which are not presented in this tiflash node, we will try to fetch streams by key ranges from other tiflash nodes, only happens in batch cop / mpp mode.
    if (!remote_requests.empty())
        executeRemoteQueryImpl(context, pipeline, table_scan.getTableScanExecutorID(), remote_requests);

    /// record local and remote io input stream
    auto & table_scan_io_input_streams = dag_context.getInBoundIOInputStreamsMap()[table_scan.getTableScanExecutorID()];
    pipeline.transform([&](auto & stream) { table_scan_io_input_streams.push_back(stream); });

    if (pipeline.streams.empty())
    {
        pipeline.streams.emplace_back(null_stream_if_empty);
        // reset remote_read_streams_start_index for null_stream_if_empty.
        remote_read_streams_start_index = 1;
    }

    /// Theoretically we could move addTableLock to DAGStorageInterpreter, but we don't wants to the table to be dropped
    /// during the lifetime of this query, and sometimes if there is no local region, we will use the RemoteBlockInputStream
    /// or even the null_stream to hold the lock, so I would like too keep the addTableLock in DAGQueryBlockInterpreter
    pipeline.transform([&](auto & stream) {
        // todo do not need to hold all locks in each stream, if the stream is reading from table a
        //  it only needs to hold the lock of table a
        for (auto & lock : storage_interpreter.drop_locks)
            stream->addTableLock(lock);
    });

    /// Set the limits and quota for reading data, the speed and time of the query.
    setQuotaAndLimitsOnTableScan(context, pipeline);
    FAIL_POINT_PAUSE(FailPoints::pause_after_copr_streams_acquired);

    /// handle timezone/duration cast for local and remote table scan.
    executeCastAfterTableScan(
        context,
        table_scan,
        *analyzer,
        storage_interpreter.is_need_add_cast_column,
        remote_read_streams_start_index,
        pipeline);
    recordProfileStreams(dag_context, pipeline, table_scan.getTableScanExecutorID());

    /// handle pushed down filter for local and remote table scan.
    if (!conditions.empty())
    {
        executePushedDownFilter(*analyzer, conditions, remote_read_streams_start_index, pipeline, dag_context.log->identifier());
        recordProfileStreams(dag_context, pipeline, filter_executor_id);
    }

    return analyzer;
}
} // namespace TableScanInterpreterHelper
} // namespace DB