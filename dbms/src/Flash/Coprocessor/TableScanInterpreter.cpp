#include <Common/FailPoint.h>
#include <Common/TiFlashException.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/TableScanInterpreter.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_after_copr_streams_acquired[];
} // namespace FailPoints

TableScanInterpreter::TableScanInterpreter(
    Context & context_,
    const DAGQueryBlock & query_block_,
    size_t max_streams_,
    bool keep_session_timezone_info_,
    const DAGQuerySource & dag_,
    const LogWithPrefixPtr & log_)
    : DAGInterpreterBase(
        context_,
        query_block_,
        max_streams_,
        keep_session_timezone_info_,
        dag_,
        log_)
{
    assert(query_block.source->tp() == tipb::ExecType::TypeTableScan);
}

namespace
{
// add timezone cast for timestamp type, this is used to support session level timezone
bool addExtraCastsAfterTs(
    DAGExpressionAnalyzer & analyzer,
    const std::vector<ExtraCastAfterTSMode> & need_cast_column,
    DAGExpressionActionsChain & chain,
    const DAGQueryBlock & query_block)
{
    bool has_need_cast_column = false;
    for (auto b : need_cast_column)
    {
        has_need_cast_column |= (b != ExtraCastAfterTSMode::None);
    }
    if (!has_need_cast_column)
        return false;
    return analyzer.appendExtraCastsAfterTS(chain, need_cast_column, query_block);
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

void copyExecutorTreeWithLocalTableScan(
    tipb::DAGRequest & dag_req,
    const tipb::Executor * root,
    const tipb::DAGRequest & org_req)
{
    const tipb::Executor * current = root;
    auto * exec = dag_req.mutable_root_executor();
    while (current->tp() != tipb::ExecType::TypeTableScan)
    {
        exec->set_tp(current->tp());
        exec->set_executor_id(current->executor_id());
        if (current->tp() == tipb::ExecType::TypeSelection)
        {
            auto * sel = exec->mutable_selection();
            for (auto const & condition : current->selection().conditions())
            {
                auto * tmp = sel->add_conditions();
                tmp->CopyFrom(condition);
            }
            exec = sel->mutable_child();
            current = &current->selection().child();
        }
        else if (current->tp() == tipb::ExecType::TypeAggregation || current->tp() == tipb::ExecType::TypeStreamAgg)
        {
            auto * agg = exec->mutable_aggregation();
            for (auto const & expr : current->aggregation().agg_func())
            {
                auto * tmp = agg->add_agg_func();
                tmp->CopyFrom(expr);
            }
            for (auto const & expr : current->aggregation().group_by())
            {
                auto * tmp = agg->add_group_by();
                tmp->CopyFrom(expr);
            }
            agg->set_streamed(current->aggregation().streamed());
            exec = agg->mutable_child();
            current = &current->aggregation().child();
        }
        else if (current->tp() == tipb::ExecType::TypeLimit)
        {
            auto * limit = exec->mutable_limit();
            limit->set_limit(current->limit().limit());
            exec = limit->mutable_child();
            current = &current->limit().child();
        }
        else if (current->tp() == tipb::ExecType::TypeTopN)
        {
            auto * topn = exec->mutable_topn();
            topn->set_limit(current->topn().limit());
            for (auto const & expr : current->topn().order_by())
            {
                auto * tmp = topn->add_order_by();
                tmp->CopyFrom(expr);
            }
            exec = topn->mutable_child();
            current = &current->topn().child();
        }
        else
        {
            throw TiFlashException("Not supported yet", Errors::Coprocessor::Unimplemented);
        }
    }

    if (current->tp() != tipb::ExecType::TypeTableScan)
        throw TiFlashException("Only support copy from table scan sourced query block", Errors::Coprocessor::Internal);
    exec->set_tp(tipb::ExecType::TypeTableScan);
    exec->set_executor_id(current->executor_id());
    auto * new_ts = new tipb::TableScan(current->tbl_scan());
    new_ts->set_next_read_engine(tipb::EngineType::Local);
    exec->set_allocated_tbl_scan(new_ts);

    /// force the encode type to be TypeCHBlock, so the receiver side does not need to handle the timezone related issues
    dag_req.set_encode_type(tipb::EncodeType::TypeCHBlock);
    dag_req.set_force_encode_type(true);
    if (org_req.has_time_zone_name() && !org_req.time_zone_name().empty())
        dag_req.set_time_zone_name(org_req.time_zone_name());
    else if (org_req.has_time_zone_offset())
        dag_req.set_time_zone_offset(org_req.time_zone_offset());
}

ExpressionActionsPtr getProjectionAfterCastForRemoteRead(
    const Context & context,
    const NamesAndTypes & old_schema,
    const NamesAndTypes & new_schema)
{
    if (old_schema.size() != new_schema.size())
        throw Exception("Schema size mismatch after extra cast!", ErrorCodes::LOGICAL_ERROR);

    NamesWithAliases projection;
    ColumnsWithTypeAndName columns;
    for (size_t i = 0; i < old_schema.size(); ++i)
    {
        columns.emplace_back(old_schema[i].type, old_schema[i].name);
        projection.emplace_back(old_schema[i].name, new_schema[i].name);
    }
    return generateProjectExpressionActions(Block{columns}, context, projection);
}

} // namespace

// the flow is the same as executeFetchcolumns
void TableScanInterpreter::executeTS(const tipb::TableScan & ts, DAGPipeline & pipeline)
{
    if (!ts.has_table_id())
    {
        // do not have table id
        throw TiFlashException("Table id not specified in table scan executor", Errors::Coprocessor::BadRequest);
    }
    if (dag.getRegions().empty() && dag.getRegionsForRemoteRead().empty())
    {
        throw TiFlashException("Dag Request does not have region to read. ", Errors::Coprocessor::BadRequest);
    }

    DAGStorageInterpreter storage_interpreter(context, dag, query_block, ts, conditions, max_streams, log);
    storage_interpreter.execute(pipeline);
    is_remote_table_scan.assign(pipeline.streams.size(), false);

    analyzer = std::move(storage_interpreter.analyzer);
    need_add_cast_column_flag_for_tablescan = std::move(storage_interpreter.is_need_add_cast_column);

    // The DeltaTree engine ensures that once input streams are created, the caller can get a consistent result
    // from those streams even if DDL operations are applied. Release the alter lock so that reading does not
    // block DDL operations, keep the drop lock so that the storage not to be dropped during reading.
    std::tie(std::ignore, table_drop_lock) = std::move(storage_interpreter.table_structure_lock).release();

    auto region_retry = std::move(storage_interpreter.region_retry);
    auto dag_req = std::move(storage_interpreter.dag_request);
    auto schema = std::move(storage_interpreter.dag_schema);
    auto null_stream_if_empty = std::move(storage_interpreter.null_stream_if_empty);

    // For those regions which are not presented in this tiflash node, we will try to fetch streams by key ranges from other tiflash nodes, only happens in batch cop mode.
    if (!region_retry.empty())
    {
#ifndef NDEBUG
        if (unlikely(!dag_req.has_value() || !schema.has_value()))
            throw TiFlashException(
                "Try to read from remote but can not build DAG request. Should not happen!",
                Errors::Coprocessor::Internal);
#endif
        std::vector<pingcap::coprocessor::KeyRange> ranges;
        for (auto & info : region_retry)
        {
            for (const auto & range : info.get().key_ranges)
                ranges.emplace_back(*range.first, *range.second);
        }
        sort(ranges.begin(), ranges.end());
        executeRemoteQueryImpl(pipeline, ranges, *dag_req, *schema);
    }

    if (pipeline.streams.empty())
    {
        pipeline.streams.emplace_back(null_stream_if_empty);
    }

    pipeline.transform([&](auto & stream) { stream->addTableLock(table_drop_lock); });

    /// Set the limits and quota for reading data, the speed and time of the query.
    setQuotaAndLimitsOnTableScan(context, pipeline);
    FAIL_POINT_PAUSE(FailPoints::pause_after_copr_streams_acquired);
}

void TableScanInterpreter::executeRemoteQuery(DAGPipeline & pipeline)
{
    // remote query containing agg/limit/topN can not running
    // in parellel, but current remote query is running in
    // parellel, so just disable this corner case.
    if (query_block.aggregation || query_block.limitOrTopN)
        throw TiFlashException("Remote query containing agg or limit or topN is not supported", Errors::Coprocessor::BadRequest);
    const auto & ts = query_block.source->tbl_scan();
    std::vector<pingcap::coprocessor::KeyRange> cop_key_ranges;
    cop_key_ranges.reserve(ts.ranges_size());
    for (const auto & range : ts.ranges())
    {
        cop_key_ranges.emplace_back(range.low(), range.high());
    }
    sort(cop_key_ranges.begin(), cop_key_ranges.end());

    ::tipb::DAGRequest dag_req;

    copyExecutorTreeWithLocalTableScan(dag_req, query_block.root, dag.getDAGRequest());
    DAGSchema schema;
    ColumnsWithTypeAndName columns;
    BoolVec is_ts_column;
    std::vector<NameAndTypePair> source_columns;
    for (int i = 0; i < static_cast<int>(query_block.output_field_types.size()); i++)
    {
        dag_req.add_output_offsets(i);
        ColumnInfo info = TiDB::fieldTypeToColumnInfo(query_block.output_field_types[i]);
        String col_name = query_block.qb_column_prefix + "col_" + std::to_string(i);
        schema.push_back(std::make_pair(col_name, info));
        is_ts_column.push_back(query_block.output_field_types[i].tp() == TiDB::TypeTimestamp);
        source_columns.emplace_back(col_name, getDataTypeByFieldTypeForComputingLayer(query_block.output_field_types[i]));
    }

    dag_req.set_collect_execution_summaries(dag.getDAGContext().collect_execution_summaries);
    executeRemoteQueryImpl(pipeline, cop_key_ranges, dag_req, schema);

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
}

void TableScanInterpreter::executeRemoteQueryImpl(
    DAGPipeline & pipeline,
    const std::vector<pingcap::coprocessor::KeyRange> & cop_key_ranges,
    ::tipb::DAGRequest & dag_req,
    const DAGSchema & schema)
{
    pingcap::coprocessor::RequestPtr req = std::make_shared<pingcap::coprocessor::Request>();
    dag_req.SerializeToString(&(req->data));
    req->tp = pingcap::coprocessor::ReqType::DAG;
    req->start_ts = context.getSettingsRef().read_tso;
    bool has_enforce_encode_type = dag_req.has_force_encode_type() && dag_req.force_encode_type();

    pingcap::kv::Cluster * cluster = context.getTMTContext().getKVCluster();
    pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
    pingcap::kv::StoreType store_type = pingcap::kv::StoreType::TiFlash;
    auto all_tasks = pingcap::coprocessor::buildCopTasks(bo, cluster, cop_key_ranges, req, store_type, &Poco::Logger::get("pingcap/coprocessor"));

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
        BlockInputStreamPtr input = std::make_shared<CoprocessorBlockInputStream>(coprocessor_reader, log);
        pipeline.streams.push_back(input);
        is_remote_table_scan.push_back(true);
        dag.getDAGContext().getRemoteInputStreams().push_back(input);
        task_start = task_end;
    }
}

// To execute a query block, you have to:
// 1. generate the date stream and push it to pipeline.
// 2. assign the analyzer
// 3. construct a final projection, even if it's not necessary. just construct it.
// Talking about projection, it has following rules.
// 1. if the query block does not contain agg, then the final project is the same as the source Executor
// 2. if the query block contains agg, then the final project is the same as agg Executor
// 3. if the cop task may contains more then 1 query block, and the current query block is not the root
//    query block, then the project should add an alias for each column that needs to be projected, something
//    like final_project.emplace_back(col.name, query_block.qb_column_prefix + col.name);
void TableScanInterpreter::executeImpl(DAGPipelinePtr & pipeline)
{
    if (query_block.isRemoteQuery())
    {
        executeRemoteQuery(*pipeline);
        return;
    }
    executeTS(query_block.source->tbl_scan(), *pipeline);
    recordProfileStreams(dag.getDAGContext(), *pipeline, query_block.source_name, query_block.id);
    dag.getDAGContext().table_scan_executor_id = query_block.source_name;

    auto old_ts_schema = analyzer->getCurrentInputColumns();
    if (addExtraCastsAfterTs(*analyzer, need_add_cast_column_flag_for_tablescan, pipeline->chain, query_block))
    {
        auto projection_after_cast_for_remote_read = getProjectionAfterCastForRemoteRead(context, old_ts_schema, analyzer->getCurrentInputColumns());
        pipeline->chain.getLastStep().setCallback(
            "appendExtraCast",
            [&, projection = std::move(projection_after_cast_for_remote_read)](const ExpressionActionsPtr & extra_cast) {
                if (pipeline->streams.size() != is_remote_table_scan.size())
                    throw Exception("Size mismatch between streams and is_remote_table_scan!", ErrorCodes::LOGICAL_ERROR);
                for (size_t i = 0; i < pipeline->streams.size(); ++i)
                {
                    auto & stream = pipeline->streams[i];
                    if (is_remote_table_scan[i])
                        stream = std::make_shared<ExpressionBlockInputStream>(stream, projection, log);
                    else
                        stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, log);
                }
            });
        pipeline->chain.addStep();
    }

    if (!conditions.empty())
    {
        String filter_column_name = analyzer->appendWhere(pipeline->chain, conditions);
        pipeline->chain.getLastStep().setCallback(
            "appendWhere",
            [&, filter_column_name = std::move(filter_column_name)](const ExpressionActionsPtr & before_where) {
                if (pipeline->streams.size() != is_remote_table_scan.size())
                    throw Exception("Size mismatch between streams and is_remote_table_scan!", ErrorCodes::LOGICAL_ERROR);
                if (!pipeline->streams_with_non_joined_data.empty())
                    throw Exception("Should not have streams_with_non_joined_data after table scan!", ErrorCodes::LOGICAL_ERROR);
                for (size_t i = 0; i < pipeline->streams.size(); ++i)
                {
                    auto & stream = pipeline->streams[i];
                    if (!is_remote_table_scan[i])
                    {
                        stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, log);
                        recordProfileStream(dag.getDAGContext(), stream, query_block.selection_name, query_block.id);
                    }
                }
            });
        pipeline->chain.addStep();

        NamesWithAliases project_cols;
        for (const auto & col : analyzer->getCurrentInputColumns())
            project_cols.emplace_back(col.name, col.name);
        pipeline->chain.getLastActions()->add(ExpressionAction::project(project_cols));
        pipeline->chain.getLastStep().setCallback(
            "projectAfterWhere",
            [&](const ExpressionActionsPtr & project_after_where) {
                for (size_t i = 0; i < pipeline->streams.size(); ++i)
                {
                    auto & stream = pipeline->streams[i];
                    if (!is_remote_table_scan[i])
                        stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, log);
                }
            });
        pipeline->chain.addStep();
    }
}
} // namespace DB
