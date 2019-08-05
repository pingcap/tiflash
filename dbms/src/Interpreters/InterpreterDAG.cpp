#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/BlockIO.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/DAGExpressionAnalyzer.h>
#include <Interpreters/DAGUtils.h>
#include <Interpreters/InterpreterDAG.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/Region.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_MANY_COLUMNS;
}

InterpreterDAG::InterpreterDAG(Context & context_, DAGQuerySource & dag_query_src_) : context(context_), dag_query_src(dag_query_src_) {}

// the flow is the same as executeFetchcolumns
bool InterpreterDAG::executeTS(const tipb::TableScan & ts, Pipeline & pipeline)
{
    if (!ts.has_table_id())
    {
        // do not have table id
        return false;
    }
    TableID id = ts.table_id();
    auto & tmt_ctx = context.getTMTContext();
    auto storage = tmt_ctx.getStorages().get(id);
    if (storage == nullptr)
    {
        tmt_ctx.getSchemaSyncer()->syncSchema(id, context, false);
        storage = tmt_ctx.getStorages().get(id);
    }
    if (storage == nullptr)
    {
        return false;
    }
    auto table_lock = storage->lockStructure(false, __PRETTY_FUNCTION__);
    const auto * merge_tree = dynamic_cast<const StorageMergeTree *>(storage.get());
    if (!merge_tree)
    {
        return false;
    }

    Names required_columns;
    for (const tipb::ColumnInfo & ci : ts.columns())
    {
        ColumnID cid = ci.column_id();
        if (cid < 1 || cid > (Int64)merge_tree->getTableInfo().columns.size())
        {
            // cid out of bound
            return false;
        }
        String name = merge_tree->getTableInfo().columns[cid - 1].name;
        required_columns.push_back(name);
    }
    if (required_columns.empty())
    {
        // no column selected, must be something wrong
        return false;
    }

    if (!dag_query_src.hasAggregation())
    {
        // if the dag request does not contain agg, then the final output is
        // based on the output of table scan
        for (auto i : dag_query_src.getDAGRequest().output_offsets())
        {
            if (i < 0 || i >= required_columns.size())
            {
                // array index out of bound
                return false;
            }
            // do not have alias
            final_project.emplace_back(required_columns[i], "");
        }
    }
    // todo handle alias column
    const Settings & settings = context.getSettingsRef();

    if (settings.max_columns_to_read && required_columns.size() > settings.max_columns_to_read)
    {
        throw Exception("Limit for number of columns to read exceeded. "
                        "Requested: "
                + toString(required_columns.size()) + ", maximum: " + settings.max_columns_to_read.toString(),
            ErrorCodes::TOO_MANY_COLUMNS);
    }

    size_t max_block_size = settings.max_block_size;
    max_streams = settings.max_threads;
    QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
    if (max_streams > 1)
    {
        max_streams *= settings.max_streams_to_max_threads_ratio;
    }

    //todo support index in
    SelectQueryInfo query_info;
    query_info.query = std::make_unique<ASTSelectQuery>();
    query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>();
    query_info.mvcc_query_info->resolve_locks = true;
    query_info.mvcc_query_info->read_tso = settings.read_tso;
    RegionQueryInfo info;
    info.region_id = dag_query_src.getRegionID();
    info.version = dag_query_src.getRegionVersion();
    info.conf_version = dag_query_src.getRegionConfVersion();
    auto current_region = context.getTMTContext().getRegionTable().getRegionById(id, info.region_id);
    if (!current_region)
    {
        return false;
    }
    info.range_in_table = current_region->getHandleRangeByTable(id);
    query_info.mvcc_query_info->regions_query_info.push_back(info);
    query_info.mvcc_query_info->concurrent = 0.0;
    pipeline.streams = storage->read(required_columns, query_info, context, from_stage, max_block_size, max_streams);
    /// Set the limits and quota for reading data, the speed and time of the query.
    {
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
    ColumnsWithTypeAndName columnsWithTypeAndName = pipeline.firstStream()->getHeader().getColumnsWithTypeAndName();
    source_columns = storage->getColumns().getAllPhysical();
    return true;
}

InterpreterDAG::AnalysisResult InterpreterDAG::analyzeExpressions()
{
    AnalysisResult res;
    ExpressionActionsChain chain;
    res.need_aggregate = dag_query_src.hasAggregation();
    DAGExpressionAnalyzer expressionAnalyzer(source_columns, context);
    if (dag_query_src.hasSelection())
    {
        if (expressionAnalyzer.appendWhere(chain, dag_query_src.getSelection(), res.filter_column_name))
        {
            res.has_where = true;
            res.before_where = chain.getLastActions();
            res.filter_column_name = chain.steps.back().required_output[0];
            chain.addStep();
        }
    }
    if (res.need_aggregate)
    {
        res.need_aggregate
            = expressionAnalyzer.appendAggregation(chain, dag_query_src.getAggregation(), res.aggregation_keys, res.aggregate_descriptions);
        res.before_aggregation = chain.getLastActions();

        chain.finalize();
        chain.clear();

        // add cast if type is not match
        expressionAnalyzer.appendAggSelect(chain, dag_query_src.getAggregation());
        //todo use output_offset to pruner the final project columns
        for (auto element : expressionAnalyzer.getCurrentInputColumns())
        {
            final_project.emplace_back(element.name, "");
        }
    }
    if (dag_query_src.hasTopN())
    {
        res.has_order_by = expressionAnalyzer.appendOrderBy(chain, dag_query_src.getTopN(), res.order_column_names);
    }
    // append final project results
    for (auto & name : final_project)
    {
        chain.steps.back().required_output.push_back(name.first);
    }
    res.before_order_and_select = chain.getLastActions();
    chain.finalize();
    chain.clear();
    //todo need call prependProjectInput??
    return res;
}

void InterpreterDAG::executeWhere(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, String & filter_column)
{
    pipeline.transform(
        [&](auto & stream) { stream = std::make_shared<FilterBlockInputStream>(stream, expressionActionsPtr, filter_column); });
}

void InterpreterDAG::executeAggregation(
    Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, Names & key_names, AggregateDescriptions & aggregates)
{
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expressionActionsPtr); });

    Block header = pipeline.firstStream()->getHeader();
    ColumnNumbers keys;
    for (const auto & name : key_names)
    {
        keys.push_back(header.getPositionByName(name));
    }
    for (auto & descr : aggregates)
    {
        if (descr.arguments.empty())
        {
            for (const auto & name : descr.argument_names)
            {
                descr.arguments.push_back(header.getPositionByName(name));
            }
        }
    }

    const Settings & settings = context.getSettingsRef();

    /** Two-level aggregation is useful in two cases:
      * 1. Parallel aggregation is done, and the results should be merged in parallel.
      * 2. An aggregation is done with store of temporary data on the disk, and they need to be merged in a memory efficient way.
      */
    bool allow_to_use_two_level_group_by = pipeline.streams.size() > 1 || settings.max_bytes_before_external_group_by != 0;

    Aggregator::Params params(header, keys, aggregates, false, settings.max_rows_to_group_by, settings.group_by_overflow_mode,
        settings.compile ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
        settings.max_bytes_before_external_group_by, settings.empty_result_for_aggregation_by_empty_set, context.getTemporaryPath());

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.streams.size() > 1)
    {
        pipeline.firstStream() = std::make_shared<ParallelAggregatingBlockInputStream>(pipeline.streams, nullptr, params, true, max_streams,
            settings.aggregation_memory_efficient_merge_threads ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                                                                : static_cast<size_t>(settings.max_threads));

        pipeline.streams.resize(1);
    }
    else
    {
        BlockInputStreams inputs;
        if (!pipeline.streams.empty())
            inputs.push_back(pipeline.firstStream());
        else
            pipeline.streams.resize(1);

        pipeline.firstStream()
            = std::make_shared<AggregatingBlockInputStream>(std::make_shared<ConcatBlockInputStream>(inputs), params, true);
    }
    // add cast
}

void InterpreterDAG::executeExpression(Pipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr)
{
    if (!expressionActionsPtr->getActions().empty())
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expressionActionsPtr); });
    }
}

SortDescription InterpreterDAG::getSortDescription(Strings & order_column_names)
{
    // construct SortDescription
    SortDescription order_descr;
    const tipb::TopN & topN = dag_query_src.getTopN();
    order_descr.reserve(topN.order_by_size());
    for (int i = 0; i < topN.order_by_size(); i++)
    {
        String name = order_column_names[i];
        int direction = topN.order_by(i).desc() ? -1 : 1;
        // todo get this information from DAGRequest
        // currently use NULLS LAST
        int nulls_direction = direction;
        // todo get this information from DAGRequest
        // currently use the defalut value
        std::shared_ptr<Collator> collator;

        order_descr.emplace_back(name, direction, nulls_direction, collator);
    }
    return order_descr;
}

void InterpreterDAG::executeUnion(Pipeline & pipeline)
{
    if (pipeline.hasMoreThanOneStream())
    {
        pipeline.firstStream() = std::make_shared<UnionBlockInputStream<>>(pipeline.streams, nullptr, max_streams);
        pipeline.streams.resize(1);
    }
}

void InterpreterDAG::executeOrder(Pipeline & pipeline, Strings & order_column_names)
{
    SortDescription order_descr = getSortDescription(order_column_names);
    const Settings & settings = context.getSettingsRef();
    Int64 limit = dag_query_src.getTopN().limit();

    pipeline.transform([&](auto & stream) {
        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, limit);

        /// Limits on sorting
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
    });

    /// If there are several streams, we merge them into one
    executeUnion(pipeline);

    /// Merge the sorted blocks.
    pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(pipeline.firstStream(), order_descr, settings.max_block_size,
        limit, settings.max_bytes_before_external_sort, context.getTemporaryPath());
}

//todo return the error message
bool InterpreterDAG::executeImpl(Pipeline & pipeline)
{
    if (!executeTS(dag_query_src.getTS(), pipeline))
    {
        return false;
    }

    auto res = analyzeExpressions();
    // execute selection
    if (res.has_where)
    {
        executeWhere(pipeline, res.before_where, res.filter_column_name);
    }
    if (res.need_aggregate)
    {
        // execute aggregation
        executeAggregation(pipeline, res.before_aggregation, res.aggregation_keys, res.aggregate_descriptions);
    }
    executeExpression(pipeline, res.before_order_and_select);

    if (res.has_order_by)
    {
        // execute topN
        executeOrder(pipeline, res.order_column_names);
    }

    // execute projection
    executeFinalProject(pipeline);

    // execute limit
    if (dag_query_src.hasLimit() && !dag_query_src.hasTopN())
    {
        executeLimit(pipeline);
    }
    return true;
}

void InterpreterDAG::executeFinalProject(Pipeline & pipeline)
{
    auto columns = pipeline.firstStream()->getHeader();
    NamesAndTypesList input_column;
    for (auto column : columns.getColumnsWithTypeAndName())
    {
        input_column.emplace_back(column.name, column.type);
    }
    ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column, context.getSettingsRef());
    project->add(ExpressionAction::project(final_project));
    // add final project
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project); });
}

void InterpreterDAG::executeLimit(Pipeline & pipeline)
{
    pipeline.transform(
        [&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, dag_query_src.getLimit().limit(), 0, false); });
    if (pipeline.hasMoreThanOneStream())
    {
        executeUnion(pipeline);
        pipeline.transform(
            [&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, dag_query_src.getLimit().limit(), 0, false); });
    }
}

BlockIO InterpreterDAG::execute()
{
    Pipeline pipeline;
    executeImpl(pipeline);
    executeUnion(pipeline);

    BlockIO res;
    res.in = pipeline.firstStream();
    return res;
}
} // namespace DB
