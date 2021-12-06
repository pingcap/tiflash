#include <Common/FailPoint.h>
#include <Common/TiFlashException.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/DAGCodec.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGInterpreterBase.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Join.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{
namespace FailPoints
{
extern const char pause_after_copr_streams_acquired[];
extern const char minimum_block_size_for_cross_join[];
} // namespace FailPoints

DAGInterpreterBase::DAGInterpreterBase(
    Context & context_,
    const DAGQueryBlock & query_block_,
    size_t max_streams_,
    bool keep_session_timezone_info_,
    const DAGQuerySource & dag_,
    const LogWithPrefixPtr & log_)
    : context(context_)
    , query_block(query_block_)
    , keep_session_timezone_info(keep_session_timezone_info_)
    , max_streams(max_streams_)
    , dag(dag_)
    , log(log_)
{
    if (query_block.selection != nullptr)
    {
        for (const auto & condition : query_block.selection->selection().conditions())
            conditions.push_back(&condition);
    }
}

void DAGInterpreterBase::executeWhere(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & expr,
    const String & filter_column)
{
    pipeline.transform([&](auto & stream) { stream = std::make_shared<FilterBlockInputStream>(stream, expr, filter_column, log); });
}

void DAGInterpreterBase::executeAggregation(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & expression_actions_ptr,
    Names & key_names,
    TiDB::TiDBCollators & collators,
    AggregateDescriptions & aggregate_descriptions)
{
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expression_actions_ptr, log); });

    Block header = pipeline.firstStream()->getHeader();
    ColumnNumbers keys;
    for (const auto & name : key_names)
    {
        keys.push_back(header.getPositionByName(name));
    }
    for (auto & descr : aggregate_descriptions)
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
    bool has_collator = std::any_of(begin(collators), end(collators), [](const auto & p) { return p != nullptr; });

    Aggregator::Params params(
        header,
        keys,
        aggregate_descriptions,
        false,
        settings.max_rows_to_group_by,
        settings.group_by_overflow_mode,
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
        allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
        settings.max_bytes_before_external_group_by,
        settings.empty_result_for_aggregation_by_empty_set,
        context.getTemporaryPath(),
        has_collator ? collators : TiDB::dummy_collators);

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.streams.size() > 1)
    {
        before_agg_streams = pipeline.streams.size();
        BlockInputStreamPtr stream_with_non_joined_data = combinedNonJoinedDataStream(pipeline, max_streams, log);
        pipeline.firstStream() = std::make_shared<ParallelAggregatingBlockInputStream>(
            pipeline.streams,
            stream_with_non_joined_data,
            params,
            context.getFileProvider(),
            true,
            max_streams,
            settings.aggregation_memory_efficient_merge_threads ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads) : static_cast<size_t>(settings.max_threads),
            log);
        pipeline.streams.resize(1);
    }
    else
    {
        BlockInputStreamPtr stream_with_non_joined_data = combinedNonJoinedDataStream(pipeline, max_streams, log);
        BlockInputStreams inputs;
        if (!pipeline.streams.empty())
            inputs.push_back(pipeline.firstStream());
        else
            pipeline.streams.resize(1);
        if (stream_with_non_joined_data)
            inputs.push_back(stream_with_non_joined_data);
        pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(
            std::make_shared<ConcatBlockInputStream>(inputs, log),
            params,
            context.getFileProvider(),
            true,
            log);
    }
    // add cast
}

void DAGInterpreterBase::executeExpression(DAGPipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr)
{
    if (!expressionActionsPtr->getActions().empty())
    {
        pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, expressionActionsPtr, log); });
    }
}

void DAGInterpreterBase::executeOrder(DAGPipeline & pipeline, const std::vector<NameAndTypePair> & order_columns)
{
    SortDescription order_descr = getSortDescription(order_columns, query_block.limitOrTopN->topn().order_by());
    const Settings & settings = context.getSettingsRef();
    Int64 limit = query_block.limitOrTopN->topn().limit();

    pipeline.transform([&](auto & stream) {
        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, log, limit);

        /// Limits on sorting
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
    });

    /// If there are several streams, we merge them into one
    executeUnion(pipeline, max_streams, log);

    /// Merge the sorted blocks.
    pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(
        pipeline.firstStream(),
        order_descr,
        settings.max_block_size,
        limit,
        settings.max_bytes_before_external_sort,
        context.getTemporaryPath(),
        log);
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
void DAGInterpreterBase::executeNonSourceExecutors(DAGPipeline & pipeline)
{
    DAGExpressionActionsChain & chain = pipeline.chain;
    // this log measures the concurrent degree in this mpp task
    LOG_INFO(
        log,
        fmt::format("execution stream size for query block(before aggregation){} is {}", query_block.qb_column_prefix, pipeline.streams.size()));

    dag.getDAGContext().final_concurrency = std::max(dag.getDAGContext().final_concurrency, pipeline.streams.size());

    /// Selection is handled for TableScan.
    if (!conditions.empty() && query_block.source->tp() != tipb::ExecType::TypeTableScan)
    {
        String filter_column_name = analyzer->appendWhere(chain, conditions);
        chain.getLastStep().setCallback(
            "appendWhere",
            [&, filter_column_name = std::move(filter_column_name)](const ExpressionActionsPtr & before_where) {
                pipeline.transform([&](auto & stream) {
                    stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, log);
                });
                recordProfileStreams(dag.getDAGContext(), pipeline, query_block.selection_name, query_block.id);
            });
        chain.addStep();
    }

    if (query_block.aggregation)
    {
        bool group_by_collation_sensitive =
            /// collation sensitive group by is slower then normal group by, use normal group by by default
            context.getSettingsRef().group_by_collation_sensitive ||
            /// in mpp task, here is no way to tell whether this aggregation is first stage aggregation or
            /// final stage aggregation, to make sure the result is right, always do collation sensitive aggregation
            context.getDAGContext()->isMPPTask();

        auto [aggregation_keys, aggregation_collators, aggregate_descriptions] = analyzer->appendAggregation(
            chain,
            query_block.aggregation->aggregation(),
            group_by_collation_sensitive);

        chain.getLastStep().setCallback(
            "beforeAggregation",
            [&,
             aggregation_keys = std::move(aggregation_keys),
             aggregation_collators = std::move(aggregation_collators),
             aggregate_descriptions = std::move(aggregate_descriptions)](const ExpressionActionsPtr & before_aggregation) mutable {
                executeAggregation(
                    pipeline,
                    before_aggregation,
                    aggregation_keys,
                    aggregation_collators,
                    aggregate_descriptions);
                recordProfileStreams(dag.getDAGContext(), pipeline, query_block.aggregation_name, query_block.id);
            });

        chain.finalize();
        chain.clear();

        // add cast if type is not match
        analyzer->appendAggSelect(chain, query_block.aggregation->aggregation());
        if (query_block.having != nullptr)
        {
            std::vector<const tipb::Expr *> having_conditions;
            for (const auto & c : query_block.having->selection().conditions())
                having_conditions.push_back(&c);
            auto having_column_name = analyzer->appendWhere(chain, having_conditions);
            chain.getLastStep().setCallback(
                "beforeHaving",
                [&, having_column_name = std::move(having_column_name)](const ExpressionActionsPtr & before_having) {
                    executeWhere(pipeline, before_having, having_column_name);
                    recordProfileStreams(dag.getDAGContext(), pipeline, query_block.having_name, query_block.id);
                });
            chain.addStep();
        }
    }

    if (query_block.limitOrTopN)
    {
        // Or TopN, not both.
        if (query_block.limitOrTopN->tp() == tipb::ExecType::TypeTopN)
        {
            auto order_columns = analyzer->appendOrderBy(chain, query_block.limitOrTopN->topn());
            chain.getLastStep().setCallback(
                "beforeOrder",
                [&, order_columns = std::move(order_columns)](const ExpressionActionsPtr & before_order) {
                    executeExpression(pipeline, before_order);
                    executeOrder(pipeline, order_columns);
                    recordProfileStreams(dag.getDAGContext(), pipeline, query_block.limitOrTopN_name, query_block.id);
                });
            chain.addStep();
        }
        else if (query_block.limitOrTopN->tp() == tipb::TypeLimit)
        {
            chain.getLastStep().setCallback(
                "beforeLimit",
                [&](const ExpressionActionsPtr & before_limit) {
                    executeExpression(pipeline, before_limit);
                    executeLimit(pipeline);
                    recordProfileStreams(dag.getDAGContext(), pipeline, query_block.limitOrTopN_name, query_block.id);
                });
            chain.addStep();
        }
        else
        {
            // TODO
        }
    }

    // Append final project results if needed.
    NamesWithAliases final_project = query_block.isRootQueryBlock()
        ? analyzer->appendFinalProjectForRootQueryBlock(
            chain,
            query_block.output_field_types,
            query_block.output_offsets,
            query_block.qb_column_prefix,
            keep_session_timezone_info)
        : analyzer->appendFinalProjectForNonRootQueryBlock(
            chain,
            query_block.qb_column_prefix);

    chain.getLastStep().setCallback(
        "beforeFinalProject",
        [&, final_project = std::move(final_project)](const ExpressionActionsPtr & before_final_project) {
            executeExpression(pipeline, before_final_project);
            executeProject(pipeline, final_project);
        });

    chain.finalize();
    chain.clear();
}

void DAGInterpreterBase::executeProject(DAGPipeline & pipeline, const NamesWithAliases & project_cols)
{
    if (project_cols.empty())
        return;
    ExpressionActionsPtr project = generateProjectExpressionActions(pipeline.firstStream()->getHeader(), context, project_cols);
    pipeline.transform([&](auto & stream) { stream = std::make_shared<ExpressionBlockInputStream>(stream, project, log); });
}

void DAGInterpreterBase::executeLimit(DAGPipeline & pipeline)
{
    size_t limit = 0;
    if (query_block.limitOrTopN->tp() == tipb::TypeLimit)
        limit = query_block.limitOrTopN->limit().limit();
    else
        limit = query_block.limitOrTopN->topn().limit();
    pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, log, false); });
    if (pipeline.hasMoreThanOneStream())
    {
        executeUnion(pipeline, max_streams, log);
        pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, log, false); });
    }
}

DAGPipelinePtr DAGInterpreterBase::execute()
{
    DAGPipelinePtr pipeline = std::make_shared<DAGPipeline>();
    executeImpl(pipeline);
    executeNonSourceExecutors(*pipeline);
    if (!pipeline->streams_with_non_joined_data.empty())
    {
        size_t concurrency = pipeline->streams.size();
        executeUnion(*pipeline, max_streams, log);
        if (!query_block.isRootQueryBlock())
            restoreConcurrency(*pipeline, concurrency, log);
    }

    /// expand concurrency after agg
    if (!query_block.isRootQueryBlock())
        restoreConcurrency(*pipeline, before_agg_streams, log);

    return pipeline;
}
} // namespace DB
