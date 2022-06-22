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
#include <Common/TiFlashException.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/ConcatBlockInputStream.h>
#include <DataStreams/ExchangeSenderBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/MockExchangeReceiverInputStream.h>
#include <DataStreams/MockExchangeSenderInputStream.h>
#include <DataStreams/MockTableScanBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataStreams/WindowBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/ExchangeSenderInterpreterHelper.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Flash/Coprocessor/MockSourceStream.h>
#include <Flash/Coprocessor/PushDownFilter.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Join.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{
namespace FailPoints
{
extern const char minimum_block_size_for_cross_join[];
} // namespace FailPoints

DAGQueryBlockInterpreter::DAGQueryBlockInterpreter(
    Context & context_,
    const std::vector<BlockInputStreams> & input_streams_vec_,
    const DAGQueryBlock & query_block_,
    size_t max_streams_)
    : context(context_)
    , input_streams_vec(input_streams_vec_)
    , query_block(query_block_)
    , max_streams(max_streams_)
    , log(Logger::get("DAGQueryBlockInterpreter", dagContext().log ? dagContext().log->identifier() : ""))
{}

namespace
{
struct AnalysisResult
{
    ExpressionActionsPtr before_where;
    ExpressionActionsPtr before_aggregation;
    ExpressionActionsPtr before_having;
    ExpressionActionsPtr before_order_and_select;
    ExpressionActionsPtr final_projection;

    String filter_column_name;
    String having_column_name;
    NamesAndTypes order_columns;

    Names aggregation_keys;
    TiDB::TiDBCollators aggregation_collators;
    AggregateDescriptions aggregate_descriptions;
    bool is_final_agg = false;
};

AnalysisResult analyzeExpressions(
    Context & context,
    DAGExpressionAnalyzer & analyzer,
    const DAGQueryBlock & query_block,
    NamesWithAliases & final_project)
{
    AnalysisResult res;
    ExpressionActionsChain chain;
    // selection on table scan had been executed in handleTableScan
    // In test mode, filter is not pushed down to table scan
    if (query_block.selection && (!query_block.isTableScanSource() || context.getDAGContext()->isTest()))
    {
        std::vector<const tipb::Expr *> where_conditions;
        for (const auto & c : query_block.selection->selection().conditions())
            where_conditions.push_back(&c);
        res.filter_column_name = analyzer.appendWhere(chain, where_conditions);
        res.before_where = chain.getLastActions();
        chain.addStep();
    }
    // There will be either Agg...
    if (query_block.aggregation)
    {
        res.is_final_agg = AggregationInterpreterHelper::isFinalAgg(query_block.aggregation->aggregation());

        std::tie(res.aggregation_keys, res.aggregation_collators, res.aggregate_descriptions, res.before_aggregation) = analyzer.appendAggregation(
            chain,
            query_block.aggregation->aggregation(),
            AggregationInterpreterHelper::isGroupByCollationSensitive(context));

        if (query_block.having != nullptr)
        {
            std::vector<const tipb::Expr *> having_conditions;
            for (const auto & c : query_block.having->selection().conditions())
                having_conditions.push_back(&c);
            res.having_column_name = analyzer.appendWhere(chain, having_conditions);
            res.before_having = chain.getLastActions();
            chain.addStep();
        }
    }
    // Or TopN, not both.
    if (query_block.limit_or_topn && query_block.limit_or_topn->tp() == tipb::ExecType::TypeTopN)
    {
        res.order_columns = analyzer.appendOrderBy(chain, query_block.limit_or_topn->topn());
    }

    const auto & dag_context = *context.getDAGContext();
    // Append final project results if needed.
    final_project = query_block.isRootQueryBlock()
        ? analyzer.appendFinalProjectForRootQueryBlock(
            chain,
            dag_context.output_field_types,
            dag_context.output_offsets,
            query_block.qb_column_prefix,
            dag_context.keep_session_timezone_info)
        : analyzer.appendFinalProjectForNonRootQueryBlock(
            chain,
            query_block.qb_column_prefix);

    res.before_order_and_select = chain.getLastActions();

    chain.finalize();
    chain.clear();
    //todo need call prependProjectInput??
    return res;
}
} // namespace

// for tests, we need to mock tableScan blockInputStream as the source stream.
void DAGQueryBlockInterpreter::handleMockTableScan(const TiDBTableScan & table_scan, DAGPipeline & pipeline)
{
    if (context.getDAGContext()->columnsForTestEmpty() || context.getDAGContext()->columnsForTest(table_scan.getTableScanExecutorID()).empty())
    {
        auto names_and_types = genNamesAndTypes(table_scan);
        auto columns_with_type_and_name = getColumnWithTypeAndName(names_and_types);
        analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(names_and_types), context);
        for (size_t i = 0; i < max_streams; ++i)
        {
            auto mock_table_scan_stream = std::make_shared<MockTableScanBlockInputStream>(columns_with_type_and_name, context.getSettingsRef().max_block_size);
            pipeline.streams.emplace_back(mock_table_scan_stream);
        }
    }
    else
    {
        auto [names_and_types, mock_table_scan_streams] = mockSourceStream<MockTableScanBlockInputStream>(context, max_streams, log, table_scan.getTableScanExecutorID());
        analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(names_and_types), context);
        pipeline.streams.insert(pipeline.streams.end(), mock_table_scan_streams.begin(), mock_table_scan_streams.end());
    }
}

void DAGQueryBlockInterpreter::handleTableScan(const TiDBTableScan & table_scan, DAGPipeline & pipeline)
{
    const auto push_down_filter = PushDownFilter::toPushDownFilter(query_block.selection_name, query_block.selection);

    DAGStorageInterpreter storage_interpreter(context, table_scan, push_down_filter, max_streams);
    storage_interpreter.execute(pipeline);

    analyzer = std::move(storage_interpreter.analyzer);
}

void DAGQueryBlockInterpreter::handleJoin(const tipb::Join & join, DAGPipeline & pipeline, SubqueryForSet & right_query)
{
    if (unlikely(input_streams_vec.size() != 2))
    {
        throw TiFlashException("Join query block must have 2 input streams", Errors::BroadcastJoin::Internal);
    }

    JoinInterpreterHelper::TiFlashJoin tiflash_join{join};

    DAGPipeline probe_pipeline;
    DAGPipeline build_pipeline;
    probe_pipeline.streams = input_streams_vec[1 - tiflash_join.build_side_index];
    build_pipeline.streams = input_streams_vec[tiflash_join.build_side_index];

    RUNTIME_ASSERT(!input_streams_vec[0].empty(), log, "left input streams cannot be empty");
    const Block & left_input_header = input_streams_vec[0].back()->getHeader();

    RUNTIME_ASSERT(!input_streams_vec[1].empty(), log, "right input streams cannot be empty");
    const Block & right_input_header = input_streams_vec[1].back()->getHeader();

    String match_helper_name = tiflash_join.genMatchHelperName(left_input_header, right_input_header);
    NamesAndTypesList columns_added_by_join = tiflash_join.genColumnsAddedByJoin(build_pipeline.firstStream()->getHeader(), match_helper_name);
    NamesAndTypes join_output_columns = tiflash_join.genJoinOutputColumns(left_input_header, right_input_header, match_helper_name);

    /// add necessary transformation if the join key is an expression

    bool is_tiflash_right_join = tiflash_join.isTiFlashRightJoin();

    // prepare probe side
    auto [probe_side_prepare_actions, probe_key_names, probe_filter_column_name] = JoinInterpreterHelper::prepareJoin(
        context,
        probe_pipeline.firstStream()->getHeader(),
        tiflash_join.getProbeJoinKeys(),
        tiflash_join.join_key_types,
        true,
        is_tiflash_right_join,
        tiflash_join.getProbeConditions());
    RUNTIME_ASSERT(probe_side_prepare_actions, log, "probe_side_prepare_actions cannot be nullptr");

    // prepare build side
    auto [build_side_prepare_actions, build_key_names, build_filter_column_name] = JoinInterpreterHelper::prepareJoin(
        context,
        build_pipeline.firstStream()->getHeader(),
        tiflash_join.getBuildJoinKeys(),
        tiflash_join.join_key_types,
        false,
        is_tiflash_right_join,
        tiflash_join.getBuildConditions());
    RUNTIME_ASSERT(build_side_prepare_actions, log, "build_side_prepare_actions cannot be nullptr");

    auto [other_condition_expr, other_filter_column_name, other_eq_filter_from_in_column_name]
        = tiflash_join.genJoinOtherConditionAction(context, left_input_header, right_input_header, probe_side_prepare_actions);

    const Settings & settings = context.getSettingsRef();
    size_t max_block_size_for_cross_join = settings.max_block_size;
    fiu_do_on(FailPoints::minimum_block_size_for_cross_join, { max_block_size_for_cross_join = 1; });

    JoinPtr join_ptr = std::make_shared<Join>(
        probe_key_names,
        build_key_names,
        true,
        SizeLimits(settings.max_rows_in_join, settings.max_bytes_in_join, settings.join_overflow_mode),
        tiflash_join.kind,
        tiflash_join.strictness,
        log->identifier(),
        tiflash_join.join_key_collators,
        probe_filter_column_name,
        build_filter_column_name,
        other_filter_column_name,
        other_eq_filter_from_in_column_name,
        other_condition_expr,
        max_block_size_for_cross_join,
        match_helper_name);

    recordJoinExecuteInfo(tiflash_join.build_side_index, join_ptr);

    size_t join_build_concurrency = settings.join_concurrent_build ? std::min(max_streams, build_pipeline.streams.size()) : 1;

    /// build side streams
    executeExpression(build_pipeline, build_side_prepare_actions, "append join key and join filters for build side");
    // add a HashJoinBuildBlockInputStream to build a shared hash table
    auto get_concurrency_build_index = JoinInterpreterHelper::concurrencyBuildIndexGenerator(join_build_concurrency);
    build_pipeline.transform([&](auto & stream) {
        stream = std::make_shared<HashJoinBuildBlockInputStream>(stream, join_ptr, get_concurrency_build_index(), log->identifier());
        stream->setExtraInfo(
            fmt::format("join build, build_side_root_executor_id = {}", dagContext().getJoinExecuteInfoMap()[query_block.source_name].build_side_root_executor_id));
    });
    // for test, join executor need the return blocks to output.
    executeUnion(build_pipeline, max_streams, log, /*ignore_block=*/!dagContext().isTest(), "for join");

    right_query.source = build_pipeline.firstStream();
    right_query.join = join_ptr;
    join_ptr->init(right_query.source->getHeader(), join_build_concurrency);

    /// probe side streams
    executeExpression(probe_pipeline, probe_side_prepare_actions, "append join key and join filters for probe side");
    NamesAndTypes source_columns;
    for (const auto & p : probe_pipeline.firstStream()->getHeader())
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    ExpressionActionsChain chain;
    dag_analyzer.appendJoin(chain, right_query, columns_added_by_join);
    pipeline.streams = probe_pipeline.streams;
    /// add join input stream
    if (is_tiflash_right_join)
    {
        auto & join_execute_info = dagContext().getJoinExecuteInfoMap()[query_block.source_name];
        size_t not_joined_concurrency = join_ptr->getNotJoinedStreamConcurrency();
        for (size_t i = 0; i < not_joined_concurrency; ++i)
        {
            auto non_joined_stream = join_ptr->createStreamWithNonJoinedRows(
                pipeline.firstStream()->getHeader(),
                i,
                not_joined_concurrency,
                settings.max_block_size);
            non_joined_stream->setExtraInfo("add stream with non_joined_data if full_or_right_join");
            pipeline.streams_with_non_joined_data.push_back(non_joined_stream);
            join_execute_info.non_joined_streams.push_back(non_joined_stream);
        }
    }
    for (auto & stream : pipeline.streams)
    {
        stream = std::make_shared<HashJoinProbeBlockInputStream>(stream, chain.getLastActions(), log->identifier());
        stream->setExtraInfo(fmt::format("join probe, join_executor_id = {}", query_block.source_name));
    }

    /// add a project to remove all the useless column
    NamesWithAliases project_cols;
    for (auto & c : join_output_columns)
    {
        /// do not need to care about duplicated column names because
        /// it is guaranteed by its children query block
        project_cols.emplace_back(c.name, c.name);
    }
    executeProject(pipeline, project_cols, "remove useless column after join");
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(join_output_columns), context);
}

void DAGQueryBlockInterpreter::recordJoinExecuteInfo(size_t build_side_index, const JoinPtr & join_ptr)
{
    const auto * build_side_root_executor = query_block.children[build_side_index]->root;
    JoinExecuteInfo join_execute_info;
    join_execute_info.build_side_root_executor_id = build_side_root_executor->executor_id();
    join_execute_info.join_ptr = join_ptr;
    assert(join_execute_info.join_ptr);
    dagContext().getJoinExecuteInfoMap()[query_block.source_name] = std::move(join_execute_info);
}

void DAGQueryBlockInterpreter::executeWhere(DAGPipeline & pipeline, const ExpressionActionsPtr & expr, String & filter_column, const String & extra_info)
{
    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<FilterBlockInputStream>(stream, expr, filter_column, log->identifier());
        stream->setExtraInfo(extra_info);
    });
}

void DAGQueryBlockInterpreter::executeWindow(
    DAGPipeline & pipeline,
    WindowDescription & window_description)
{
    executeExpression(pipeline, window_description.before_window, "before window");

    /// If there are several streams, we merge them into one
    executeUnion(pipeline, max_streams, log, false, "merge into one for window input");
    assert(pipeline.streams.size() == 1);
    pipeline.firstStream() = std::make_shared<WindowBlockInputStream>(pipeline.firstStream(), window_description, log->identifier());
}

void DAGQueryBlockInterpreter::executeAggregation(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & expression_actions_ptr,
    const Names & key_names,
    const TiDB::TiDBCollators & collators,
    AggregateDescriptions & aggregate_descriptions,
    bool is_final_agg)
{
    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, expression_actions_ptr, log->identifier());
        stream->setExtraInfo("before aggregation");
    });

    Block before_agg_header = pipeline.firstStream()->getHeader();

    AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
    auto params = AggregationInterpreterHelper::buildParams(
        context,
        before_agg_header,
        pipeline.streams.size(),
        key_names,
        collators,
        aggregate_descriptions,
        is_final_agg);

    /// If there are several sources, then we perform parallel aggregation
    if (pipeline.streams.size() > 1)
    {
        const Settings & settings = context.getSettingsRef();
        BlockInputStreamPtr stream_with_non_joined_data = combinedNonJoinedDataStream(pipeline, max_streams, log);
        pipeline.firstStream() = std::make_shared<ParallelAggregatingBlockInputStream>(
            pipeline.streams,
            stream_with_non_joined_data,
            params,
            context.getFileProvider(),
            true,
            max_streams,
            settings.aggregation_memory_efficient_merge_threads ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads) : static_cast<size_t>(settings.max_threads),
            log->identifier());
        pipeline.streams.resize(1);
        // should record for agg before restore concurrency. See #3804.
        recordProfileStreams(pipeline, query_block.aggregation_name);
        restorePipelineConcurrency(pipeline);
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
            std::make_shared<ConcatBlockInputStream>(inputs, log->identifier()),
            params,
            context.getFileProvider(),
            true,
            log->identifier());
        recordProfileStreams(pipeline, query_block.aggregation_name);
    }
}

void DAGQueryBlockInterpreter::executeExpression(DAGPipeline & pipeline, const ExpressionActionsPtr & expressionActionsPtr, const String & extra_info)
{
    if (!expressionActionsPtr->getActions().empty())
    {
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<ExpressionBlockInputStream>(stream, expressionActionsPtr, log->identifier());
            stream->setExtraInfo(extra_info);
        });
    }
}

void DAGQueryBlockInterpreter::executeWindowOrder(DAGPipeline & pipeline, SortDescription sort_desc)
{
    orderStreams(pipeline, sort_desc, 0);
}

void DAGQueryBlockInterpreter::executeOrder(DAGPipeline & pipeline, const NamesAndTypes & order_columns)
{
    Int64 limit = query_block.limit_or_topn->topn().limit();
    orderStreams(pipeline, getSortDescription(order_columns, query_block.limit_or_topn->topn().order_by()), limit);
}

void DAGQueryBlockInterpreter::orderStreams(DAGPipeline & pipeline, SortDescription order_descr, Int64 limit)
{
    const Settings & settings = context.getSettingsRef();

    pipeline.transform([&](auto & stream) {
        auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, log->identifier(), limit);

        /// Limits on sorting
        IProfilingBlockInputStream::LocalLimits limits;
        limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
        limits.size_limits = SizeLimits(settings.max_rows_to_sort, settings.max_bytes_to_sort, settings.sort_overflow_mode);
        sorting_stream->setLimits(limits);

        stream = sorting_stream;
    });

    /// If there are several streams, we merge them into one
    executeUnion(pipeline, max_streams, log, false, "for partial order");

    /// Merge the sorted blocks.
    pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(
        pipeline.firstStream(),
        order_descr,
        settings.max_block_size,
        limit,
        settings.max_bytes_before_external_sort,
        context.getTemporaryPath(),
        log->identifier());
}

void DAGQueryBlockInterpreter::recordProfileStreams(DAGPipeline & pipeline, const String & key)
{
    auto & profile_streams = dagContext().getProfileStreamsMap()[key];
    pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
}

void DAGQueryBlockInterpreter::handleExchangeReceiver(DAGPipeline & pipeline)
{
    auto exchange_receiver = dagContext().getMPPExchangeReceiver(query_block.source_name);
    if (unlikely(exchange_receiver == nullptr))
        throw Exception("Can not find exchange receiver for " + query_block.source_name, ErrorCodes::LOGICAL_ERROR);
    // todo choose a more reasonable stream number
    auto & exchange_receiver_io_input_streams = dagContext().getInBoundIOInputStreamsMap()[query_block.source_name];
    for (size_t i = 0; i < max_streams; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(exchange_receiver, log->identifier(), query_block.source_name);
        exchange_receiver_io_input_streams.push_back(stream);
        stream = std::make_shared<SquashingBlockInputStream>(stream, 8192, 0, log->identifier());
        stream->setExtraInfo("squashing after exchange receiver");
        pipeline.streams.push_back(stream);
    }
    NamesAndTypes source_columns;
    for (const auto & col : pipeline.firstStream()->getHeader())
    {
        source_columns.emplace_back(col.name, col.type);
    }
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
}

// for tests, we need to mock ExchangeReceiver blockInputStream as the source stream.
void DAGQueryBlockInterpreter::handleMockExchangeReceiver(DAGPipeline & pipeline)
{
    if (context.getDAGContext()->columnsForTestEmpty() || context.getDAGContext()->columnsForTest(query_block.source_name).empty())
    {
        for (size_t i = 0; i < max_streams; ++i)
        {
            // use max_block_size / 10 to determine the mock block's size
            pipeline.streams.push_back(std::make_shared<MockExchangeReceiverInputStream>(query_block.source->exchange_receiver(), context.getSettingsRef().max_block_size, context.getSettingsRef().max_block_size / 10));
        }
        NamesAndTypes source_columns;
        for (const auto & col : pipeline.firstStream()->getHeader())
        {
            source_columns.emplace_back(col.name, col.type);
        }
        analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
    }
    else
    {
        auto [names_and_types, mock_exchange_streams] = mockSourceStream<MockExchangeReceiverInputStream>(context, max_streams, log, query_block.source_name);
        analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(names_and_types), context);
        pipeline.streams.insert(pipeline.streams.end(), mock_exchange_streams.begin(), mock_exchange_streams.end());
    }
}

void DAGQueryBlockInterpreter::handleProjection(DAGPipeline & pipeline, const tipb::Projection & projection)
{
    NamesAndTypes input_columns;
    pipeline.streams = input_streams_vec[0];
    for (auto const & p : pipeline.firstStream()->getHeader().getNamesAndTypesList())
        input_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(input_columns), context);
    ExpressionActionsChain chain;
    auto & last_step = dag_analyzer.initAndGetLastStep(chain);
    NamesAndTypes output_columns;
    NamesWithAliases project_cols;
    UniqueNameGenerator unique_name_generator;
    for (const auto & expr : projection.exprs())
    {
        auto expr_name = dag_analyzer.getActions(expr, last_step.actions);
        last_step.required_output.emplace_back(expr_name);
        const auto & col = last_step.actions->getSampleBlock().getByName(expr_name);
        String alias = unique_name_generator.toUniqueName(col.name);
        output_columns.emplace_back(alias, col.type);
        project_cols.emplace_back(col.name, alias);
    }
    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, chain.getLastActions(), log->identifier());
        stream->setExtraInfo("before projection");
    });
    executeProject(pipeline, project_cols, "projection");
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(output_columns), context);
}

void DAGQueryBlockInterpreter::handleWindow(DAGPipeline & pipeline, const tipb::Window & window)
{
    NamesAndTypes input_columns;
    assert(input_streams_vec.size() == 1);
    pipeline.streams = input_streams_vec.back();
    for (auto const & p : pipeline.firstStream()->getHeader())
        input_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(input_columns, context);
    WindowDescription window_description = dag_analyzer.buildWindowDescription(window);
    executeWindow(pipeline, window_description);
    executeExpression(pipeline, window_description.after_window, "cast after window");

    analyzer = std::make_unique<DAGExpressionAnalyzer>(window_description.after_window_columns, context);
}

void DAGQueryBlockInterpreter::handleWindowOrder(DAGPipeline & pipeline, const tipb::Sort & window_sort)
{
    NamesAndTypes input_columns;
    assert(input_streams_vec.size() == 1);
    pipeline.streams = input_streams_vec.back();
    for (auto const & p : pipeline.firstStream()->getHeader())
        input_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(input_columns, context);
    auto order_columns = dag_analyzer.buildWindowOrderColumns(window_sort);
    executeWindowOrder(pipeline, getSortDescription(order_columns, window_sort.byitems()));

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(input_columns), context);
}

// To execute a query block, you have to:
// 1. generate the date stream and push it to pipeline.
// 2. assign the analyzer
// 3. construct a final projection, even if it's not necessary. just construct it.
// Talking about projection, it has the following rules.
// 1. if the query block does not contain agg, then the final project is the same as the source Executor
// 2. if the query block contains agg, then the final project is the same as agg Executor
// 3. if the cop task may contains more then 1 query block, and the current query block is not the root
//    query block, then the project should add an alias for each column that needs to be projected, something
//    like final_project.emplace_back(col.name, query_block.qb_column_prefix + col.name);
void DAGQueryBlockInterpreter::executeImpl(DAGPipeline & pipeline)
{
    if (query_block.source->tp() == tipb::ExecType::TypeJoin)
    {
        SubqueryForSet right_query;
        handleJoin(query_block.source->join(), pipeline, right_query);
        recordProfileStreams(pipeline, query_block.source_name);
        dagContext().addSubquery(query_block.source_name, std::move(right_query));
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeExchangeReceiver)
    {
        if (unlikely(dagContext().isTest()))
            handleMockExchangeReceiver(pipeline);
        else
            handleExchangeReceiver(pipeline);
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeProjection)
    {
        handleProjection(pipeline, query_block.source->projection());
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else if (query_block.isTableScanSource())
    {
        TiDBTableScan table_scan(query_block.source, query_block.source_name, dagContext());
        if (unlikely(dagContext().isTest()))
            handleMockTableScan(table_scan, pipeline);
        else
            handleTableScan(table_scan, pipeline);
        dagContext().table_scan_executor_id = query_block.source_name;
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeWindow)
    {
        handleWindow(pipeline, query_block.source->window());
        recordProfileStreams(pipeline, query_block.source_name);
        restorePipelineConcurrency(pipeline);
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeSort)
    {
        handleWindowOrder(pipeline, query_block.source->sort());
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else
    {
        throw TiFlashException(
            std::string(__PRETTY_FUNCTION__) + ": Unsupported source node: " + query_block.source_name,
            Errors::Coprocessor::BadRequest);
    }

    auto res = analyzeExpressions(
        context,
        *analyzer,
        query_block,
        final_project);

    if (res.before_where)
    {
        // execute where
        executeWhere(pipeline, res.before_where, res.filter_column_name, "execute where");
        recordProfileStreams(pipeline, query_block.selection_name);
    }

    // this log measures the concurrent degree in this mpp task
    LOG_FMT_DEBUG(
        log,
        "execution stream size for query block(before aggregation) {} is {}",
        query_block.qb_column_prefix,
        pipeline.streams.size());
    dagContext().final_concurrency = std::min(std::max(dagContext().final_concurrency, pipeline.streams.size()), max_streams);

    if (res.before_aggregation)
    {
        // execute aggregation
        executeAggregation(pipeline, res.before_aggregation, res.aggregation_keys, res.aggregation_collators, res.aggregate_descriptions, res.is_final_agg);
    }
    if (res.before_having)
    {
        // execute having
        executeWhere(pipeline, res.before_having, res.having_column_name, "execute having");
        recordProfileStreams(pipeline, query_block.having_name);
    }
    if (res.before_order_and_select)
    {
        executeExpression(pipeline, res.before_order_and_select, "before order and select");
    }

    if (!res.order_columns.empty())
    {
        // execute topN
        executeOrder(pipeline, res.order_columns);
        recordProfileStreams(pipeline, query_block.limit_or_topn_name);
    }

    // execute final project action
    executeProject(pipeline, final_project, "final projection");
    // execute limit
    if (query_block.limit_or_topn && query_block.limit_or_topn->tp() == tipb::TypeLimit)
    {
        executeLimit(pipeline);
        recordProfileStreams(pipeline, query_block.limit_or_topn_name);
    }
    restorePipelineConcurrency(pipeline);

    // execute exchange_sender
    if (query_block.exchange_sender)
    {
        if (unlikely(dagContext().isTest()))
            handleMockExchangeSender(pipeline);
        else
            handleExchangeSender(pipeline);
        recordProfileStreams(pipeline, query_block.exchange_sender_name);
    }
}

void DAGQueryBlockInterpreter::executeProject(DAGPipeline & pipeline, NamesWithAliases & project_cols, const String & extra_info)
{
    if (project_cols.empty())
        return;
    ExpressionActionsPtr project = generateProjectExpressionActions(pipeline.firstStream(), context, project_cols);
    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, project, log->identifier());
        stream->setExtraInfo(extra_info);
    });
}

void DAGQueryBlockInterpreter::executeLimit(DAGPipeline & pipeline)
{
    size_t limit = 0;
    if (query_block.limit_or_topn->tp() == tipb::TypeLimit)
        limit = query_block.limit_or_topn->limit().limit();
    else
        limit = query_block.limit_or_topn->topn().limit();
    pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, log->identifier(), false); });
    if (pipeline.hasMoreThanOneStream())
    {
        executeUnion(pipeline, max_streams, log, false, "for partial limit");
        pipeline.transform([&](auto & stream) { stream = std::make_shared<LimitBlockInputStream>(stream, limit, 0, log->identifier(), false); });
    }
}

void DAGQueryBlockInterpreter::handleExchangeSender(DAGPipeline & pipeline)
{
    RUNTIME_ASSERT(dagContext().isMPPTask() && dagContext().tunnel_set != nullptr, log, "exchange_sender only run in MPP");
    /// exchange sender should be at the top of operators
    const auto & exchange_sender = query_block.exchange_sender->exchange_sender();
    std::vector<Int64> partition_col_ids = ExchangeSenderInterpreterHelper::genPartitionColIds(exchange_sender);
    TiDB::TiDBCollators partition_col_collators = ExchangeSenderInterpreterHelper::genPartitionColCollators(exchange_sender);
    int stream_id = 0;
    pipeline.transform([&](auto & stream) {
        // construct writer
        std::unique_ptr<DAGResponseWriter> response_writer = std::make_unique<StreamingDAGResponseWriter<MPPTunnelSetPtr>>(
            context.getDAGContext()->tunnel_set,
            partition_col_ids,
            partition_col_collators,
            exchange_sender.tp(),
            context.getSettingsRef().dag_records_per_chunk,
            context.getSettingsRef().batch_send_min_limit,
            stream_id++ == 0, /// only one stream needs to sending execution summaries for the last response
            dagContext());
        stream = std::make_shared<ExchangeSenderBlockInputStream>(stream, std::move(response_writer), log->identifier());
    });
}

void DAGQueryBlockInterpreter::handleMockExchangeSender(DAGPipeline & pipeline)
{
    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<MockExchangeSenderInputStream>(stream, log->identifier());
    });
}

void DAGQueryBlockInterpreter::restorePipelineConcurrency(DAGPipeline & pipeline)
{
    if (query_block.can_restore_pipeline_concurrency)
        restoreConcurrency(pipeline, dagContext().final_concurrency, log);
}

BlockInputStreams DAGQueryBlockInterpreter::execute()
{
    DAGPipeline pipeline;
    executeImpl(pipeline);
    if (!pipeline.streams_with_non_joined_data.empty())
    {
        executeUnion(pipeline, max_streams, log, false, "final union for non_joined_data");
        restorePipelineConcurrency(pipeline);
    }

    return pipeline.streams;
}
} // namespace DB