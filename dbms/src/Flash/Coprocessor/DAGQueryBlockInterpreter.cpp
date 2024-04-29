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

#include <Common/FailPoint.h>
#include <Common/ThresholdUtils.h>
#include <Common/TiFlashException.h>
#include <Core/FineGrainedOperatorSpillContext.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/AggregatingBlockInputStream.h>
#include <DataStreams/ExchangeSenderBlockInputStream.h>
#include <DataStreams/ExpandBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/MockExchangeSenderInputStream.h>
#include <DataStreams/MockTableScanBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataStreams/WindowBlockInputStream.h>
#include <Flash/Coprocessor/AggregationInterpreterHelper.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGQueryBlockInterpreter.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/ExchangeSenderInterpreterHelper.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/FineGrainedShuffle.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/JoinInterpreterHelper.h>
#include <Flash/Coprocessor/MockSourceStream.h>
#include <Flash/Coprocessor/StorageDisaggregatedInterpreter.h>
#include <Flash/Mpp/newMPPExchangeWriter.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/Expand2.h>
#include <Interpreters/Join.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/KVStore/TMTContext.h>

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
    , log(Logger::get(dagContext().log ? dagContext().log->identifier() : ""))
{}

namespace
{
struct AnalysisResult
{
    ExpressionActionsPtr before_where;
    ExpressionActionsPtr before_aggregation;
    ExpressionActionsPtr before_having;
    ExpressionActionsPtr before_order;
    ExpressionActionsPtr before_expand;
    ExpressionActionsPtr before_select;
    ExpressionActionsPtr final_projection;

    String filter_column_name;
    String having_column_name;
    NamesAndTypes order_columns;

    Names aggregation_keys;
    TiDB::TiDBCollators aggregation_collators;
    AggregateDescriptions aggregate_descriptions;
    bool is_final_agg = false;
    bool enable_fine_grained_shuffle_agg = false;
    std::unordered_map<String, String> key_ref_agg_func;
};

AnalysisResult analyzeExpressions(
    Context & context,
    DAGExpressionAnalyzer & analyzer,
    const DAGQueryBlock & query_block,
    NamesWithAliases & final_project)
{
    AnalysisResult res;
    ExpressionActionsChain chain;
    // selection on table scan had been executed in handleTableScan.
    // In test mode, filter is not pushed down to table scan.
    if (query_block.selection && (!query_block.isTableScanSource() || context.isTest()))
    {
        res.filter_column_name = analyzer.appendWhere(chain, query_block.selection->selection().conditions());
        res.before_where = chain.getLastActions();
        chain.addStep();
    }
    // There will be either Agg...
    if (query_block.aggregation)
    {
        res.is_final_agg = AggregationInterpreterHelper::isFinalAgg(query_block.aggregation->aggregation());
        res.enable_fine_grained_shuffle_agg
            = enableFineGrainedShuffle(query_block.aggregation->fine_grained_shuffle_stream_count());

        std::tie(
            res.aggregation_keys,
            res.aggregation_collators,
            res.aggregate_descriptions,
            res.before_aggregation,
            res.key_ref_agg_func)
            = analyzer.appendAggregation(
                chain,
                query_block.aggregation->aggregation(),
                AggregationInterpreterHelper::isGroupByCollationSensitive(context));

        if (query_block.having != nullptr)
        {
            res.having_column_name = analyzer.appendWhere(chain, query_block.having->selection().conditions());
            res.before_having = chain.getLastActions();
            chain.addStep();
        }
    }
    // Or TopN, not both.
    if (query_block.limit_or_topn && query_block.limit_or_topn->tp() == tipb::ExecType::TypeTopN)
    {
        res.order_columns = analyzer.appendOrderBy(chain, query_block.limit_or_topn->topn());
        res.before_order = chain.getLastActions();
        chain.addStep();
    }

    if (query_block.expand)
    {
        res.before_expand = analyzer.appendExpand(query_block.expand->expand(), chain);
        chain.addStep();
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
        : analyzer.appendFinalProjectForNonRootQueryBlock(chain, query_block.qb_column_prefix);

    res.before_select = chain.getLastActions();

    chain.finalize();
    chain.clear();
    //todo need call prependProjectInput??
    return res;
}
} // namespace

// for tests, we need to mock tableScan blockInputStream as the source stream.
void DAGQueryBlockInterpreter::handleMockTableScan(const TiDBTableScan & table_scan, DAGPipeline & pipeline)
{
    if (context.mockStorage()->useDeltaMerge())
    {
        assert(context.mockStorage()->tableExistsForDeltaMerge(table_scan.getLogicalTableID()));
        auto names_and_types = context.mockStorage()->getNameAndTypesForDeltaMerge(table_scan.getLogicalTableID());
        auto mock_table_scan_stream
            = context.mockStorage()->getStreamFromDeltaMerge(context, table_scan.getLogicalTableID());
        analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(names_and_types), context);
        pipeline.streams.push_back(mock_table_scan_stream);
    }
    else
    {
        /// build from user input blocks.
        size_t scan_concurrency = getMockSourceStreamConcurrency(
            max_streams,
            context.mockStorage()->getScanConcurrencyHint(table_scan.getLogicalTableID()));
        assert(context.mockStorage()->tableExists(table_scan.getLogicalTableID()));
        NamesAndTypes names_and_types;
        std::vector<std::shared_ptr<DB::MockTableScanBlockInputStream>> mock_table_scan_streams;
        if (context.isMPPTest())
        {
            std::tie(names_and_types, mock_table_scan_streams)
                = mockSourceStreamForMpp(context, scan_concurrency, log, table_scan);
        }
        else
        {
            std::tie(names_and_types, mock_table_scan_streams) = mockSourceStream<MockTableScanBlockInputStream>(
                context,
                scan_concurrency,
                log,
                table_scan.getTableScanExecutorID(),
                table_scan.getLogicalTableID());
        }

        analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(names_and_types), context);
        pipeline.streams.insert(pipeline.streams.end(), mock_table_scan_streams.begin(), mock_table_scan_streams.end());
    }

    // Ignore handling GeneratedColumnPlaceholderBlockInputStream for now, because we don't support generated column in test framework.
}


void DAGQueryBlockInterpreter::handleTableScan(const TiDBTableScan & table_scan, DAGPipeline & pipeline)
{
    const auto filter_conditions
        = FilterConditions::filterConditionsFrom(query_block.selection_name, query_block.selection);

    if (context.getSharedContextDisagg()->isDisaggregatedComputeMode())
    {
        StorageDisaggregatedInterpreter disaggregated_tiflash_interpreter(
            context,
            table_scan,
            filter_conditions,
            max_streams);
        disaggregated_tiflash_interpreter.execute(pipeline);
    }
    else
    {
        DAGStorageInterpreter storage_interpreter(context, table_scan, filter_conditions, max_streams);
        storage_interpreter.execute(pipeline);
    }
    analyzer = std::make_unique<DAGExpressionAnalyzer>(pipeline.firstStream()->getHeader(), context);
}

void DAGQueryBlockInterpreter::handleJoin(
    const tipb::Join & join,
    DAGPipeline & pipeline,
    SubqueryForSet & right_query,
    size_t fine_grained_shuffle_count)
{
    if (unlikely(input_streams_vec.size() != 2))
    {
        throw TiFlashException("Join query block must have 2 input streams", Errors::BroadcastJoin::Internal);
    }

    JoinInterpreterHelper::TiFlashJoin tiflash_join(join, context.isTest());

    DAGPipeline probe_pipeline;
    DAGPipeline build_pipeline;
    probe_pipeline.streams = input_streams_vec[1 - tiflash_join.build_side_index];
    build_pipeline.streams = input_streams_vec[tiflash_join.build_side_index];
    /// for DAGQueryBlockInterpreter, the schema is already aligned to TiDB's schema after appendFinalProjectForNonRootQueryBlock
    const auto probe_source_columns
        = JoinInterpreterHelper::genDAGExpressionAnalyzerSourceColumns(probe_pipeline.firstStream()->getHeader(), {});
    const auto build_source_columns
        = JoinInterpreterHelper::genDAGExpressionAnalyzerSourceColumns(build_pipeline.firstStream()->getHeader(), {});

    RUNTIME_ASSERT(!input_streams_vec[0].empty(), log, "left input streams cannot be empty");
    const Block & left_input_header = input_streams_vec[0].back()->getHeader();
    const auto left_source_columns
        = JoinInterpreterHelper::genDAGExpressionAnalyzerSourceColumns(left_input_header, {});

    RUNTIME_ASSERT(!input_streams_vec[1].empty(), log, "right input streams cannot be empty");
    const Block & right_input_header = input_streams_vec[1].back()->getHeader();
    const auto right_source_columns
        = JoinInterpreterHelper::genDAGExpressionAnalyzerSourceColumns(right_input_header, {});

    String match_helper_name = tiflash_join.genMatchHelperName(left_input_header, right_input_header);
    NamesAndTypes join_output_columns
        = tiflash_join.genJoinOutputColumns(left_source_columns, right_source_columns, match_helper_name);
    /// add necessary transformation if the join key is an expression

    bool is_tiflash_right_join = isRightOuterJoin(tiflash_join.kind);

    JoinNonEqualConditions join_non_equal_conditions;
    // prepare probe side
    auto [probe_side_prepare_actions, probe_key_names, original_probe_key_names, probe_filter_column_name]
        = JoinInterpreterHelper::prepareJoin(
            context,
            probe_source_columns,
            tiflash_join.getProbeJoinKeys(),
            tiflash_join.join_key_types,
            true,
            is_tiflash_right_join,
            tiflash_join.getProbeConditions());
    RUNTIME_ASSERT(probe_side_prepare_actions, log, "probe_side_prepare_actions cannot be nullptr");
    join_non_equal_conditions.left_filter_column = std::move(probe_filter_column_name);

    // prepare build side
    auto [build_side_prepare_actions, build_key_names, original_build_key_names, build_filter_column_name]
        = JoinInterpreterHelper::prepareJoin(
            context,
            build_source_columns,
            tiflash_join.getBuildJoinKeys(),
            tiflash_join.join_key_types,
            false,
            is_tiflash_right_join,
            tiflash_join.getBuildConditions());
    RUNTIME_ASSERT(build_side_prepare_actions, log, "build_side_prepare_actions cannot be nullptr");
    join_non_equal_conditions.right_filter_column = std::move(build_filter_column_name);

    tiflash_join.fillJoinOtherConditionsAction(
        context,
        left_source_columns,
        right_source_columns,
        probe_side_prepare_actions,
        original_probe_key_names,
        original_build_key_names,
        join_non_equal_conditions);

    const Settings & settings = context.getSettingsRef();
    SpillConfig build_spill_config(
        context.getTemporaryPath(),
        fmt::format("{}_0_build", log->identifier()),
        settings.max_cached_data_bytes_in_spiller,
        settings.max_spilled_rows_per_file,
        settings.max_spilled_bytes_per_file,
        context.getFileProvider(),
        settings.max_threads,
        settings.max_block_size);
    SpillConfig probe_spill_config(
        context.getTemporaryPath(),
        fmt::format("{}_0_probe", log->identifier()),
        settings.max_cached_data_bytes_in_spiller,
        settings.max_spilled_rows_per_file,
        settings.max_spilled_bytes_per_file,
        context.getFileProvider(),
        settings.max_threads,
        settings.max_block_size);
    size_t max_block_size = settings.max_block_size;
    fiu_do_on(FailPoints::minimum_block_size_for_cross_join, { max_block_size = 1; });

    String flag_mapped_entry_helper_name = tiflash_join.genFlagMappedEntryHelperName(
        left_input_header,
        right_input_header,
        join_non_equal_conditions.other_cond_expr != nullptr);
    JoinPtr join_ptr = std::make_shared<Join>(
        probe_key_names,
        build_key_names,
        tiflash_join.kind,
        log->identifier(),
        fine_grained_shuffle_count,
        settings.max_bytes_before_external_join,
        build_spill_config,
        probe_spill_config,
        RestoreConfig{settings.join_restore_concurrency, 0, 0},
        join_output_columns,
        [&](const OperatorSpillContextPtr & operator_spill_context) {
            if (context.getDAGContext() != nullptr)
            {
                context.getDAGContext()->registerOperatorSpillContext(operator_spill_context);
            }
        },
        context.getDAGContext() != nullptr ? context.getDAGContext()->getAutoSpillTrigger() : nullptr,
        tiflash_join.join_key_collators,
        join_non_equal_conditions,
        max_block_size,
        settings.shallow_copy_cross_probe_threshold,
        match_helper_name,
        flag_mapped_entry_helper_name,
        settings.join_probe_cache_columns_threshold,
        context.isTest());

    recordJoinExecuteInfo(tiflash_join.build_side_index, join_ptr);

    auto & join_execute_info = dagContext().getJoinExecuteInfoMap()[query_block.source_name];

    size_t join_build_concurrency = build_pipeline.streams.size();

    /// build side streams
    executeExpression(
        build_pipeline,
        build_side_prepare_actions,
        log,
        "append join key and join filters for build side");
    // add a HashJoinBuildBlockInputStream to build a shared hash table
    auto build_streams = [&](BlockInputStreams & streams) {
        size_t build_index = 0;
        auto extra_info = fmt::format(
            "join build, build_side_root_executor_id = {}",
            dagContext().getJoinExecuteInfoMap()[query_block.source_name].build_side_root_executor_id);
        if (enableFineGrainedShuffle(fine_grained_shuffle_count))
            extra_info = fmt::format("{} {}", extra_info, String(enableFineGrainedShuffleExtraInfo));
        for (auto & stream : streams)
        {
            stream
                = std::make_shared<HashJoinBuildBlockInputStream>(stream, join_ptr, build_index++, log->identifier());
            stream->setExtraInfo(extra_info);
            join_execute_info.join_build_streams.push_back(stream);
        }
    };
    build_streams(build_pipeline.streams);
    // for test, join executor need the return blocks to output.
    executeUnion(
        build_pipeline,
        max_streams,
        context.getSettingsRef().max_buffered_bytes_in_executor,
        log,
        /*ignore_block=*/!context.isTest(),
        "for join");

    right_query.source = build_pipeline.firstStream();
    right_query.join = join_ptr;

    join_ptr->finalize(DB::toNames(join_output_columns));
    join_ptr->initBuild(right_query.source->getHeader(), join_build_concurrency);

    /// probe side streams
    executeExpression(
        probe_pipeline,
        probe_side_prepare_actions,
        log,
        "append join key and join filters for probe side");
    NamesAndTypes source_columns;
    for (const auto & p : probe_pipeline.firstStream()->getHeader())
        source_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(source_columns), context);
    pipeline.streams = probe_pipeline.streams;
    /// add join input stream
    size_t probe_index = 0;
    join_ptr->initProbe(pipeline.firstStream()->getHeader(), pipeline.streams.size());
    for (auto & stream : pipeline.streams)
    {
        stream = std::make_shared<HashJoinProbeBlockInputStream>(
            stream,
            join_ptr,
            probe_index++,
            log->identifier(),
            settings.max_block_size);
        stream->setExtraInfo(fmt::format(
            "join probe, join_executor_id = {}, scan_hash_map_after_probe = {}",
            query_block.source_name,
            needScanHashMapAfterProbe(join_ptr->getKind())));
    }

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(join_output_columns), context);
}

void DAGQueryBlockInterpreter::recordJoinExecuteInfo(size_t build_side_index, const JoinPtr & join_ptr)
{
    const auto * build_side_root_executor = query_block.children[build_side_index]->root;
    JoinExecuteInfo join_execute_info;
    join_execute_info.build_side_root_executor_id = build_side_root_executor->executor_id();
    join_execute_info.join_profile_info = join_ptr->profile_info;
    assert(join_execute_info.join_profile_info);
    dagContext().getJoinExecuteInfoMap()[query_block.source_name] = std::move(join_execute_info);
}

void DAGQueryBlockInterpreter::executeWhere(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & expr,
    String & filter_column,
    const String & extra_info) const
{
    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<FilterBlockInputStream>(stream, expr, filter_column, log->identifier());
        stream->setExtraInfo(extra_info);
    });
}

void DAGQueryBlockInterpreter::executeWindow(
    DAGPipeline & pipeline,
    WindowDescription & window_description,
    bool enable_fine_grained_shuffle)
{
    executeExpression(pipeline, window_description.before_window, log, "before window");

    if (enable_fine_grained_shuffle)
    {
        /// Window function can be multiple threaded when fine grained shuffle is enabled.
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<WindowBlockInputStream>(stream, window_description, log->identifier());
            stream->setExtraInfo(String(enableFineGrainedShuffleExtraInfo));
        });
    }
    else
    {
        /// If there are several streams, we merge them into one.
        executeUnion(
            pipeline,
            max_streams,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log,
            false,
            "merge into one for window input");
        assert(pipeline.streams.size() == 1);
        pipeline.firstStream()
            = std::make_shared<WindowBlockInputStream>(pipeline.firstStream(), window_description, log->identifier());
    }
}

void DAGQueryBlockInterpreter::executeAggregation(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & expression_actions_ptr,
    const Names & key_names,
    const TiDB::TiDBCollators & collators,
    AggregateDescriptions & aggregate_descriptions,
    const std::unordered_map<String, String> & key_ref_agg_func,
    bool is_final_agg,
    bool enable_fine_grained_shuffle)
{
    executeExpression(pipeline, expression_actions_ptr, log, "before aggregation");

    Block before_agg_header = pipeline.firstStream()->getHeader();
    const Settings & settings = context.getSettingsRef();

    AggregationInterpreterHelper::fillArgColumnNumbers(aggregate_descriptions, before_agg_header);
    SpillConfig spill_config(
        context.getTemporaryPath(),
        log->identifier(),
        settings.max_cached_data_bytes_in_spiller,
        settings.max_spilled_rows_per_file,
        settings.max_spilled_bytes_per_file,
        context.getFileProvider(),
        settings.max_threads,
        settings.max_block_size);
    auto params = *AggregationInterpreterHelper::buildParams(
        context,
        before_agg_header,
        pipeline.streams.size(),
        enable_fine_grained_shuffle ? pipeline.streams.size() : 1,
        key_names,
        key_ref_agg_func,
        collators,
        aggregate_descriptions,
        is_final_agg,
        spill_config);

    if (enable_fine_grained_shuffle)
    {
        std::shared_ptr<FineGrainedOperatorSpillContext> fine_grained_spill_context;
        if (context.getDAGContext() != nullptr && context.getDAGContext()->isInAutoSpillMode()
            && pipeline.hasMoreThanOneStream())
            fine_grained_spill_context = std::make_shared<FineGrainedOperatorSpillContext>("aggregation", log);
        /// Go straight forward without merging phase when enable_fine_grained_shuffle
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<AggregatingBlockInputStream>(
                stream,
                params,
                true,
                log->identifier(),
                [&](const OperatorSpillContextPtr & operator_spill_context) {
                    if (fine_grained_spill_context != nullptr)
                        fine_grained_spill_context->addOperatorSpillContext(operator_spill_context);
                    else if (context.getDAGContext() != nullptr)
                    {
                        context.getDAGContext()->registerOperatorSpillContext(operator_spill_context);
                    }
                });
            stream->setExtraInfo(String(enableFineGrainedShuffleExtraInfo));
        });
        if (fine_grained_spill_context != nullptr)
            context.getDAGContext()->registerOperatorSpillContext(fine_grained_spill_context);
        recordProfileStreams(pipeline, query_block.aggregation_name);
    }
    else if (pipeline.streams.size() > 1)
    {
        /// If there are several sources, then we perform parallel aggregation
        BlockInputStreamPtr stream = std::make_shared<ParallelAggregatingBlockInputStream>(
            pipeline.streams,
            BlockInputStreams{},
            params,
            true,
            max_streams,
            settings.max_buffered_bytes_in_executor,
            settings.aggregation_memory_efficient_merge_threads
                ? static_cast<size_t>(settings.aggregation_memory_efficient_merge_threads)
                : static_cast<size_t>(settings.max_threads),
            log->identifier(),
            [&](const OperatorSpillContextPtr & operator_spill_context) {
                if (context.getDAGContext() != nullptr)
                {
                    context.getDAGContext()->registerOperatorSpillContext(operator_spill_context);
                }
            });

        pipeline.streams.resize(1);
        pipeline.firstStream() = std::move(stream);

        // should record for agg before restore concurrency. See #3804.
        recordProfileStreams(pipeline, query_block.aggregation_name);
        restorePipelineConcurrency(pipeline);
    }
    else
    {
        assert(pipeline.streams.size() == 1);
        pipeline.firstStream() = std::make_shared<AggregatingBlockInputStream>(
            pipeline.firstStream(),
            params,
            true,
            log->identifier(),
            [&](const OperatorSpillContextPtr & operator_spill_context) {
                if (context.getDAGContext() != nullptr)
                {
                    context.getDAGContext()->registerOperatorSpillContext(operator_spill_context);
                }
            });
        recordProfileStreams(pipeline, query_block.aggregation_name);
    }
}

void DAGQueryBlockInterpreter::executeWindowOrder(
    DAGPipeline & pipeline,
    SortDescription sort_desc,
    bool enable_fine_grained_shuffle) const
{
    orderStreams(pipeline, max_streams, sort_desc, 0, enable_fine_grained_shuffle, context, log);
}

void DAGQueryBlockInterpreter::executeOrder(DAGPipeline & pipeline, const NamesAndTypes & order_columns) const
{
    Int64 limit = query_block.limit_or_topn->topn().limit();
    orderStreams(
        pipeline,
        max_streams,
        getSortDescription(order_columns, query_block.limit_or_topn->topn().order_by()),
        limit,
        false,
        context,
        log);
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

    const bool enable_fine_grained_shuffle
        = enableFineGrainedShuffle(exchange_receiver->getFineGrainedShuffleStreamCount());
    String extra_info = "squashing after exchange receiver";
    size_t stream_count = max_streams;
    if (enable_fine_grained_shuffle)
    {
        extra_info += ", " + String(enableFineGrainedShuffleExtraInfo);
        stream_count = std::min(max_streams, exchange_receiver->getFineGrainedShuffleStreamCount());
    }

    for (size_t i = 0; i < stream_count; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(
            exchange_receiver,
            log->identifier(),
            query_block.source_name,
            /*stream_id=*/enable_fine_grained_shuffle ? i : 0);
        exchange_receiver_io_input_streams.push_back(stream);
        stream->setExtraInfo(extra_info);
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
    size_t fine_grained_stream_count = query_block.source->has_fine_grained_shuffle_stream_count()
        ? query_block.source->fine_grained_shuffle_stream_count()
        : 0;
    auto [schema, mock_streams] = mockSchemaAndStreamsForExchangeReceiver(
        context,
        query_block.source_name,
        log,
        query_block.source->exchange_receiver(),
        fine_grained_stream_count);
    pipeline.streams.insert(pipeline.streams.end(), mock_streams.begin(), mock_streams.end());
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(schema), context);
}

void DAGQueryBlockInterpreter::handleExpand2(DAGPipeline & pipeline, const tipb::Expand2 & expand2)
{
    NamesAndTypes input_columns;
    pipeline.streams = input_streams_vec[0];
    for (auto const & p : pipeline.firstStream()->getHeader().getNamesAndTypesList())
        input_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(std::move(input_columns), context);
    ExpressionActionsChain chain;
    NamesAndTypes gen_columns;
    NamesAndTypes output_columns;
    NamesWithAliasesVec project_cols_vec;
    UniqueNameGenerator unique_name_generator;
    ExpressionActionsPtrVec expression_actions_ptr_vec;
    auto input_col_size = dag_analyzer.getCurrentInputColumns().size();
    /// since every expr in one project level is quite different from the other in the same position of different level.
    /// eg: [#col1, null, 1]
    ///     [null, col#2, 2]
    /// we couldn't derive a unified action for merged column 1,2,3, so we use seperated action for every single projection level.
    /// but for output columns we should make a coordination for the output name, make them be always the same for a single column.
    ///
    /// Adding the example project expression to get the column type and column name in the expression system of tiflash, and use
    /// that as the expand OP's output column and type.
    ///
    /// projection example: [#col1, #col2, uint64_literal(grouping id)]
    /// projection      L1: [#col1, null, 1]
    ///                 L2: [null, col#2, 2]
    // pre-detect the nullability change action in the first projection level and generate the pre-actions.
    chain.clear();
    // strong ref here to avoid internal actions to be cleaned.
    auto header_step = dag_analyzer.initAndGetLastStep(chain);
    assert(!expand2.proj_exprs().empty());
    auto first_proj_level = expand2.proj_exprs().Get(0);
    auto horizontal_size = first_proj_level.exprs().size();
    auto vertical_size = expand2.proj_exprs().size();
    for (auto i = 0; i < horizontal_size; ++i)
    {
        // horizontally search nullability change column.
        auto expr = first_proj_level.exprs().Get(i);
        /// record the ref-col nullable attributes for header.
        /// case1: origin col is a normal col, if it's not-null, make it nullable if expr specified.
        /// case2: origin col is a const col, <non-null value>, it's must be not-null, make it nullable and break const attribute if expr specified.
        /// case3: origin col is a const col, <null value>, it's must be nullable, nothing to do.
        if (static_cast<size_t>(i) < input_col_size
            && (expr.has_field_type() && (expr.field_type().flag() & TiDB::ColumnFlagNotNull) == 0)
            && !dag_analyzer.getCurrentInputColumns()[i].type->isNullable())
        {
            // vertically search column-ref rather than literal null.
            for (auto j = 0; j < vertical_size; ++j)
            {
                // relocate expr.
                expr = expand2.proj_exprs().Get(j).exprs().Get(i);
                if (isColumnExpr(expr))
                {
                    auto col = getColumnNameAndTypeForColumnExpr(expr, dag_analyzer.getCurrentInputColumns());
                    header_step.actions->add(ExpressionAction::convertToNullable(col.name));
                    break;
                }
            }
        }
    }
    NamesAndTypes new_source_cols;
    for (const auto & origin_col : header_step.actions->getSampleBlock().getNamesAndTypesList())
        new_source_cols.emplace_back(origin_col.name, origin_col.type);
    dag_analyzer.reset(new_source_cols);

    // generate N level projections separately.
    for (auto i = 0; i < expand2.proj_exprs().size(); i++)
    {
        chain.clear();
        NamesWithAliases project_cols;
        auto & last_step = dag_analyzer.initAndGetLastStep(chain);
        const auto & project_expr = expand2.proj_exprs().Get(i);
        // output name is composed of source column name from child and generated column name.
        for (auto j = 0; j < project_expr.exprs().size(); j++)
        {
            // the original col-ref may be changed as nullable column-ref in planner side.
            auto expr = project_expr.exprs().Get(j);

            auto expr_name = dag_analyzer.getActions(expr, last_step.actions);
            last_step.required_output.emplace_back(expr_name);

            const auto & col = last_step.actions->getSampleBlock().getByName(expr_name);
            // link the current projected block column name with source output column name.
            auto output_name = static_cast<size_t>(j) < input_col_size
                ? dag_analyzer.getCurrentInputColumns()[j].name
                : expand2.generated_output_names()[j - input_col_size];
            project_cols.emplace_back(col.name, output_name);
            if (i == 0)
            {
                // just collect the output schema in the first projection level is enough.
                // output name is composed of two parts: origin base col names + specified generated col names.
                // the first part is absolutely unique while the second part is guaranteed by planner, the func below is just in case.
                String alias = unique_name_generator.toUniqueName(output_name);
                // record the generated appended schema.
                if (static_cast<size_t>(j) >= input_col_size)
                    gen_columns.emplace_back(alias, col.type);
                output_columns.emplace_back(alias, col.type);
            }
        }
        // cache this N level projection actions for expand output control.
        expression_actions_ptr_vec.emplace_back(last_step.actions);
        project_cols_vec.emplace_back(project_cols);
    }
    // add column that used to for header, append it to the pre-actions as well (convenient for header generation).
    for (const auto & gen_col : gen_columns)
    {
        ColumnWithTypeAndName column;
        column.column = gen_col.type->createColumn();
        column.name = gen_col.name;
        column.type = gen_col.type;
        header_step.actions->add(ExpressionAction::addColumn(column));
    }
    header_step.actions->finalize(toNames(output_columns));

    // unified these N level actions as one expand action.
    auto ep2 = std::make_shared<Expand2>(expression_actions_ptr_vec, header_step.actions, project_cols_vec);
    executeExpand2(pipeline, ep2);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(output_columns), context);
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
    executeExpression(pipeline, chain.getLastActions(), log, "before projection");
    executeProject(pipeline, project_cols, "projection");
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(output_columns), context);
}

void DAGQueryBlockInterpreter::handleWindow(
    DAGPipeline & pipeline,
    const tipb::Window & window,
    bool enable_fine_grained_shuffle)
{
    NamesAndTypes input_columns;
    assert(input_streams_vec.size() == 1);
    pipeline.streams = input_streams_vec.back();
    for (auto const & p : pipeline.firstStream()->getHeader())
        input_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(input_columns, context);
    WindowDescription window_description = dag_analyzer.buildWindowDescription(window);
    window_description.fillArgColumnNumbers();
    executeWindow(pipeline, window_description, enable_fine_grained_shuffle);
    executeExpression(pipeline, window_description.after_window, log, "cast after window");

    analyzer = std::make_unique<DAGExpressionAnalyzer>(window_description.after_window_columns, context);
}

void DAGQueryBlockInterpreter::handleWindowOrder(
    DAGPipeline & pipeline,
    const tipb::Sort & window_sort,
    bool enable_fine_grained_shuffle)
{
    NamesAndTypes input_columns;
    assert(input_streams_vec.size() == 1);
    pipeline.streams = input_streams_vec.back();
    for (auto const & p : pipeline.firstStream()->getHeader())
        input_columns.emplace_back(p.name, p.type);
    DAGExpressionAnalyzer dag_analyzer(input_columns, context);
    auto order_columns = dag_analyzer.buildWindowOrderColumns(window_sort);
    executeWindowOrder(pipeline, getSortDescription(order_columns, window_sort.byitems()), enable_fine_grained_shuffle);

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(input_columns), context);
}

// To execute a query block, you have to:
// 1. generate the data stream and push it to pipeline.
// 2. assign the analyzer
// 3. construct a final projection, even if it's not necessary. just construct it.
// Talking about projection, it has the following rules.
// 1. if the query block does not contain agg, then the final project is the same as the source Executor
// 2. if the query block contains agg/expand, then the final project is the same as agg/expand Executor
// 3. if the cop task may contains more then 1 query block, and the current query block is not the root
//    query block, then the project should add an alias for each column that needs to be projected, something
//    like final_project.emplace_back(col.name, query_block.qb_column_prefix + col.name);
void DAGQueryBlockInterpreter::executeImpl(DAGPipeline & pipeline)
{
    if (query_block.source->tp() == tipb::ExecType::TypeJoin)
    {
        SubqueryForSet right_query;
        handleJoin(
            query_block.source->join(),
            pipeline,
            right_query,
            query_block.source->fine_grained_shuffle_stream_count());
        recordProfileStreams(pipeline, query_block.source_name);
        dagContext().addSubquery(query_block.source_name, std::move(right_query));
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeExchangeReceiver)
    {
        if (unlikely(context.isExecutorTest() || context.isInterpreterTest()))
            handleMockExchangeReceiver(pipeline);
        else
        {
            // for MPP test, we can use real exchangeReceiver to run an query across different compute nodes
            // or use one compute node to simulate MPP process.
            handleExchangeReceiver(pipeline);
        }
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeProjection)
    {
        handleProjection(pipeline, query_block.source->projection());
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeExpand2)
    {
        handleExpand2(pipeline, query_block.source->expand2());
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else if (query_block.isTableScanSource())
    {
        TiDBTableScan table_scan(query_block.source, query_block.source_name, dagContext());
        if (!table_scan.getPushedDownFilters().empty() && unlikely(!context.getSettingsRef().dt_enable_bitmap_filter))
            throw Exception("Running late materialization but bitmap filter is disabled, please set the config "
                            "`profiles.default.dt_enable_bitmap_filter` of TiFlash to true,"
                            "or disable late materialization by set tidb variable "
                            "`tidb_opt_enable_late_materialization` to false.");
        if (unlikely(context.isTest()))
        {
            handleMockTableScan(table_scan, pipeline);
            recordProfileStreams(pipeline, query_block.source_name);
        }
        else
            handleTableScan(table_scan, pipeline);
        dagContext().table_scan_executor_id = query_block.source_name;
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeWindow)
    {
        handleWindow(
            pipeline,
            query_block.source->window(),
            enableFineGrainedShuffle(query_block.source->fine_grained_shuffle_stream_count()));
        recordProfileStreams(pipeline, query_block.source_name);
        restorePipelineConcurrency(pipeline);
    }
    else if (query_block.source->tp() == tipb::ExecType::TypeSort)
    {
        handleWindowOrder(
            pipeline,
            query_block.source->sort(),
            enableFineGrainedShuffle(query_block.source->fine_grained_shuffle_stream_count()));
        recordProfileStreams(pipeline, query_block.source_name);
    }
    else
    {
        throw TiFlashException(
            std::string(__PRETTY_FUNCTION__) + ": Unsupported source node: " + query_block.source_name,
            Errors::Coprocessor::BadRequest);
    }

    auto res = analyzeExpressions(context, *analyzer, query_block, final_project);

    if (res.before_where)
    {
        // execute where
        executeWhere(pipeline, res.before_where, res.filter_column_name, "execute where");
        recordProfileStreams(pipeline, query_block.selection_name);
    }

    // this log measures the concurrent degree in this mpp task
    LOG_DEBUG(
        log,
        "execution stream size for query block(before aggregation) {} is {}",
        query_block.qb_column_prefix,
        pipeline.streams.size());
    dagContext().updateFinalConcurrency(pipeline.streams.size(), max_streams);

    if (res.before_aggregation)
    {
        // execute aggregation
        executeAggregation(
            pipeline,
            res.before_aggregation,
            res.aggregation_keys,
            res.aggregation_collators,
            res.aggregate_descriptions,
            res.key_ref_agg_func,
            res.is_final_agg,
            res.enable_fine_grained_shuffle_agg);
    }
    if (res.before_having)
    {
        // execute having
        executeWhere(pipeline, res.before_having, res.having_column_name, "execute having");
        recordProfileStreams(pipeline, query_block.having_name);
    }
    if (res.before_order)
    {
        executeExpression(pipeline, res.before_order, log, "before order");
    }

    if (!res.order_columns.empty())
    {
        // execute topN
        executeOrder(pipeline, res.order_columns);
        recordProfileStreams(pipeline, query_block.limit_or_topn_name);
    }

    // execute limit
    if (query_block.limit_or_topn && query_block.limit_or_topn->tp() == tipb::TypeLimit)
    {
        executeLimit(pipeline);
        recordProfileStreams(pipeline, query_block.limit_or_topn_name);
    }

    // execute the expand OP after all filter/limits and so on.
    // since expand OP has some row replication work to do, place it after limit can reduce some unnecessary burden.
    // and put it before the final projection, because we should recognize some base col as grouping set col before change their alias.
    if (res.before_expand)
    {
        executeExpand(pipeline, res.before_expand);
        recordProfileStreams(pipeline, query_block.expand_name);
    }

    if (res.before_select)
    {
        executeExpression(pipeline, res.before_select, log, "before select");
    }

    // execute final project action
    executeProject(pipeline, final_project, "final projection");

    restorePipelineConcurrency(pipeline);

    // execute exchange_sender
    if (query_block.exchange_sender)
    {
        if (unlikely(context.isExecutorTest() || context.isInterpreterTest()))
            handleMockExchangeSender(pipeline);
        else
        {
            // for MPP test, we can use real exchangeReceiver to run an query across different compute nodes
            // or use one compute node to simulate MPP process.
            handleExchangeSender(pipeline);
        }
        recordProfileStreams(pipeline, query_block.exchange_sender_name);
    }
}

void DAGQueryBlockInterpreter::executeProject(
    DAGPipeline & pipeline,
    NamesWithAliases & project_cols,
    const String & extra_info) const
{
    if (project_cols.empty())
        return;
    ExpressionActionsPtr project = generateProjectExpressionActions(pipeline.firstStream(), project_cols);
    executeExpression(pipeline, project, log, extra_info);
}

void DAGQueryBlockInterpreter::executeLimit(DAGPipeline & pipeline)
{
    size_t limit = 0;
    if (query_block.limit_or_topn->tp() == tipb::TypeLimit)
        limit = query_block.limit_or_topn->limit().limit();
    else
        limit = query_block.limit_or_topn->topn().limit();
    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<LimitBlockInputStream>(stream, limit, /*offset*/ 0, log->identifier());
    });
    if (pipeline.hasMoreThanOneStream())
    {
        executeUnion(
            pipeline,
            max_streams,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log,
            false,
            "for partial limit");
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<LimitBlockInputStream>(stream, limit, /*offset*/ 0, log->identifier());
        });
    }
}

void DAGQueryBlockInterpreter::executeExpand(DAGPipeline & pipeline, const ExpressionActionsPtr & expr) const
{
    String expand_extra_info
        = fmt::format("expand: grouping set {}", expr->getActions().back().expand->getGroupingSetsDes());
    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<ExpressionBlockInputStream>(stream, expr, log->identifier());
        stream->setExtraInfo(expand_extra_info);
    });
}

void DAGQueryBlockInterpreter::executeExpand2(DAGPipeline & pipeline, const Expand2Ptr & expand) const
{
    String expand_extra_info = fmt::format("expand: leveled projection: {}", expand->getLevelProjectionDes());
    pipeline.transform([&](auto & stream) {
        // make expand2 header ahead for every stream.
        auto header = stream->getHeader();
        expand->getBeforeExpandActions()->execute(header);
        // construct ExpandBlockInputStream.
        stream = std::make_shared<ExpandBlockInputStream>(stream, expand, header, log->identifier());
        stream->setExtraInfo(expand_extra_info);
    });
}

void DAGQueryBlockInterpreter::handleExchangeSender(DAGPipeline & pipeline)
{
    /// exchange sender should be at the top of operators
    const auto & exchange_sender = query_block.exchange_sender->exchange_sender();
    std::vector<Int64> partition_col_ids = ExchangeSenderInterpreterHelper::genPartitionColIds(exchange_sender);
    TiDB::TiDBCollators partition_col_collators
        = ExchangeSenderInterpreterHelper::genPartitionColCollators(exchange_sender);
    const uint64_t stream_count = query_block.exchange_sender->fine_grained_shuffle_stream_count();
    const uint64_t batch_size = query_block.exchange_sender->fine_grained_shuffle_batch_size();

    auto enable_fine_grained_shuffle = enableFineGrainedShuffle(stream_count);
    String extra_info;
    if (enable_fine_grained_shuffle)
    {
        extra_info = String(enableFineGrainedShuffleExtraInfo);
        RUNTIME_CHECK(exchange_sender.tp() == tipb::ExchangeType::Hash, ExchangeType_Name(exchange_sender.tp()));
        RUNTIME_CHECK(stream_count <= maxFineGrainedStreamCount, stream_count);
    }
    pipeline.transform([&](auto & stream) {
        // construct writer
        std::unique_ptr<DAGResponseWriter> response_writer = newMPPExchangeWriter(
            partition_col_ids,
            partition_col_collators,
            exchange_sender.tp(),
            context.getSettingsRef().dag_records_per_chunk,
            context.getSettingsRef().batch_send_min_limit,
            dagContext(),
            enable_fine_grained_shuffle,
            stream_count,
            batch_size,
            exchange_sender.compression(),
            context.getSettingsRef().batch_send_min_limit_compression,
            log->identifier());
        stream
            = std::make_shared<ExchangeSenderBlockInputStream>(stream, std::move(response_writer), log->identifier());
        stream->setExtraInfo(extra_info);
    });
}

void DAGQueryBlockInterpreter::handleMockExchangeSender(DAGPipeline & pipeline) const
{
    pipeline.transform(
        [&](auto & stream) { stream = std::make_shared<MockExchangeSenderInputStream>(stream, log->identifier()); });
}

void DAGQueryBlockInterpreter::restorePipelineConcurrency(DAGPipeline & pipeline)
{
    if (query_block.can_restore_pipeline_concurrency)
        restoreConcurrency(
            pipeline,
            dagContext().final_concurrency,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log);
}

BlockInputStreams DAGQueryBlockInterpreter::execute()
{
    DAGPipeline pipeline;
    executeImpl(pipeline);
    return pipeline.streams;
}
DAGContext & DAGQueryBlockInterpreter::dagContext() const
{
    return *context.getDAGContext();
}
} // namespace DB
