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

#include <Common/ThresholdUtils.h>
#include <Core/FineGrainedOperatorSpillContext.h>
#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/GeneratedColumnPlaceholderBlockInputStream.h>
#include <DataStreams/LimitTransformAction.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <DataStreams/SharedQueryBlockInputStream.h>
#include <DataStreams/SortHelper.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Interpreters/Context.h>
#include <Operators/ExpressionTransformOp.h>
#include <Operators/FilterTransformOp.h>
#include <Operators/GeneratedColumnPlaceHolderTransformOp.h>
#include <Operators/LimitTransformOp.h>
#include <Operators/MergeSortTransformOp.h>
#include <Operators/PartialSortTransformOp.h>
#include <Operators/SharedQueue.h>

namespace DB
{
namespace
{
using UnionWithBlock = UnionBlockInputStream<>;
using UnionWithoutBlock = UnionBlockInputStream<StreamUnionMode::Basic, /*ignore_block=*/true>;
} // namespace

void restoreConcurrency(DAGPipeline & pipeline, size_t concurrency, Int64 max_buffered_bytes, const LoggerPtr & log)
{
    if (concurrency > 1 && pipeline.streams.size() == 1)
    {
        BlockInputStreamPtr shared_query_block_input_stream = std::make_shared<SharedQueryBlockInputStream>(
            concurrency * 5,
            max_buffered_bytes,
            pipeline.firstStream(),
            log->identifier());
        shared_query_block_input_stream->setExtraInfo("restore concurrency");
        pipeline.streams.assign(concurrency, shared_query_block_input_stream);
    }
}

void executeUnion(
    DAGPipeline & pipeline,
    size_t max_streams,
    Int64 max_buffered_bytes,
    const LoggerPtr & log,
    bool ignore_block,
    const String & extra_info)
{
    if (pipeline.streams.size() > 1)
    {
        BlockInputStreamPtr stream;
        if (ignore_block)
            stream = std::make_shared<UnionWithoutBlock>(
                pipeline.streams,
                BlockInputStreams{},
                max_streams,
                max_buffered_bytes,
                log->identifier());
        else
            stream = std::make_shared<UnionWithBlock>(
                pipeline.streams,
                BlockInputStreams{},
                max_streams,
                max_buffered_bytes,
                log->identifier());
        stream->setExtraInfo(extra_info);

        pipeline.streams.resize(1);
        pipeline.firstStream() = std::move(stream);
    }
}

void restoreConcurrency(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    size_t concurrency,
    Int64 max_buffered_bytes,
    const LoggerPtr & log)
{
    if (concurrency > 1 && group_builder.concurrency() == 1)
    {
        // Doesn't use `auto [shared_queue_sink_holder, shared_queue_source_holder]` just to make c++ compiler happy.
        SharedQueueSinkHolderPtr shared_queue_sink_holder;
        SharedQueueSourceHolderPtr shared_queue_source_holder;
        std::tie(shared_queue_sink_holder, shared_queue_source_holder)
            = SharedQueue::build(exec_context, 1, concurrency, max_buffered_bytes);
        // sink op of builder must be empty.
        group_builder.transform([&](auto & builder) {
            builder.setSinkOp(
                std::make_unique<SharedQueueSinkOp>(exec_context, log->identifier(), shared_queue_sink_holder));
        });
        auto cur_header = group_builder.getCurrentHeader();
        group_builder.addGroup();
        for (size_t i = 0; i < concurrency; ++i)
            group_builder.addConcurrency(std::make_unique<SharedQueueSourceOp>(
                exec_context,
                log->identifier(),
                cur_header,
                shared_queue_source_holder));
    }
}

void executeUnion(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Int64 max_buffered_bytes,
    const LoggerPtr & log)
{
    if (group_builder.concurrency() > 1)
    {
        // Doesn't use `auto [shared_queue_sink_holder, shared_queue_source_holder]` just to make c++ compiler happy.
        SharedQueueSinkHolderPtr shared_queue_sink_holder;
        SharedQueueSourceHolderPtr shared_queue_source_holder;
        std::tie(shared_queue_sink_holder, shared_queue_source_holder)
            = SharedQueue::build(exec_context, group_builder.concurrency(), 1, max_buffered_bytes);
        group_builder.transform([&](auto & builder) {
            builder.setSinkOp(
                std::make_unique<SharedQueueSinkOp>(exec_context, log->identifier(), shared_queue_sink_holder));
        });
        auto cur_header = group_builder.getCurrentHeader();
        group_builder.addGroup();
        group_builder.addConcurrency(std::make_unique<SharedQueueSourceOp>(
            exec_context,
            log->identifier(),
            cur_header,
            shared_queue_source_holder));
    }
}

ExpressionActionsPtr generateProjectExpressionActions(
    const BlockInputStreamPtr & stream,
    const NamesWithAliases & project_cols)
{
    NamesAndTypesList input_column;
    for (const auto & column : stream->getHeader())
        input_column.emplace_back(column.name, column.type);
    ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column);
    project->add(ExpressionAction::project(project_cols));
    return project;
}

void executeExpression(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & expr_actions,
    const LoggerPtr & log,
    const String & extra_info)
{
    if (expr_actions && !expr_actions->getActions().empty())
    {
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<ExpressionBlockInputStream>(stream, expr_actions, log->identifier());
            stream->setExtraInfo(extra_info);
        });
    }
}

void executeExpression(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const ExpressionActionsPtr & expr_actions,
    const LoggerPtr & log)
{
    if (expr_actions && !expr_actions->getActions().empty())
    {
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(
                std::make_unique<ExpressionTransformOp>(exec_context, log->identifier(), expr_actions));
        });
    }
}

void orderStreams(
    DAGPipeline & pipeline,
    size_t max_streams,
    const SortDescription & order_descr,
    Int64 limit,
    bool enable_fine_grained_shuffle,
    const Context & context,
    const LoggerPtr & log)
{
    const Settings & settings = context.getSettingsRef();
    String extra_info;
    if (enable_fine_grained_shuffle)
        extra_info = enableFineGrainedShuffleExtraInfo;

    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, log->identifier(), limit);
        stream->setExtraInfo(extra_info);
    });

    if (enable_fine_grained_shuffle)
    {
        std::shared_ptr<FineGrainedOperatorSpillContext> fine_grained_spill_context;
        if (context.getDAGContext() != nullptr && context.getDAGContext()->isInAutoSpillMode()
            && pipeline.hasMoreThanOneStream())
            fine_grained_spill_context = std::make_shared<FineGrainedOperatorSpillContext>("sort", log);
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<MergeSortingBlockInputStream>(
                stream,
                order_descr,
                settings.max_block_size,
                limit,
                getAverageThreshold(settings.max_bytes_before_external_sort, pipeline.streams.size()),
                SpillConfig(
                    context.getTemporaryPath(),
                    log->identifier(),
                    settings.max_cached_data_bytes_in_spiller,
                    settings.max_spilled_rows_per_file,
                    settings.max_spilled_bytes_per_file,
                    context.getFileProvider()),
                log->identifier(),
                [&](const OperatorSpillContextPtr & operator_spill_context) {
                    if (fine_grained_spill_context != nullptr)
                        fine_grained_spill_context->addOperatorSpillContext(operator_spill_context);
                    else if (context.getDAGContext() != nullptr)
                        context.getDAGContext()->registerOperatorSpillContext(operator_spill_context);
                });
            stream->setExtraInfo(String(enableFineGrainedShuffleExtraInfo));
        });
        if (fine_grained_spill_context != nullptr)
            context.getDAGContext()->registerOperatorSpillContext(fine_grained_spill_context);
    }
    else
    {
        /// If there are several streams, we merge them into one
        executeUnion(pipeline, max_streams, settings.max_buffered_bytes_in_executor, log, false, "for partial order");

        /// Merge the sorted blocks.
        pipeline.firstStream() = std::make_shared<MergeSortingBlockInputStream>(
            pipeline.firstStream(),
            order_descr,
            settings.max_block_size,
            limit,
            settings.max_bytes_before_external_sort,
            // todo use identifier_executor_id as the spill id
            SpillConfig(
                context.getTemporaryPath(),
                log->identifier(),
                settings.max_cached_data_bytes_in_spiller,
                settings.max_spilled_rows_per_file,
                settings.max_spilled_bytes_per_file,
                context.getFileProvider()),
            log->identifier(),
            [&](const OperatorSpillContextPtr & operator_spill_context) {
                if (context.getDAGContext() != nullptr)
                {
                    context.getDAGContext()->registerOperatorSpillContext(operator_spill_context);
                }
            });
    }
}

void executeLocalSort(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const SortDescription & order_descr,
    std::optional<size_t> limit,
    bool for_fine_grained_executor,
    const Context & context,
    const LoggerPtr & log)
{
    auto input_header = group_builder.getCurrentHeader();
    if (SortHelper::isSortByConstants(input_header, order_descr))
    {
        // For order by const col and has limit, we will generate LimitOperator directly.
        if (limit)
        {
            group_builder.transform([&](auto & builder) {
                auto local_limit = std::make_shared<LocalLimitTransformAction>(input_header, *limit);
                builder.appendTransformOp(
                    std::make_unique<LimitTransformOp<LocalLimitPtr>>(exec_context, log->identifier(), local_limit));
            });
        }
        // For order by const and doesn't has limit, do nothing here.
    }
    else
    {
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<PartialSortTransformOp>(
                exec_context,
                log->identifier(),
                order_descr,
                limit.value_or(0))); // 0 means that no limit in PartialSortTransformOp.
        });
        const Settings & settings = context.getSettingsRef();
        size_t max_bytes_before_external_sort
            = getAverageThreshold(settings.max_bytes_before_external_sort, group_builder.concurrency());
        std::shared_ptr<FineGrainedOperatorSpillContext> fine_grained_spill_context;
        if (for_fine_grained_executor && context.getDAGContext() != nullptr
            && context.getDAGContext()->isInAutoSpillMode() && group_builder.concurrency() > 1)
            fine_grained_spill_context = std::make_shared<FineGrainedOperatorSpillContext>("sort", log);
        SpillConfig spill_config{
            context.getTemporaryPath(),
            log->identifier(),
            settings.max_cached_data_bytes_in_spiller,
            settings.max_spilled_rows_per_file,
            settings.max_spilled_bytes_per_file,
            context.getFileProvider()};
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<MergeSortTransformOp>(
                exec_context,
                log->identifier(),
                order_descr,
                limit.value_or(0), // 0 means that no limit in MergeSortTransformOp.
                settings.max_block_size,
                max_bytes_before_external_sort,
                spill_config,
                fine_grained_spill_context));
        });
        if (fine_grained_spill_context != nullptr)
            exec_context.registerOperatorSpillContext(fine_grained_spill_context);
    }
}

void executeFinalSort(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const SortDescription & order_descr,
    std::optional<size_t> limit,
    const Context & context,
    const LoggerPtr & log)
{
    auto input_header = group_builder.getCurrentHeader();
    if (SortHelper::isSortByConstants(input_header, order_descr))
    {
        // For order by const col and has limit, we will generate LimitOperator directly.
        if (limit)
        {
            auto global_limit = std::make_shared<GlobalLimitTransformAction>(input_header, *limit);
            group_builder.transform([&](auto & builder) {
                builder.appendTransformOp(
                    std::make_unique<LimitTransformOp<GlobalLimitPtr>>(exec_context, log->identifier(), global_limit));
            });
        }
        // For order by const and doesn't has limit, do nothing here.
    }
    else
    {
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<PartialSortTransformOp>(
                exec_context,
                log->identifier(),
                order_descr,
                limit.value_or(0))); // 0 means that no limit in PartialSortTransformOp.
        });

        const Settings & settings = context.getSettingsRef();
        executeUnion(exec_context, group_builder, settings.max_buffered_bytes_in_executor, log);

        SpillConfig spill_config{
            context.getTemporaryPath(),
            log->identifier(),
            settings.max_cached_data_bytes_in_spiller,
            settings.max_spilled_rows_per_file,
            settings.max_spilled_bytes_per_file,
            context.getFileProvider()};
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(std::make_unique<MergeSortTransformOp>(
                exec_context,
                log->identifier(),
                order_descr,
                limit.value_or(0), // 0 means that no limit in MergeSortTransformOp.
                settings.max_block_size,
                settings.max_bytes_before_external_sort,
                spill_config,
                nullptr));
        });
    }
}

void executeCreatingSets(DAGPipeline & pipeline, const Context & context, size_t max_streams, const LoggerPtr & log)
{
    DAGContext & dag_context = *context.getDAGContext();
    /// add union to run in parallel if needed
    if (unlikely(context.isExecutorTest() || context.isInterpreterTest()))
        executeUnion(
            pipeline,
            max_streams,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log,
            /*ignore_block=*/false,
            "for test");
    else if (context.isMPPTest())
        executeUnion(
            pipeline,
            max_streams,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log,
            /*ignore_block=*/true,
            "for mpp test");
    else if (dag_context.isMPPTask())
        /// MPPTask do not need the returned blocks.
        executeUnion(
            pipeline,
            max_streams,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log,
            /*ignore_block=*/true,
            "for mpp");
    else
        executeUnion(
            pipeline,
            max_streams,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log,
            /*ignore_block=*/false,
            "for non mpp");
    if (dag_context.hasSubquery())
    {
        const Settings & settings = context.getSettingsRef();
        pipeline.firstStream() = std::make_shared<CreatingSetsBlockInputStream>(
            pipeline.firstStream(),
            std::move(dag_context.moveSubqueries()),
            SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
            log->identifier());
    }
}

void executePushedDownFilter(
    const FilterConditions & filter_conditions,
    DAGExpressionAnalyzer & analyzer,
    LoggerPtr log,
    DAGPipeline & pipeline)
{
    auto [before_where, filter_column_name, project_after_where]
        = analyzer.buildPushDownFilter(filter_conditions.conditions, true);

    for (auto & stream : pipeline.streams)
    {
        stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, log->identifier());
        // todo link runtime filter
        stream->setExtraInfo("push down filter");
        // after filter, do project action to keep the schema of local streams and remote streams the same.
        stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, log->identifier());
        stream->setExtraInfo("projection after push down filter");
    }
}

void executePushedDownFilter(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const FilterConditions & filter_conditions,
    DAGExpressionAnalyzer & analyzer,
    LoggerPtr log)
{
    auto [before_where, filter_column_name, project_after_where]
        = analyzer.buildPushDownFilter(filter_conditions.conditions, true);

    auto input_header = group_builder.getCurrentHeader();
    for (size_t i = 0; i < group_builder.concurrency(); ++i)
    {
        auto & builder = group_builder.getCurBuilder(i);
        builder.appendTransformOp(std::make_unique<FilterTransformOp>(
            exec_context,
            log->identifier(),
            input_header,
            before_where,
            filter_column_name));
        // after filter, do project action to keep the schema of local transforms and remote transforms the same.
        builder.appendTransformOp(
            std::make_unique<ExpressionTransformOp>(exec_context, log->identifier(), project_after_where));
    }
}

void executeGeneratedColumnPlaceholder(
    const std::vector<std::tuple<UInt64, String, DataTypePtr>> & generated_column_infos,
    LoggerPtr log,
    DAGPipeline & pipeline)
{
    if (generated_column_infos.empty())
        return;
    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<GeneratedColumnPlaceholderBlockInputStream>(
            stream,
            generated_column_infos,
            log->identifier());
        stream->setExtraInfo("generated column placeholder above table scan");
    });
}

void executeGeneratedColumnPlaceholder(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const std::vector<std::tuple<UInt64, String, DataTypePtr>> & generated_column_infos,
    LoggerPtr log)
{
    if (generated_column_infos.empty())
        return;

    auto input_header = group_builder.getCurrentHeader();
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(std::make_unique<GeneratedColumnPlaceHolderTransformOp>(
            exec_context,
            log->identifier(),
            input_header,
            generated_column_infos));
    });
}

} // namespace DB
