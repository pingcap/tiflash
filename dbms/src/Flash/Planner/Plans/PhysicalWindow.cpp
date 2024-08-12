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

#include <Common/Logger.h>
#include <DataStreams/WindowBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/Plans/PhysicalWindow.h>
#include <Interpreters/Context.h>
#include <Operators/WindowTransformOp.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalWindow::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Window & window,
    const FineGrainedShuffle & fine_grained_shuffle,
    const PhysicalPlanNodePtr & child)
{
    RUNTIME_CHECK(child);
    /// The plan tree will be `PhysicalWindow <-- ... <-- PhysicalWindow <-- ... <-- PhysicalSort`.
    /// PhysicalWindow relies on the ordered data stream provided by PhysicalSort,
    /// so the child plan cannot call `restoreConcurrency` that would destroy the ordering of the input data.
    child->disableRestoreConcurrency();

    DAGExpressionAnalyzer analyzer(child->getSchema(), context);
    WindowDescription window_description = analyzer.buildWindowDescription(window);

    /// project action after window to remove useless columns.
    /// For window, we need to add column_prefix to distinguish it from the output of the next window.
    /// such as `window(row_number()) <-- window(row_number())`.
    auto schema = PhysicalPlanHelper::addSchemaProjectAction(
        window_description.after_window,
        window_description.after_window_columns,
        fmt::format("{}_", executor_id));
    window_description.after_window_columns = schema;

    auto physical_window = std::make_shared<PhysicalWindow>(
        executor_id,
        schema,
        fine_grained_shuffle,
        log->identifier(),
        child,
        window_description);
    return physical_window;
}

void PhysicalWindow::buildBlockInputStreamImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->buildBlockInputStream(pipeline, context, max_streams);

    executeExpression(pipeline, window_description.before_window, log, "before window");
    window_description.fillArgColumnNumbers();

    if (fine_grained_shuffle.enabled())
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
        RUNTIME_CHECK(pipeline.streams.size() == 1);
        pipeline.firstStream()
            = std::make_shared<WindowBlockInputStream>(pipeline.firstStream(), window_description, log->identifier());
    }

    executeExpression(pipeline, window_description.after_window, log, "expr after window");
}

void PhysicalWindow::buildPipelineExecGroupImpl(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    Context & context,
    size_t concurrency)
{
    executeExpression(exec_context, group_builder, window_description.before_window, log);
    window_description.fillArgColumnNumbers();

    if (!fine_grained_shuffle.enabled())
        executeUnion(exec_context, group_builder, context.getSettingsRef().max_buffered_bytes_in_executor, log);

    /// Window function can be multiple threaded when fine grained shuffle is enabled.
    group_builder.transform([&](auto & builder) {
        builder.appendTransformOp(
            std::make_unique<WindowTransformOp>(exec_context, log->identifier(), window_description));
    });

    if (!fine_grained_shuffle.enabled() && is_restore_concurrency)
        restoreConcurrency(
            exec_context,
            group_builder,
            concurrency,
            context.getSettingsRef().max_buffered_bytes_in_executor,
            log);

    executeExpression(exec_context, group_builder, window_description.after_window, log);
}

void PhysicalWindow::finalizeImpl(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);

    child->finalize(window_description.before_window->getRequiredColumns());
    FinalizeHelper::prependProjectInputIfNeed(window_description.before_window, child->getSampleBlock().columns());
}

const Block & PhysicalWindow::getSampleBlock() const
{
    return window_description.after_window->getSampleBlock();
}
} // namespace DB
