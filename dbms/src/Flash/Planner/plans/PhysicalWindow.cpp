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

#include <Common/Logger.h>
#include <DataStreams/WindowBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/FinalizeHelper.h>
#include <Flash/Planner/PhysicalPlanHelper.h>
#include <Flash/Planner/plans/PhysicalWindow.h>
#include <Interpreters/Context.h>

namespace DB
{
PhysicalPlanNodePtr PhysicalWindow::build(
    const Context & context,
    const String & executor_id,
    const LoggerPtr & log,
    const tipb::Window & window,
    const PhysicalPlanNodePtr & child)
{
    assert(child);
    child->disableRestoreConcurrency();

    DAGExpressionAnalyzer analyzer(child->getSchema(), context);
    WindowDescription window_description = analyzer.buildWindowDescription(window);

    /// project action after window to remove useless columns.
    const auto & schema = window_description.after_window_columns;
    window_description.after_window->add(ExpressionAction::project(PhysicalPlanHelper::schemaToNames(schema)));

    auto physical_window = std::make_shared<PhysicalWindow>(
        executor_id,
        schema,
        log->identifier(),
        child,
        window_description);
    return physical_window;
}

void PhysicalWindow::transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams)
{
    child->transform(pipeline, context, max_streams);

    executeExpression(pipeline, window_description.before_window, log, "before window");

    // todo support fine_grained_shuffle

    /// If there are several streams, we merge them into one
    executeUnion(pipeline, max_streams, log, false, "merge into one for window input");
    assert(pipeline.streams.size() == 1);
    pipeline.firstStream() = std::make_shared<WindowBlockInputStream>(pipeline.firstStream(), window_description, log->identifier());

    executeExpression(pipeline, window_description.after_window, log, "cast after window");
}

void PhysicalWindow::finalize(const Names & parent_require)
{
    FinalizeHelper::checkSchemaContainsParentRequire(schema, parent_require);

    FinalizeHelper::prependProjectInputIfNeed(window_description.before_window, child->getSampleBlock().columns());
}

const Block & PhysicalWindow::getSampleBlock() const
{
    return window_description.after_window->getSampleBlock();
}
} // namespace DB
