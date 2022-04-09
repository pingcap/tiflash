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

#include <DataStreams/CreatingSetsBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/ExecutorIdGenerator.h>
#include <Flash/Planner/InterpreterPlan.h>
#include <Flash/Planner/PhysicalPlanBuilder.h>
#include <Flash/Planner/Rule.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Interpreters/Context.h>

namespace DB
{
InterpreterPlan::InterpreterPlan(Context & context_, const PlanQuerySource & plan_source_)
    : context(context_)
    , plan_source(plan_source_)
    , max_streams(plan_source_.getDAGContext().maxStreams(context_))
{}

BlockIO InterpreterPlan::execute()
{
    /// Due to learner read, DAGQueryBlockInterpreter may take a long time to build
    /// the query plan, so we init mpp exchange receiver before executeQueryBlock
    dagContext().initExchangeReceiverIfMPP(context, max_streams);

    PhysicalPlanBuilder physical_plan_builder(context);
    ExecutorIdGenerator id_generator;
    traverseExecutorsReverse(
        &plan_source.dagRequest(),
        [&](const tipb::Executor & executor) {
            physical_plan_builder.build(id_generator.generate(executor), &executor);
            return true;
        });
    auto physical_plan = physical_plan_builder.getResult();

    DAGPipeline pipeline;
    physical_plan->transform(pipeline, context, max_streams);

    optimize(context, physical_plan);

    /// add union to run in parallel if needed
    if (context.getDAGContext()->isMPPTask())
        /// MPPTask do not need the returned blocks.
        executeUnion(pipeline, max_streams, dagContext().log, /*ignore_block=*/true);
    else
        executeUnion(pipeline, max_streams, dagContext().log);
    if (!dagContext().subqueries_for_sets.empty())
    {
        const Settings & settings = context.getSettingsRef();
        pipeline.firstStream() = std::make_shared<CreatingSetsBlockInputStream>(
            pipeline.firstStream(),
            std::move(dagContext().subqueries_for_sets),
            SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
            dagContext().log->identifier());
    }

    BlockIO res;
    res.in = pipeline.firstStream();
    return res;
}
} // namespace DB
