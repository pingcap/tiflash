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
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Core/QueryProcessingStage.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Executor/DataStreamExecutor.h>
#include <Flash/Executor/PipelineExecutor.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/PlanQuerySource.h>
#include <Flash/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Quota.h>
#include <Interpreters/executeQuery.h>

namespace ProfileEvents
{
extern const Event Query;
}

namespace DB
{
namespace FailPoints
{
extern const char random_interpreter_failpoint[];
} // namespace FailPoints

namespace
{
void prepareForExecute(Context & context)
{
    ProfileEvents::increment(ProfileEvents::Query);
    context.setQueryContext(context);

    QuotaForIntervals & quota = context.getQuota();
    quota.addQuery(); /// NOTE Seems that when new time interval has come, first query is not accounted in number of queries.
    quota.checkExceeded(time(nullptr));
}

ProcessList::EntryPtr getProcessListEntry(Context & context, DAGContext & dag_context)
{
    if (dag_context.is_mpp_task)
    {
        /// for MPPTask, process list entry is created in MPPTask::prepare()
        RUNTIME_ASSERT(dag_context.getProcessListEntry() != nullptr, "process list entry for MPP task must not be nullptr");
        return dag_context.getProcessListEntry();
    }
    else
    {
        RUNTIME_ASSERT(dag_context.getProcessListEntry() == nullptr, "process list entry for non-MPP must be nullptr");
        auto process_list_entry = setProcessListElement(
            context,
            dag_context.dummy_query_string,
            dag_context.dummy_ast.get());
        dag_context.setProcessListEntry(process_list_entry);
        return process_list_entry;
    }
}

QueryExecutorPtr doExecuteAsBlockIO(IQuerySource & dag, Context & context, bool internal)
{
    RUNTIME_ASSERT(context.getDAGContext());
    auto & dag_context = *context.getDAGContext();
    const auto & logger = dag_context.log;
    RUNTIME_ASSERT(logger);

    prepareForExecute(context);

    ProcessList::EntryPtr process_list_entry;
    if (likely(!internal))
    {
        process_list_entry = getProcessListEntry(context, dag_context);
        logQuery(dag.str(context.getSettingsRef().log_queries_cut_to_length), context, logger);
    }

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_interpreter_failpoint);
    auto interpreter = dag.interpreter(context, QueryProcessingStage::Complete);
    BlockIO res = interpreter->execute();
    MemoryTrackerPtr memory_tracker;
    if (likely(process_list_entry))
    {
        (*process_list_entry)->setQueryStreams(res);
        memory_tracker = (*process_list_entry)->getMemoryTrackerPtr();
    }


    /// Hold element of process list till end of query execution.
    res.process_list_entry = process_list_entry;

    if (likely(!internal))
        logQueryPipeline(logger, res.in);

    return std::make_unique<DataStreamExecutor>(memory_tracker, context, logger->identifier(), res.in);
}

std::optional<QueryExecutorPtr> executeAsPipeline(Context & context, bool internal)
{
    RUNTIME_ASSERT(context.getDAGContext());
    auto & dag_context = *context.getDAGContext();
    const auto & logger = dag_context.log;
    RUNTIME_ASSERT(logger);

    if (!TaskScheduler::instance || !Pipeline::isSupported(*dag_context.dag_request))
    {
        LOG_DEBUG(logger, "Can't run by pipeline model, fallback to block inputstream model");
        return {};
    }

    prepareForExecute(context);

    ProcessList::EntryPtr process_list_entry;
    if (likely(!internal))
    {
        process_list_entry = getProcessListEntry(context, dag_context);
        logQuery(dag_context.dummy_query_string, context, logger);
    }

    MemoryTrackerPtr memory_tracker;
    if (likely(process_list_entry))
        memory_tracker = (*process_list_entry)->getMemoryTrackerPtr();

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_interpreter_failpoint);

    PhysicalPlan physical_plan{context, logger->identifier()};
    physical_plan.build(dag_context.dag_request());
    physical_plan.outputAndOptimize();
    auto pipeline = physical_plan.toPipeline();
    auto executor = std::make_unique<PipelineExecutor>(memory_tracker, context, logger->identifier(), pipeline);
    if (likely(!internal))
        LOG_INFO(logger, fmt::format("Query pipeline:\n{}", executor->toString()));
    return {std::move(executor)};
}

QueryExecutorPtr executeAsBlockIO(Context & context, bool internal)
{
    if (context.getSettingsRef().enable_planner)
    {
        PlanQuerySource plan(context);
        return doExecuteAsBlockIO(plan, context, internal);
    }
    else
    {
        DAGQuerySource dag(context);
        return doExecuteAsBlockIO(dag, context, internal);
    }
}
} // namespace

QueryExecutorPtr queryExecute(Context & context, bool internal)
{
    // now only support pipeline model in test mode.
    if (context.isTest()
        && context.getSettingsRef().enable_planner
        && context.getSettingsRef().enable_pipeline)
    {
        if (auto res = executeAsPipeline(context, internal); res)
            return std::move(*res);
    }
    return executeAsBlockIO(context, internal);
}
} // namespace DB
