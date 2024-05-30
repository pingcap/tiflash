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
#include <Core/AutoSpillTrigger.h>
#include <Core/QueryProcessingStage.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Executor/DataStreamExecutor.h>
#include <Flash/Executor/PipelineExecutor.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Planner/Planner.h>
#include <Flash/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/Quota.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Interpreters/executeQuery.h>
#include <Storages/S3/S3Common.h>

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
    quota
        .addQuery(); /// NOTE Seems that when new time interval has come, first query is not accounted in number of queries.
    quota.checkExceeded(time(nullptr));
}

ProcessList::EntryPtr getProcessListEntry(Context & context, DAGContext & dag_context)
{
    if (dag_context.isMPPTask())
    {
        /// for MPPTask, process list entry is set in MPPTask::initProcessListEntry()
        RUNTIME_ASSERT(
            dag_context.getProcessListEntry() != nullptr,
            "process list entry for MPP task must not be nullptr");
        return dag_context.getProcessListEntry();
    }
    else
    {
        RUNTIME_ASSERT(dag_context.getProcessListEntry() == nullptr, "process list entry for non-MPP must be nullptr");
        auto process_list_entry
            = setProcessListElement(context, dag_context.dummy_query_string, dag_context.dummy_ast.get(), true);
        dag_context.setProcessListEntry(process_list_entry);
        return process_list_entry;
    }
}

QueryExecutorPtr executeAsBlockIO(Context & context, bool internal)
{
    RUNTIME_ASSERT(context.getDAGContext());
    auto & dag_context = *context.getDAGContext();
    // Resource control should only works for pipeline model.
    dag_context.clearResourceGroupName();
    const auto & logger = dag_context.log;
    RUNTIME_ASSERT(logger);

    prepareForExecute(context);

    ProcessList::EntryPtr process_list_entry;
    /// query level memory tracker
    MemoryTrackerPtr memory_tracker = nullptr;
    if (likely(!internal))
    {
        process_list_entry = getProcessListEntry(context, dag_context);
        memory_tracker = (*process_list_entry)->getMemoryTrackerPtr();
        logQuery(dag_context.dummy_query_string, context, logger);
    }

    if (memory_tracker != nullptr && memory_tracker->getLimit() != 0
        && context.getSettingsRef().auto_memory_revoke_trigger_threshold > 0)
    {
        dag_context.setAutoSpillMode();
        auto auto_spill_trigger_threshold = context.getSettingsRef().auto_memory_revoke_trigger_threshold.get();
        auto auto_spill_trigger = std::make_shared<AutoSpillTrigger>(
            memory_tracker,
            dag_context.getQueryOperatorSpillContexts(),
            auto_spill_trigger_threshold);
        dag_context.setAutoSpillTrigger(auto_spill_trigger);
    }

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_interpreter_failpoint);
    Planner planner{context};
    BlockIO res = planner.execute();
    /// Hold element of process list till end of query execution.
    res.process_list_entry = process_list_entry;

    /// if query is in auto spill mode, then setup auto spill trigger
    if (dag_context.isInAutoSpillMode())
    {
        auto * stream = dynamic_cast<IProfilingBlockInputStream *>(res.in.get());
        RUNTIME_ASSERT(stream != nullptr);
        stream->setAutoSpillTrigger(dag_context.getAutoSpillTrigger());
    }
    if (likely(!internal))
        logQueryPipeline(logger, res.in);

    dag_context.switchToStreamMode();
    return std::make_unique<DataStreamExecutor>(memory_tracker, context, logger->identifier(), res.in);
}

std::optional<QueryExecutorPtr> executeAsPipeline(Context & context, bool internal)
{
    RUNTIME_ASSERT(context.getDAGContext());
    auto & dag_context = *context.getDAGContext();
    const auto & logger = dag_context.log;
    RUNTIME_ASSERT(logger);

    RUNTIME_ASSERT(
        TaskScheduler::instance,
        "The task scheduler of the pipeline model has not been initialized, which is an exception. "
        "It is necessary to restart the TiFlash node.");

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

    if (memory_tracker != nullptr && memory_tracker->getLimit() > 0
        && context.getSettingsRef().auto_memory_revoke_trigger_threshold.get() > 0)
        dag_context.setAutoSpillMode();

    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_interpreter_failpoint);
    std::unique_ptr<PipelineExecutor> executor;
    /// if query level memory tracker has a limit, then setup auto spill trigger
    if (dag_context.isInAutoSpillMode())
    {
        auto register_operator_spill_context = [&context](const OperatorSpillContextPtr & operator_spill_context) {
            context.getDAGContext()->registerOperatorSpillContext(operator_spill_context);
        };
        auto auto_spill_trigger_threshold = context.getSettingsRef().auto_memory_revoke_trigger_threshold.get();
        auto auto_spill_trigger = std::make_shared<AutoSpillTrigger>(
            memory_tracker,
            dag_context.getQueryOperatorSpillContexts(),
            auto_spill_trigger_threshold);
        dag_context.setAutoSpillTrigger(auto_spill_trigger);
        executor = std::make_unique<PipelineExecutor>(
            memory_tracker,
            auto_spill_trigger.get(),
            register_operator_spill_context,
            context,
            logger->identifier());
    }
    else
    {
        executor = std::make_unique<PipelineExecutor>(memory_tracker, nullptr, nullptr, context, logger->identifier());
    }
    if (likely(!internal))
        LOG_INFO(logger, fmt::format("Query pipeline:\n{}", executor->toString()));
    dag_context.switchToPipelineMode();
    return {std::move(executor)};
}
} // namespace

QueryExecutorPtr queryExecute(Context & context, bool internal)
{
    if (context.getSettingsRef().enable_resource_control)
    {
        if (auto res = executeAsPipeline(context, internal); likely(res))
            return std::move(*res);
    }
    return executeAsBlockIO(context, internal);
}
} // namespace DB
