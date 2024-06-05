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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Executor/PipelineExecutor.h>
#include <Flash/Pipeline/Pipeline.h>
#include <Flash/Pipeline/Schedule/Events/Event.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <Interpreters/Context.h>

namespace DB
{
PipelineExecutor::PipelineExecutor(
    const MemoryTrackerPtr & memory_tracker_,
    AutoSpillTrigger * auto_spill_trigger,
    const RegisterOperatorSpillContext & register_operator_spill_context,
    Context & context_,
    const String & req_id)
    : QueryExecutor(memory_tracker_, context_, req_id)
    , exec_context(
          // For mpp task, there is a unique identifier MPPTaskId, so MPPTaskId is used here as the query id of PipelineExecutor.
          // But for cop/batchCop, there is no such unique identifier, so an empty value is given here, indicating that the query id of PipelineExecutor is invalid.
          /*query_id=*/context.getDAGContext()->isMPPTask() ? context.getDAGContext()->getMPPTaskId().toString() : "",
          req_id,
          memory_tracker_,
          context.getDAGContext(),
          auto_spill_trigger,
          register_operator_spill_context,
          context.getDAGContext()->getResourceGroupName())
{
    PhysicalPlan physical_plan{context, log->identifier()};
    physical_plan.build(context.getDAGContext()->dag_request());
    physical_plan.outputAndOptimize();
    root_pipeline = physical_plan.toPipeline(exec_context, context);
    LocalAdmissionController::global_instance->warmupResourceGroupInfoCache(dagContext().getResourceGroupName());
}

void PipelineExecutor::scheduleEvents()
{
    assert(root_pipeline);
    auto events = root_pipeline->toEvents(exec_context, context, context.getMaxStreams());
    Events sources;
    for (const auto & event : events)
    {
        if (event->prepare())
            sources.push_back(event);
    }
    for (const auto & event : sources)
        event->schedule();
}

void PipelineExecutor::wait()
{
    if (unlikely(context.isTest()))
    {
        // In test mode, a single query should take no more than 5 minutes to execute.
        static std::chrono::minutes timeout(5);
        exec_context.waitFor(timeout);
    }
    else
    {
        exec_context.wait();
    }
}

void PipelineExecutor::consume(ResultHandler & result_handler)
{
    assert(result_handler);
    if (unlikely(context.isTest()))
    {
        // In test mode, a single query should take no more than 5 minutes to execute.
        static std::chrono::minutes timeout(5);
        exec_context.consumeFor(result_handler, timeout);
    }
    else
    {
        exec_context.consume(result_handler);
    }
}

ExecutionResult PipelineExecutor::execute(ResultHandler && result_handler)
{
    if (result_handler)
    {
        ///                                 ┌──get_result_sink
        /// result_handler◄──result_queue◄──┼──get_result_sink
        ///                                 └──get_result_sink

        // The queue size is same as UnionBlockInputStream = concurrency * 5.
        assert(root_pipeline);
        root_pipeline->addGetResultSink(exec_context.toConsumeMode(/*queue_size=*/context.getMaxStreams() * 5));
        scheduleEvents();
        consume(result_handler);
    }
    else
    {
        scheduleEvents();
        wait();
    }
    LOG_DEBUG(log, "query finish with {}", exec_context.getQueryProfileInfo().toJson());
    return exec_context.toExecutionResult();
}

void PipelineExecutor::cancel()
{
    exec_context.cancel();
}

String PipelineExecutor::toString() const
{
    assert(root_pipeline);
    return fmt::format("query concurrency: {}\n{}", context.getMaxStreams(), root_pipeline->toTreeString());
}

int PipelineExecutor::estimateNewThreadCount()
{
    return 0;
}

UInt64 PipelineExecutor::collectCPUTimeNs()
{
    // TODO Get cputime more accurately.
    // Currently, it is assumed that
    // - The size of the CPU task thread pool is equal to the number of CPU cores.
    // - Most of the CPU computations are executed in the CPU task thread pool.
    // Therefore, `query_profile_info.getCPUExecuteTimeNs()` is approximately equal to the actual CPU time of the query.
    // However, once these two assumptions are broken, it will lead to inaccurate acquisition of CPU time.
    // It may be necessary to obtain CPU time using a more accurate method, such as using system call `clock_gettime`.
    const auto & query_profile_info = exec_context.getQueryProfileInfo();
    auto cpu_time_ns = query_profile_info.getCPUExecuteTimeNs();
    return cpu_time_ns;
}

Block PipelineExecutor::getSampleBlock() const
{
    assert(root_pipeline);
    return root_pipeline->getSampleBlock();
}

BaseRuntimeStatistics PipelineExecutor::getRuntimeStatistics() const
{
    assert(root_pipeline);
    auto final_plan_exec_id = root_pipeline->getFinalPlanExecId();
    BaseRuntimeStatistics runtime_statistics;
    if (!final_plan_exec_id.empty())
    {
        const auto & final_profile_infos = context.getDAGContext()->getOperatorProfileInfosMap()[final_plan_exec_id];
        for (const auto & profile_info : final_profile_infos)
            runtime_statistics.append(*profile_info);
    }
    return runtime_statistics;
}

String PipelineExecutor::getExtraJsonInfo() const
{
    return exec_context.getQueryProfileInfo().toJson();
}
} // namespace DB
