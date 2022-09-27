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
#include <Common/ProfileEvents.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
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

BlockIO executeDAG(IQuerySource & dag, Context & context, bool internal)
{
    RUNTIME_ASSERT(context.getDAGContext());
    auto & dag_context = *context.getDAGContext();
    const auto & logger = dag_context.log;
    RUNTIME_ASSERT(logger);

    prepareForExecute(context);

    if (!internal)
        logQuery(dag.str(context.getSettingsRef().log_queries_cut_to_length), context, logger);
    FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::random_interpreter_failpoint);
    auto interpreter = dag.interpreter(context, QueryProcessingStage::Complete);
    BlockIO res = interpreter->execute();

    // For internal == true, no need to set up the resource usage tracker.
    if (!internal)
    {
        /// Hold element of process list till end of query execution.
        if (dag_context.is_mpp_task)
        {
            /// for MPPTask, process list entry is created in MPPTask::prepare()
            RUNTIME_ASSERT(dag_context.getProcessListEntry() != nullptr, "process list entry for MPP task must not be nullptr");
            res.process_list_entry = dag_context.getProcessListEntry();
        }
        else
        {
            RUNTIME_ASSERT(dag_context.getProcessListEntry() == nullptr, "process list entry for non-MPP must be nullptr");
            res.process_list_entry = setProcessListElement(
                context,
                dag_context.dummy_query_string,
                dag_context.dummy_ast.get());
        }
        (*res.process_list_entry)->setQueryStreams(res);
    }

    prepareForInputStream(context, QueryProcessingStage::Complete, res.in);
    if (!internal)
        logQueryPipeline(logger, res.in);

    dag_context.attachBlockIO(res);
    return res;
}
} // namespace

BlockIO executeQuery(Context & context, bool internal)
{
    if (context.getSettingsRef().enable_planner)
    {
        PlanQuerySource plan(context);
        return executeDAG(plan, context, internal);
    }
    else
    {
        DAGQuerySource dag(context);
        return executeDAG(dag, context, internal);
    }
}
} // namespace DB
