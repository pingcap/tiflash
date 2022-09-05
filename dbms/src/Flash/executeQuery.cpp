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

#include <Common/ProfileEvents.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Executor/DataStreamExecutor.h>
#include <Flash/Mpp/MPPReceiverSet.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Planner/PlanQuerySource.h>
#include <Flash/Planner/Planner.h>
#include <Flash/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/makeDummyQuery.h>

namespace ProfileEvents
{
extern const Event Query;
}

namespace DB
{
BlockIO executeQueryAsStream(
    Context & context,
    bool internal,
    QueryProcessingStage::Enum stage)
{
    if (context.getSettingsRef().enable_planner)
    {
        PlanQuerySource plan(context);
        return executeQuery(plan, context, internal, stage);
    }
    else
    {
        DAGQuerySource dag(context);
        return executeQuery(dag, context, internal, stage);
    }
}

QueryExecutorPtr executeQuery(
    Context & context,
    bool internal,
    QueryProcessingStage::Enum stage)
{
    if (context.getSettingsRef().enable_planner && context.getSettingsRef().enable_pipeline)
    {
        PlanQuerySource plan(context);
        if (plan.isSupportPipeline())
        {
            context.getDAGContext()->is_pipeline_mode = true;
            assert(!internal && stage == QueryProcessingStage::Complete);
            const Settings & settings = context.getSettingsRef();

            auto [query, ast] = plan.parse(settings.max_query_size);
    
            ProfileEvents::increment(ProfileEvents::Query);
            context.setQueryContext(context);
            ProcessList::EntryPtr process_list_entry = context.getProcessList().insert(
                query,
                ast.get(),
                context.getClientInfo(),
                settings);
            context.setProcessListElement(&process_list_entry->get());
    
            // Do set-up work for tunnels and receivers after ProcessListEntry is constructed,
            // so that we can propagate current_memory_tracker into them.
            if (context.getDAGContext()) // When using TiFlash client, dag context will be nullptr in this case.
            {
                if (context.getDAGContext()->tunnel_set)
                    context.getDAGContext()->tunnel_set->updateMemTracker();
                if (context.getDAGContext()->getMppReceiverSet())
                    context.getDAGContext()->getMppReceiverSet()->setUpConnection();
            }
    
            auto interpreter = plan.interpreter(context, stage);
            const auto * planner = static_cast<Planner *>(interpreter.get());
            return planner->pipelineExecute(process_list_entry);
        }
    }
    return std::make_shared<DataStreamExecutor>(executeQueryAsStream(context, internal, stage));
}
} // namespace DB
