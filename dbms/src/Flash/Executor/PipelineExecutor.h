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

#pragma once

#include <Flash/Executor/QueryExecutor.h>
#include <Flash/Planner/PhysicalPlanNode.h>
#include <Interpreters/ProcessList.h>

namespace DB
{
class DAGScheduler;
using DAGSchedulerPtr = std::shared_ptr<DAGScheduler>;

class PipelineExecutor : public QueryExecutor
{
public:
    PipelineExecutor(
        Context & context_,
        const PhysicalPlanNodePtr & plan_node_,
        size_t max_streams,
        const String & req_id,
        std::shared_ptr<ProcessListEntry> process_list_entry_);

    ~PipelineExecutor();

    String dump() const override;

    void cancel(bool is_kill) override;

protected:
    std::pair<bool, String> execute(ResultHandler result_handler) override;

protected:
    /** process_list_entry should be destroyed after in and after out,
      *  since in and out contain pointer to an object inside process_list_entry
      *  (MemoryTracker * current_memory_tracker),
      *  which could be used before destroying of in and out.
      */
    std::shared_ptr<ProcessListEntry> process_list_entry;

    DAGSchedulerPtr dag_scheduler;

    PhysicalPlanNodePtr plan_node;

    Context & context;
};
} // namespace DB
