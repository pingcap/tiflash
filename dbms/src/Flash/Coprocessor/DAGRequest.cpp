// Copyright 2023 PingCAP, Ltd.
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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGRequest.h>
#include <Flash/Statistics/traverseExecutors.h>
#include <Flash/Coprocessor/collectOutputFieldTypes.h>

namespace DB
{
namespace
{
void check(bool condition, const String & err_msg)
{
    if unlikely (!condition)
        throw TiFlashException(err_msg, Errors::Coprocessor::BadRequest);
}

class ExecutorIdGenerator
{
public:
    String generate(const tipb::Executor & executor)
    {
        assert(!executor.has_executor_id());
        switch (executor.tp())
        {
        case tipb::ExecType::TypeSelection:
            return fmt::format("selection_{}", current_id++);
        case tipb::ExecType::TypeProjection:
            return fmt::format("project_{}", current_id++);
        case tipb::ExecType::TypeStreamAgg:
        case tipb::ExecType::TypeAggregation:
            return fmt::format("aggregation_{}", current_id++);
        case tipb::ExecType::TypeTopN:
            return fmt::format("topn_{}", current_id++);
        case tipb::ExecType::TypeLimit:
            return fmt::format("limit_{}", current_id++);
        case tipb::ExecType::TypeExchangeSender:
            return fmt::format("exchange_sender_{}", current_id++);
        case tipb::ExecType::TypeExchangeReceiver:
            return fmt::format("exchange_receiver_{}", current_id++);
        case tipb::ExecType::TypeTableScan:
        case tipb::ExecType::TypePartitionTableScan:
            return fmt::format("table_scan_{}", current_id++);
        case tipb::ExecType::TypeSort:
            return fmt::format("sort_{}", current_id++);
        case tipb::ExecType::TypeWindow:
            return fmt::format("window_{}", current_id++);
        case tipb::ExecType::TypeJoin:
            return fmt::format("join_{}", current_id++);
        case tipb::ExecType::TypeExpand:
            return fmt::format("expand_{}", current_id++);
        default:
            throw TiFlashException(
                fmt::format("Unsupported executor in DAG request: {}", executor.DebugString()),
                Errors::Coprocessor::Unimplemented);
        }
    }

private:
    UInt32 current_id = 0;
};
}

DAGRequest::DAGRequest(tipb::DAGRequest * dag_request_): dag_request(dag_request_)
{
    // Will only appear in tests.
    if unlikely (!dag_request)
        return;

    check((dag_request->executors_size() > 0) != dag_request->has_root_executor(), "dagrequest must be one of list based and tree based");
    is_tree_based = dag_request->has_root_executor();

    checkOrSetExecutorId();
}

void DAGRequest::checkOrSetExecutorId()
{
    if (isTreeBased())
    {
        // check duplicate executor_id for tree based request.
        std::unordered_set<String> ids;
        traverseExecutorTree(dag_request->root_executor(), [&](const tipb::Executor & executor) {
            check(executor.has_executor_id(), "for tree based request, executor id cannot be null");
            auto executor_id = executor.executor_id();
            check(ids.find(executor_id) == ids.end(), fmt::format("in tree based request, executor id `{}` duplicate, which is unexpected.", executor_id));
            ids.insert(executor_id);
            return true;
        });
    }
    else
    {
        // generate executor_id for list based request.
        ExecutorIdGenerator id_generator;
        for (int i = 0; i < dag_request->executors_size(); ++i)
        {
            auto * executor = dag_request->mutable_executors(i);
            check(!executor->has_executor_id(), fmt::format("for list based request, executor id must be null, the unexpected executor id: {}", executor->executor_id()));
            const auto & executor_id = id_generator.generate(*executor);
            // Set executor_id for list based executor,
            // then we can fill executor_id for Execution Summaries of list-based executors
            executor->set_executor_id(executor_id);
            list_based_executors_order.push_back(executor_id);
        }
    }
}
} // namespace DB
