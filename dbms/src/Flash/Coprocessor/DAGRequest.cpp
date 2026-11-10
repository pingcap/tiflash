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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/DAGRequest.h>
#include <Flash/Statistics/traverseExecutors.h>

namespace DB
{
namespace
{
void check(bool condition, const String & err_msg)
{
    if unlikely (!condition)
        throw TiFlashException(err_msg, Errors::Coprocessor::BadRequest);
}

void checkExecutorList(const tipb::DAGRequest & request)
{
    // List-based executors are ordered from the leaf to the root. Child fields are not used.
    for (int i = 0; i < request.executors_size(); ++i)
    {
        const auto type = request.executors(i).tp();
        bool is_source = false;
        switch (type)
        {
        case tipb::ExecType::TypeTableScan:
        case tipb::ExecType::TypePartitionTableScan:
        case tipb::ExecType::TypeIndexScan:
        case tipb::ExecType::TypeExchangeReceiver:
        case tipb::ExecType::TypeCTESource:
            is_source = true;
            break;
        case tipb::ExecType::TypeSelection:
        case tipb::ExecType::TypeProjection:
        case tipb::ExecType::TypeAggregation:
        case tipb::ExecType::TypeStreamAgg:
        case tipb::ExecType::TypeTopN:
        case tipb::ExecType::TypeLimit:
        case tipb::ExecType::TypeSort:
        case tipb::ExecType::TypeWindow:
        case tipb::ExecType::TypeExpand:
        case tipb::ExecType::TypeExpand2:
        case tipb::ExecType::TypeExchangeSender:
        case tipb::ExecType::TypeCTESink:
            break;
        case tipb::ExecType::TypeJoin:
            throw TiFlashException(
                Errors::Coprocessor::BadRequest,
                "Join executor is not supported in a list-based DAG");
        default:
            throw TiFlashException(
                Errors::Coprocessor::Unimplemented,
                "Unsupported executor in list based DAG request: {}",
                request.executors(i).DebugString());
        }

        if (unlikely(is_source != (i == 0)))
            throw TiFlashException(
                Errors::Coprocessor::BadRequest,
                "Invalid list-based DAG: executor {} must be {}",
                i,
                i == 0 ? "a data source" : "a unary executor");
    }
}

class ListBasedExecutorIdGenerator
{
public:
    explicit ListBasedExecutorIdGenerator(tipb::DAGRequest * dag_request)
    {
        assert(dag_request->executors_size() > 0);
        // To check duplicate executor_id for list based request.
        // Normally list based requests do not set the executor id, but here it is just in case.
        for (int i = 0; i < dag_request->executors_size(); ++i)
        {
            const auto & executor = dag_request->executors(i);
            if (executor.has_executor_id())
            {
                const auto & executor_id = executor.executor_id();
                check(
                    executor_id_set.find(executor_id) == executor_id_set.end(),
                    fmt::format(
                        "in list based request, executor id `{}` duplicate, which is unexpected.",
                        executor_id));
                executor_id_set.insert(executor_id);
            }
        }
    }

    String generate(const tipb::Executor & executor)
    {
        // Has checked in constructor.
        if (executor.has_executor_id())
            return executor.executor_id();

        for (size_t i = 0; i < 5; ++i)
        {
            auto gen_id = doGenerate(executor);
            if (executor_id_set.find(gen_id) == executor_id_set.end())
            {
                executor_id_set.insert(gen_id);
                return gen_id;
            }
        }
        throw Exception(fmt::format(
            "We have failed five times to generate a unique id for list base executor, exists ids are: [{}]",
            fmt::join(executor_id_set, ",")));
    }

private:
    String doGenerate(const tipb::Executor & executor)
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
        case tipb::ExecType::TypeExpand:
            return fmt::format("expand_{}", current_id++);
        default:
            throw TiFlashException(
                fmt::format("Unsupported executor in list based DAG request: {}", executor.DebugString()),
                Errors::Coprocessor::Unimplemented);
        }
    }

private:
    UInt32 current_id = 0;
    std::unordered_set<String> executor_id_set;
};
} // namespace

DAGRequest::DAGRequest(tipb::DAGRequest * dag_request_)
    : dag_request(dag_request_)
{
    // Will only occur in tests.
    if unlikely (!dag_request)
        return;

    check(
        (dag_request->executors_size() > 0) != dag_request->has_root_executor(),
        "dagrequest must be one of list based and tree based");
    is_tree_based = dag_request->has_root_executor();

    if (!is_tree_based)
        checkExecutorList(*dag_request);
    checkOrSetExecutorId();
}

void DAGRequest::checkOrSetExecutorId()
{
    if (is_tree_based)
    {
        // check duplicate executor_id for tree based request.
        std::unordered_set<String> ids;
        traverseExecutorTree(dag_request->root_executor(), [&](const tipb::Executor & executor) {
            check(executor.has_executor_id(), "for tree based request, executor id cannot be null");
            const auto & executor_id = executor.executor_id();
            check(
                ids.find(executor_id) == ids.end(),
                fmt::format("in tree based request, executor id `{}` duplicate, which is unexpected.", executor_id));
            ids.insert(executor_id);
            return true;
        });
    }
    else
    {
        // generate executor_id for list based request.
        ListBasedExecutorIdGenerator id_generator{dag_request};
        for (int i = 0; i < dag_request->executors_size(); ++i)
        {
            auto * executor = dag_request->mutable_executors(i);
            const auto & executor_id = id_generator.generate(*executor);
            // Set executor_id for list based executor,
            // then we can fill executor_id for Execution Summaries of list-based executors
            executor->set_executor_id(executor_id);
            // The begin of list_based_executors_order is the leaf node like table scan.
            list_based_executors_order.push_back(executor_id);
        }
    }
}

const tipb::Executor & DAGRequest::rootExecutor() const
{
    check(dag_request, "dagrequest can't be null");
    return isTreeBased() ? dag_request->root_executor() : dag_request->executors(dag_request->executors_size() - 1);
}

String DAGRequest::rootExecutorID() const
{
    const auto & root_executor = rootExecutor();
    return root_executor.executor_id();
}

void DAGRequest::traverse(std::function<bool(const tipb::Executor &)> && func) const
{
    check(dag_request, "dagrequest can't be null");
    traverseExecutors(dag_request, std::move(func));
}

void DAGRequest::traverseReverse(std::function<void(const tipb::Executor &)> && func) const
{
    check(dag_request, "dagrequest can't be null");
    traverseExecutorsReverse(dag_request, std::move(func));
}
} // namespace DB
