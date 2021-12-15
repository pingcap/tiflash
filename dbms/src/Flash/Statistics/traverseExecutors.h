#pragma once

#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <functional>

namespace DB
{
std::vector<const tipb::Executor *> getChildren(const tipb::Executor & executor);

template <typename FF>
void traverseExecutors(const tipb::DAGRequest * dag_request, FF && f)
{
    assert(dag_request->executors_size() > 0 || dag_request->has_root_executor());
    if (dag_request->executors_size() > 0)
    {
        for (const auto & executor : dag_request->executors())
        {
            f(executor);
        }
    }
    else // dag_request->has_root_executor()
    {
        std::function<void(const tipb::Executor & executor)> traverse_tree;
        traverse_tree = [&](const tipb::Executor & executor) {
            f(executor);
            for (const auto & child : getChildren(executor))
            {
                traverse_tree(*child);
            }
        };
        traverse_tree(dag_request->root_executor());
    }
}
} // namespace DB