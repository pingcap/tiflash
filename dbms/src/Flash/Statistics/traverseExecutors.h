#pragma once

#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

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
        static auto traverse_tree = [](auto && self, const tipb::Executor & executor, FF && f) {
            f(executor);
            for (const auto & child : getChildren(executor))
            {
                self(*child, std::forward<FF>(f));
            }
        };
        traverse_tree(traverse_tree, dag_request->root_executor(), f);
    }
}
} // namespace DB