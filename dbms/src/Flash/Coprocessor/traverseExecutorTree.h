#pragma once

#include <tipb/executor.pb.h>

namespace DB
{
std::vector<const tipb::Executor *> getChildren(const tipb::Executor & executor);

template <typename FF>
void traverseExecutorTree(const tipb::Executor & executor, FF && f)
{
    f(executor);
    for (const auto & child : getChildren(executor))
    {
        traverseExecutorTree(*child, std::forward<FF>(f));
    }
}
} // namespace DB
