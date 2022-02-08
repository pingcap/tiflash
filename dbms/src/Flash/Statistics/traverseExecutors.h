#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <functional>

namespace DB
{
// tipb::executor.children().size() <= 2
class Children
{
public:
    Children() = default;

    explicit Children(const tipb::Executor * child)
        : left(child)
    {}

    Children(const tipb::Executor * left_, const tipb::Executor * right_)
        : left(left_)
        , right(right_)
    {}

    template <typename FF>
    void forEach(FF && f)
    {
        if (left)
        {
            f(*left);
            if (right)
                f(*right);
        }
    }

private:
    const tipb::Executor * left = nullptr;
    const tipb::Executor * right = nullptr;
};

Children getChildren(const tipb::Executor & executor);

/// traverse tipb::executor list and apply function.
/// f: (const tipb::Executor &) -> bool, return true to continue traverse.
/// traverse in reverse order, because the head of executor list is the leaf node like table scan.
template <typename Container, typename FF>
void traverseExecutorList(const Container & list, FF && f)
{
    for (auto it = list.rbegin(); it != list.rend(); ++it)
    {
        if (!f(*it))
            return;
    }
}

/// traverse tipb::executor tree and apply function.
/// f: (const tipb::Executor &) -> bool, return true to continue traverse.
template <typename FF>
void traverseExecutorTree(const tipb::Executor & executor, FF && f)
{
    if (f(executor))
        getChildren(executor).forEach([&f](const tipb::Executor & child) { traverseExecutorTree(child, f); });
}

/// traverse tipb::executor of DAGRequest and apply function.
/// f: (const tipb::Executor &) -> bool, return true to continue traverse.
template <typename FF>
void traverseExecutors(const tipb::DAGRequest * dag_request, FF && f)
{
    assert(dag_request->executors_size() > 0 || dag_request->has_root_executor());
    if (dag_request->executors_size() > 0)
    {
        traverseExecutorList(dag_request->executors(), std::forward<FF>(f));
    }
    else // dag_request->has_root_executor()
    {
        traverseExecutorTree(dag_request->root_executor(), std::forward<FF>(f));
    }
}
} // namespace DB