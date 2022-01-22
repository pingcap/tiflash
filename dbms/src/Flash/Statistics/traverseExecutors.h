#pragma once

#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <functional>

namespace DB
{
/// tipb::executor.children().size() <= 2
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

template <typename FF>
void traverseExecutorArray(const google::protobuf::RepeatedPtrField<::tipb::Executor> & array, FF && f)
{
    for (const auto & executor : array)
    {
        if (!f(executor))
            return;
    }
}

template <typename FF>
void traverseExecutorTree(const tipb::Executor & executor, FF && f)
{
    std::function<void(const tipb::Executor & executor)> traverse_tree;
    traverse_tree = [&](const tipb::Executor & executor) {
        if (f(executor))
            getChildren(executor).forEach(traverse_tree);
    };
    traverse_tree(executor);
}

template <typename FF>
void traverseExecutors(const tipb::DAGRequest * dag_request, FF && f)
{
    assert(dag_request->executors_size() > 0 || dag_request->has_root_executor());
    if (dag_request->executors_size() > 0)
    {
        traverseExecutorArray(dag_request->executors(), std::forward<FF>(f));
    }
    else // dag_request->has_root_executor()
    {
        traverseExecutorTree(dag_request->root_executor(), std::forward<FF>(f));
    }
}
} // namespace DB