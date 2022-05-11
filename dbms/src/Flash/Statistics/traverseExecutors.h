#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <functional>

namespace DB
{
/// tipb::executor.children().size() <= 2
class Children
{
public:
    Children() = default;

    explicit Children(const tipb::Executor * child) : left(child) {}

    Children(const tipb::Executor * left_, const tipb::Executor * right_) : left(left_), right(right_) {}

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
    else /// dag_request->has_root_executor()
    {
        std::function<void(const tipb::Executor & executor)> traverse_tree;
        traverse_tree = [&](const tipb::Executor & executor) {
            f(executor);
            getChildren(executor).template forEach(traverse_tree);
        };
        traverse_tree(dag_request->root_executor());
    }
}
} // namespace DB