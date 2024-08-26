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
    assert((dag_request->executors_size() > 0) != dag_request->has_root_executor());
    if (dag_request->executors_size() > 0)
    {
        traverseExecutorList(dag_request->executors(), std::forward<FF>(f));
    }
    else // dag_request->has_root_executor()
    {
        traverseExecutorTree(dag_request->root_executor(), std::forward<FF>(f));
    }
}

/// traverse tipb::executor list in reverse order and apply function.
/// f: (const tipb::Executor &).
/// traverse in order, because the head of executor list is the leaf node like table scan.
template <typename Container, typename FF>
void traverseExecutorListReverse(const Container & list, FF && f)
{
    for (const auto & executor : list)
        f(executor);
}

/// traverse tipb::executor tree in post order and apply function.
/// f: (const tipb::Executor &).
template <typename FF>
void traverseExecutorTreePostOrder(const tipb::Executor & executor, FF && f)
{
    getChildren(executor).forEach([&f](const tipb::Executor & child) { traverseExecutorTreePostOrder(child, f); });
    f(executor);
}

/// traverse tipb::executor of DAGRequest in reverse order and apply function.
/// f: (const tipb::Executor &).
template <typename FF>
void traverseExecutorsReverse(const tipb::DAGRequest * dag_request, FF && f)
{
    assert((dag_request->executors_size() > 0) != dag_request->has_root_executor());
    if (dag_request->executors_size() > 0)
    {
        traverseExecutorListReverse(dag_request->executors(), std::forward<FF>(f));
    }
    else // dag_request->has_root_executor()
    {
        traverseExecutorTreePostOrder(dag_request->root_executor(), std::forward<FF>(f));
    }
}
} // namespace DB
