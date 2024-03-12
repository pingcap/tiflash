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

#include <common/types.h>
#include <tipb/select.pb.h>

#include <functional>

namespace DB
{
// TODO move more requrest related codes from DAGContext to here.
class DAGRequest
{
public:
    explicit DAGRequest(tipb::DAGRequest * dag_request_);

    const tipb::DAGRequest * operator()() const { return dag_request; }

    const tipb::DAGRequest * operator->() const { return dag_request; }

    const tipb::DAGRequest & operator*() const { return *dag_request; }

    bool isTreeBased() const { return is_tree_based; }

    const tipb::Executor & rootExecutor() const;
    String rootExecutorID() const;

    /// traverse tipb::executor of DAGRequest and apply function.
    /// func: (const tipb::Executor &) -> bool, return true to continue traverse.
    void traverse(std::function<bool(const tipb::Executor &)> && func) const;

    /// traverse tipb::executor of DAGRequest in reverse order and apply function.
    /// func: (const tipb::Executor &).
    void traverseReverse(std::function<void(const tipb::Executor &)> && func) const;

private:
    void checkOrSetExecutorId();

public:
    tipb::DAGRequest * dag_request;

    bool is_tree_based{false};

    /// Hold the order of list based executors.
    /// It is used to ensure that the order of Execution summary of list based executors is the same as the order of list based executors.
    std::vector<String> list_based_executors_order;
};

} // namespace DB
