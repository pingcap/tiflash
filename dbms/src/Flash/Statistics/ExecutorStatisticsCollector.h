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

#include <Flash/Statistics/ExecutorStatisticsBase.h>
#include <tipb/executor.pb.h>

#include <map>

namespace DB
{
class DAGContext;

class ExecutorStatisticsCollector
{
public:
    void initialize(DAGContext * dag_context_);

    void collectRuntimeDetails();

    const std::map<String, ExecutorStatisticsPtr> & getResult() const { return res; }

    String resToJson() const;

    DAGContext & getDAGContext() const;

private:
    DAGContext * dag_context = nullptr;

    std::map<String, ExecutorStatisticsPtr> res;

    template <typename T>
    bool doAppend(const String & executor_id, const tipb::Executor * executor)
    {
        if (T::isMatch(executor))
        {
            res[executor_id] = std::make_shared<T>(executor, *dag_context);
            return true;
        }
        return false;
    }

    template <typename... Ts>
    bool append(const String & executor_id, const tipb::Executor * executor)
    {
        RUNTIME_CHECK(res.find(executor_id) == res.end());
        return (doAppend<Ts>(executor_id, executor) || ...);
    }
};
} // namespace DB