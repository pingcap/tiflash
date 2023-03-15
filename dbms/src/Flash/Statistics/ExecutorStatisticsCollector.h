// Copyright 2023 PingCAP, Ltd.
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

#include <Common/Exception.h>
#include <Flash/Statistics/ExecutorStatisticsBase.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <map>
#include <memory>

namespace DB
{
class DAGContext;

class ExecutorStatisticsCollector
{
public:
    explicit ExecutorStatisticsCollector(const String & req_id,
                                         bool fill_executor_id_ = false)
        : log(Logger::get(req_id))
        , fill_executor_id(fill_executor_id_)
    {}

    void initialize(DAGContext * dag_context_);

    void collectRuntimeDetails();

    const std::map<String, ExecutorStatisticsPtr> & getResult() const { return res; }

    String resToJson() const;

    void addExecuteSummaries(tipb::SelectResponse & response);

    void fillExecutionSummary(
        tipb::SelectResponse & response,
        const String & executor_id,
        const BaseRuntimeStatistics & statistic,
        UInt64 join_build_time,
        const std::unordered_map<String, DM::ScanContextPtr> & scan_context_map) const;

    tipb::SelectResponse genExecutionSummaryResponse();

private:
    DAGContext * dag_context = nullptr;
    std::map<String, ExecutorStatisticsPtr> res;

    const LoggerPtr log;

    bool fill_executor_id; // for testing list based executors

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
using ExecutorStatisticsCollectorPtr = std::unique_ptr<ExecutorStatisticsCollector>;
} // namespace DB
