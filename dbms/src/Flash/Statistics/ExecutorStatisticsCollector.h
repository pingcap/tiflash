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

#include <Common/Exception.h>
#include <Flash/Executor/toRU.h>
#include <Flash/Statistics/ExecutorStatisticsBase.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
#include <kvproto/resource_manager.pb.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <map>

namespace DB
{
class DAGContext;
struct RUConsumption
{
    RU cpu_ru;
    UInt64 cpu_time_ns;
    RU read_ru;
    UInt64 read_bytes;
};

class ExecutorStatisticsCollector
{
public:
    explicit ExecutorStatisticsCollector(const String & req_id, bool force_fill_executor_id_ = false)
        : log(Logger::get(req_id))
        , force_fill_executor_id(force_fill_executor_id_)
    {}

    void initialize(DAGContext * dag_context_);

    String profilesToJson() const;

    void fillExecuteSummaries(tipb::SelectResponse & response);

    tipb::TiFlashExecutionInfo genTiFlashExecutionInfo();

    const std::map<String, ExecutorStatisticsPtr> & getProfiles() const { return profiles; }

    void setLocalRUConsumption(const RUConsumption & ru_info);

private:
    void collectRuntimeDetails();

    void fillLocalExecutionSummaries(tipb::SelectResponse & response);

    void fillRemoteExecutionSummaries(tipb::SelectResponse & response);

    void fillExecutionSummary(
        tipb::SelectResponse & response,
        const String & executor_id,
        const BaseRuntimeStatistics & statistic,
        UInt64 join_build_time,
        const std::unordered_map<String, DM::ScanContextPtr> & scan_context_map) const;

    void fillChildren();

    template <typename T>
    bool appendImpl(const tipb::Executor * executor)
    {
        if (T::isMatch(executor))
        {
            profiles[executor->executor_id()] = std::make_shared<T>(executor, *dag_context);
            return true;
        }
        return false;
    }

    template <typename... Ts>
    bool append(const tipb::Executor * executor)
    {
        assert(executor->has_executor_id());
        assert(profiles.find(executor->executor_id()) == profiles.end());
        return (appendImpl<Ts>(executor) || ...);
    }

private:
    DAGContext * dag_context = nullptr;
    std::map<String, ExecutorStatisticsPtr> profiles;
    const LoggerPtr log;
    bool force_fill_executor_id; // for testing list based executors
    std::optional<resource_manager::Consumption> local_ru;
};
using ExecutorStatisticsCollectorPtr = std::unique_ptr<ExecutorStatisticsCollector>;
} // namespace DB
