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

#include <Flash/Coprocessor/ExecutionSummary.h>
#include <Flash/Statistics/ExecutorStatistics.h>
#include <tipb/executor.pb.h>

namespace DB
{
struct AggImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Agg";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_aggregation(); }

    static bool isSourceExecutor() { return false; }
};
using AggStatisticsBase = ExecutorStatistics<AggImpl>;

class AggStatistics : public AggStatisticsBase
{
public:
    AggStatistics(const tipb::Executor * executor, DAGContext & dag_context)
        : AggStatisticsBase(executor, dag_context)
    {}

    void fillExtraExecutionSummary(ExecutionSummary & summary) const override
    {
        // The statistics collector for an MPP task is initialized before the physical
        // plan is built.  The aggregation profile is registered while building that
        // plan, so it must be looked up when the summary is filled rather than cached
        // in the constructor above.
        const auto hash_table_stats_profile_info = dag_context.getAggregationProfileInfo(executor_id);
        if (!hash_table_stats_profile_info)
            return;
        if (const auto stats = hash_table_stats_profile_info->getHashTableStats(); stats)
            summary.hash_table_stats = *stats;
    }
};

struct WindowImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Window";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_window(); }

    static bool isSourceExecutor() { return false; }
};
using WindowStatistics = ExecutorStatistics<WindowImpl>;

struct SortImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Sort";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_sort(); }

    static bool isSourceExecutor() { return false; }
};
using SortStatistics = ExecutorStatistics<SortImpl>;

struct ExpandImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Expand";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_expand() || executor->has_expand2(); }

    static bool isSourceExecutor() { return false; }
};
using ExpandStatistics = ExecutorStatistics<ExpandImpl>;

struct FilterImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Selection";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_selection(); }

    static bool isSourceExecutor() { return false; }
};
using FilterStatistics = ExecutorStatistics<FilterImpl>;

struct LimitImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Limit";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_limit(); }

    static bool isSourceExecutor() { return false; }
};
using LimitStatistics = ExecutorStatistics<LimitImpl>;

struct ProjectImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "Projection";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_projection(); }

    static bool isSourceExecutor() { return false; }
};
using ProjectStatistics = ExecutorStatistics<ProjectImpl>;

struct TopNImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "TopN";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_topn(); }

    static bool isSourceExecutor() { return false; }
};
using TopNStatistics = ExecutorStatistics<TopNImpl>;

struct CTESinkImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "CTESink";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_cte_sink(); }

    static bool isSourceExecutor() { return false; }
};
using CTESinkStatistics = ExecutorStatistics<CTESinkImpl>;

struct CTESourceImpl
{
    static constexpr bool has_extra_info = false;

    static constexpr auto type = "CTESource";

    static bool isMatch(const tipb::Executor * executor) { return executor->has_cte_source(); }

    static bool isSourceExecutor() { return true; }
};
using CTESourceStatistics = ExecutorStatistics<CTESourceImpl>;
} // namespace DB
