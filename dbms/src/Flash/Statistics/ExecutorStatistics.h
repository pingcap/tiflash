#pragma once

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Statistics/AggStatistics.h>
#include <Flash/Statistics/FilterStatistics.h>

#include <map>

namespace DB
{
struct ExecutorStatistics
{
    std::vector<AggStatisticsPtr> agg_stats;
    std::vector<FilterStatisticsPtr> filter_stats;

    void collectExecutorStatistics(DAGContext & dag_context);

    String toString() const;
};
} // namespace DB