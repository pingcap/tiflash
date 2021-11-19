#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>

#include <vector>

namespace DB
{
std::vector<ExecutorStatisticsPtr> collectExecutorStatistics(DAGContext & dag_context);
} // namespace DB