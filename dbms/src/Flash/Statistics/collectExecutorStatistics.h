#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>

#include <map>

namespace DB
{
std::map<String, ExecutorStatisticsPtr> initExecutorStatistics(DAGContext & dag_context);
} // namespace DB