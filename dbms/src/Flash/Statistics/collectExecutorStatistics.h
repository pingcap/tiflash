#pragma once

#include <Flash/Statistics/ExecutorStatistics.h>

#include <map>

namespace DB
{
std::map<String, ExecutorStatisticsPtr> collectExecutorStatistics(Context & context);
} // namespace DB