#pragma once
#include <common/types.h>

#include <functional>
#include <vector>

namespace Poco
{
class Logger;
}

namespace DB
{
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;

namespace PathSelector
{
template <typename T>
String choose(const std::vector<T> & paths, const PathCapacityMetricsPtr & path_capacity,
    std::function<String(const String & path)> && path_generator, Poco::Logger * log, const String & log_msg);
}

} // namespace DB
