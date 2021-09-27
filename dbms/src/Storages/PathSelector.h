#pragma once
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <Storages/PathSelector.h>
#include <common/likely.h>
#include <common/logger_useful.h>
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
template <typename T, typename P>
String choose(const std::vector<T> & paths, const P & global_capacity, std::function<String(const String & path)> && path_generator, Poco::Logger * log, const String & log_msg)
{
    assert(!paths.empty());
    if (paths.size() == 1)
        return path_generator(paths[0].path);

    DisksCapacity all_disks = global_capacity->getDiskStatsForPaths(paths);

    /// Calutate total_available_size and get a biggest fs
    size_t total_available_size = 0;
    size_t total_stat_available_size = 0;
    std::vector<std::pair<FsStats, String>> path_stats;
    std::tie(total_stat_available_size, total_available_size, path_stats) = all_disks.getAvailablePaths();

    // We should choose path even if there is no available space.
    // If the actual disk space is running out, let the later `write` to throw exception.
    // If available space is limited by the quota, then write down a GC-ed file can make
    // some files be deleted later.
    if (unlikely(total_available_size == 0))
    {
        LOG_WARNING(log, "No available space for all disks, choose the first one.");
        // If no available space. direct return the first one.
        // There is no need to calculate and choose.
        return path_generator(paths[0].path);
    }

    if (total_stat_available_size > total_available_size)
    {
        LOG_WARNING(log, "Total stat available size bigger than total disk available size.");
    }

    std::vector<double> ratio;
    for (const auto & stats : path_stats)
    {
        ratio.push_back(1.0 * stats.first.avail_size / total_stat_available_size);
    }

    double rand_number = (double)rand() / RAND_MAX;
    double ratio_sum = 0.0;
    for (size_t i = 0; i < ratio.size(); i++)
    {
        ratio_sum += ratio[i];
        if ((rand_number < ratio_sum) || (i == ratio.size() - 1))
        {
            LOG_INFO(log, "Choose path [index=" << i << "] " << log_msg);
            return path_generator(path_stats[i].second);
        }
    }

    throw Exception("Should not reach here", ErrorCodes::LOGICAL_ERROR);
}

} // namespace PathSelector

} // namespace DB
