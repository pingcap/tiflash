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
String choose(const std::vector<T> & paths, const P & global_capacity, std::function<String(const String & path)> && path_generator,
    Poco::Logger * log, const String & log_msg)
{
    assert(!paths.empty());
    if (paths.size() == 1)
        return path_generator(paths[0].path);

    DisksCapacity all_disks = global_capacity->getDiskStatsForPaths(paths);

    /// Calutate total_available_size and get a biggest fs
    size_t total_available_size = 0;
    size_t biggest_avail_size = 0;
    DisksCapacity::Iterator biggest_disk_iter = all_disks.end();
    std::tie(total_available_size, biggest_avail_size, biggest_disk_iter) = all_disks.getBiggestAvailableDisk();

    // We should choose path even if there is no available space.
    // If the actual disk space is running out, let the later `write` to throw exception.
    // If available space is limited by the quota, then write down a GC-ed file can make
    // some files be deleted later.
    if (unlikely(total_available_size == 0))
    {
        LOG_WARNING(log, "No available space for all disks, choose randomly.");
        // If no available space. direct return the first one.
        // There is no need to calculate and choose.
        return path_generator(paths[0].path);
    }

    if (unlikely(biggest_disk_iter == all_disks.end()))
    {
        LOG_WARNING(log, "Some of DISK have been removed.");
        return path_generator(paths[0].path);
    }

    const auto & select_paths = biggest_disk_iter->second.paths;

    double rand_number = rand() % select_paths.size();
    LOG_INFO(log, "Choose path [path=" << select_paths[rand_number] << "] " << log_msg);
    return path_generator(select_paths[rand_number]);
}

} // namespace PathSelector

} // namespace DB
