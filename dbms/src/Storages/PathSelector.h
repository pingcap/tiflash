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

class PathSelector
{
#ifdef DBMS_PUBLIC_GTEST
public:
#else
private:
#endif
    // Only export for testing
    template <typename T>
    inline static String chooseFromDisksCapacity(
        const std::vector<T> & paths,
        const DisksCapacity & all_disks,
        std::function<String(const String & path)> && path_generator,
        Poco::Logger * log,
        const String & log_msg)
    {
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
            LOG_WARNING(log, "No available space for all disks, choose the first one.");
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

        const auto rand_number = static_cast<size_t>(rand() % select_paths.size());
        const auto selected_path = path_generator(select_paths[rand_number]);
        LOG_INFO(log, "Choose path [path=" << selected_path << "] " << log_msg);
        return selected_path;
    }

public:
    template <typename T>
    static String choose(
        const std::vector<T> & paths,
        const PathCapacityMetricsPtr & capacity_metrics,
        std::function<String(const String & path)> && path_generator,
        Poco::Logger * log,
        const String & log_msg)
    {
        assert(!paths.empty());
        if (paths.size() == 1)
            return path_generator(paths[0].path);

        DisksCapacity all_disks = capacity_metrics->getDiskStatsForPaths(paths);
        return chooseFromDisksCapacity(paths, all_disks, std::move(path_generator), log, log_msg);
    }
};

} // namespace DB
