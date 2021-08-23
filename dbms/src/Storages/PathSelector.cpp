
#include <RaftStoreProxyFFI/ProxyFFI.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/PathPool.h>
#include <Storages/PathSelector.h>
#include <common/likely.h>
#include <common/logger_useful.h>

namespace DB
{
namespace PathSelector
{
template <typename T>
String choose(const std::vector<T> & paths, const PathCapacityMetricsPtr & global_capacity,
    std::function<String(const String & path)> && path_generator, Poco::Logger * log, const String & log_msg)
{
    assert(!paths.empty());
    if (paths.size() == 1)
        return path_generator(paths[0].path);

    DisksCapacity all_disks;
    std::map<String, FsStats> path_capacity; // TODO: seems that we don't need it actually
    std::tie(all_disks, path_capacity) = global_capacity->getDiskStatsForPaths(paths);

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

    const auto & biggest_disk = biggest_disk_iter->second;
    const auto & select_paths = biggest_disk.paths;

    // FIXME: all paths in `select_paths` are on the same disk, just choose randomly from them is ok?
    /// Find biggest available path in biggest available disk
    String chosen_path;
    double ratio = 0, biggest_ratio = 0;
    for (size_t i = 0; i < select_paths.size(); ++i)
    {
        auto & single_path_stat = path_capacity.find(select_paths[i])->second;
        ratio = 1.0 * single_path_stat.avail_size / total_available_size;
        if (biggest_ratio < ratio)
        {
            biggest_ratio = ratio;
            chosen_path = select_paths[i];
        }
    }

    LOG_INFO(log, "Choose path [" << chosen_path << "] " << log_msg);
    return path_generator(chosen_path);
}

#define INSTANTIATE_CHOOSE_FUNC(PathInfo)                                                                                 \
    template String choose<PathInfo>(const std::vector<PathInfo> & paths, const PathCapacityMetricsPtr & global_capacity, \
        std::function<String(const String & path)> && path_generator, Poco::Logger * log, const String & log_msg);

INSTANTIATE_CHOOSE_FUNC(StoragePathPool::MainPathInfo);
INSTANTIATE_CHOOSE_FUNC(StoragePathPool::LatestPathInfo);
INSTANTIATE_CHOOSE_FUNC(StoragePathPool::RaftPathInfo);

} // namespace PathSelector

} // namespace DB
