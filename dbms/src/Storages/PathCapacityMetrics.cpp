#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <common/logger_useful.h>
#include <sys/statvfs.h>

#include <string>
#include <vector>

namespace DB
{
PathCapacityMetrics::PathCapacityMetrics(const std::vector<std::string> & all_paths, const std::vector<size_t> & capacities)
{
    for (size_t i = 0; i < all_paths.size(); ++i)
    {
        CapacityInfo info;
        info.path = all_paths[i];
        info.capacity_bytes = (i >= capacities.size()) ? 0 : capacities[i];
        path_infos.emplace_back(info);
    }
}

void PathCapacityMetrics::addUsedSize(const std::string & file_path, size_t used_bytes)
{
    ssize_t path_idx = locatePath(file_path);
    if (path_idx == INVALID_INDEX)
    {
        LOG_ERROR(log, "Can not locate path in addUsedSize. File: " + file_path);
        return;
    }

    // Now we expect size of path_infos not change, don't acquire hevay lock on `path_infos` now.
    path_infos[path_idx].used_bytes += used_bytes;
}

void PathCapacityMetrics::freeUsedSize(const std::string & file_path, size_t used_bytes)
{
    ssize_t path_idx = locatePath(file_path);
    if (path_idx == INVALID_INDEX)
    {
        LOG_ERROR(log, "Can not locate path in removeUsedSize. File: " + file_path);
        return;
    }

    // Now we expect size of path_infos not change, don't acquire hevay lock on `path_infos` now.
    path_infos[path_idx].used_bytes -= used_bytes;
}

FsStats PathCapacityMetrics::getFsStats() const
{
    // Now we expect size of path_infos not change, don't acquire hevay lock on `path_infos` now.
    FsStats total_stat;
    double max_used_rate = 0.0;
    std::optional<uint64_t> first_avali_size = 0;
    for (size_t i = 0; i < path_infos.size(); ++i)
    {
        FsStats path_stat = path_infos[i].getStats(log);
        if (!path_stat.ok)
            return total_stat;

        // sum of all path's capacity
        total_stat.capacity_size += path_stat.capacity_size;

        max_used_rate = std::max(max_used_rate, 1.0 * path_stat.used_size / path_stat.capacity_size);
        if (!first_avali_size)
            first_avali_size = path_stat.avail_size;
    }

    // appromix used size, make pd happy
    // all capacity * max used rate
    total_stat.used_size = total_stat.capacity_size * max_used_rate;

    // appromix avali size
    total_stat.avail_size = total_stat.capacity_size - total_stat.used_size;
    total_stat.avail_size = std::min(total_stat.avail_size, *first_avali_size);

    total_stat.ok = 1;

    return total_stat;
}

// Return the index of the longest prefix matching path in `path_info`
ssize_t PathCapacityMetrics::locatePath(const std::string & file_path) const
{
    // TODO: maybe build a trie-tree to do this.
    ssize_t max_match_size = 0;
    ssize_t max_match_index = INVALID_INDEX;
    for (size_t i = 0; i < path_infos.size(); ++i)
    {
        const auto & path_info = path_infos[i];
        ssize_t match_size = 0;
        for (size_t str_idx = 0; str_idx < path_info.path.size() && str_idx < file_path.size(); ++str_idx)
        {
            if (path_info.path[str_idx] != file_path[str_idx])
            {
                match_size = str_idx - 1;
                break;
            }
        }
        if (max_match_size < match_size)
        {
            max_match_size = match_size;
            max_match_index = i;
        }
    }
    return max_match_index;
}

FsStats PathCapacityMetrics::CapacityInfo::getStats(Poco::Logger * log) const
{
    FsStats res;
    // Similar to `handle_store_heartbeat` in TiKV release-4.0 branch
    // https://github.com/tikv/tikv/blob/f14e8288f3/components/raftstore/src/store/worker/pd.rs#L593
    struct statvfs vfs;
    if (int code = statvfs(path.c_str(), &vfs); code != 0)
    {
        LOG_ERROR(log, "Could not calculate avaliable disk space (statvfs) of path: " << path << ", errno: " << errno);
        return res;
    }

    // capacity
    uint64_t capacity = 0;
    const uint64_t disk_capacity_size = vfs.f_blocks * vfs.f_frsize;
    if (capacity_bytes == 0 || disk_capacity_size < capacity)
        capacity = disk_capacity_size;
    else
        capacity = capacity_bytes;
    res.capacity_size = capacity;

    // used
    res.used_size = used_bytes.load();

    // avali
    uint64_t avali = 0;
    if (capacity > res.used_size)
        avali = capacity - res.used_size;
    else
        LOG_WARNING(log,
            "No avaliable space for path: " << path << ", capacity: " << formatReadableSizeWithBinarySuffix(capacity) //
                                            << ", used: " << formatReadableSizeWithBinarySuffix(used_bytes));

    const uint64_t disk_free_bytes = vfs.f_bavail * vfs.f_frsize;
    if (avali > disk_free_bytes)
        avali = disk_free_bytes;
    res.avail_size = avali;

    return res;
}


} // namespace DB
