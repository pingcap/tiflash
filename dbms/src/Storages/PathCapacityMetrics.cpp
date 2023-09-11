// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Core/Types.h>
#include <IO/WriteHelpers.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/PathCapacityMetrics.h>
#include <Storages/S3/S3Common.h>
#include <common/logger_useful.h>
#include <sys/statvfs.h>

#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace CurrentMetrics
{
extern const Metric StoreSizeCapacity;
extern const Metric StoreSizeAvailable;
extern const Metric StoreSizeUsed;
extern const Metric StoreSizeUsedRemote;
} // namespace CurrentMetrics


namespace DB
{
inline size_t safeGetQuota(const std::vector<size_t> & quotas, size_t idx)
{
    return idx < quotas.size() ? quotas[idx] : 0;
}

PathCapacityMetrics::PathCapacityMetrics(
    const size_t capacity_quota_, // will be ignored if `main_capacity_quota` is not empty
    const Strings & main_paths_,
    const std::vector<size_t> & main_capacity_quota_,
    const Strings & latest_paths_,
    const std::vector<size_t> & latest_capacity_quota_,
    const Strings & remote_cache_paths,
    const std::vector<size_t> & remote_cache_capacity_quota_)
    : capacity_quota(capacity_quota_)
    , log(Logger::get())
{
    if (!main_capacity_quota_.empty())
    {
        // The `capacity_quota_` is left for backward compatibility.
        // If `main_capacity_quota_` is not empty, use the capacity for each path instead of global capacity.
        capacity_quota = 0;
    }

    // Get unique <path, capacity> from `main_paths_` && `latest_paths_`
    std::map<String, size_t> all_paths;
    for (size_t i = 0; i < main_paths_.size(); ++i)
    {
        all_paths[main_paths_[i]] = safeGetQuota(main_capacity_quota_, i);
    }
    for (size_t i = 0; i < latest_paths_.size(); ++i)
    {
        if (auto iter = all_paths.find(latest_paths_[i]); iter != all_paths.end())
        {
            // If we have set a unlimited quota for this path, then we keep it limited, else use the bigger quota
            if (iter->second == 0 || safeGetQuota(latest_capacity_quota_, i) == 0)
                iter->second = 0;
            else
                iter->second = std::max(iter->second, safeGetQuota(latest_capacity_quota_, i));
        }
        else
        {
            all_paths[latest_paths_[i]] = safeGetQuota(latest_capacity_quota_, i);
        }
    }
    for (size_t i = 0; i < remote_cache_paths.size(); ++i)
    {
        all_paths[remote_cache_paths[i]] = safeGetQuota(remote_cache_capacity_quota_, i);
    }

    for (auto && [path, quota] : all_paths)
    {
        LOG_INFO(log, "Init capacity [path={}] [capacity={}]", path, formatReadableSizeWithBinarySuffix(quota));
        path_infos.emplace_back(CapacityInfo{path, quota});
    }
}

void PathCapacityMetrics::addUsedSize(std::string_view file_path, size_t used_bytes)
{
    ssize_t path_idx = locatePath(file_path);
    if (path_idx == INVALID_INDEX)
    {
        LOG_ERROR(log, "Can not locate path in addUsedSize. File: {}", file_path);
        return;
    }

    // Now we expect size of path_infos not change, don't acquire heavy lock on `path_infos` now.
    path_infos[path_idx].used_bytes += used_bytes;
}

void PathCapacityMetrics::freeUsedSize(std::string_view file_path, size_t used_bytes)
{
    ssize_t path_idx = locatePath(file_path);
    if (path_idx == INVALID_INDEX)
    {
        LOG_ERROR(log, "Can not locate path in removeUsedSize. File: {}", file_path);
        return;
    }

    // Now we expect size of path_infos not change, don't acquire heavy lock on `path_infos` now.
    path_infos[path_idx].used_bytes -= used_bytes;
}

void PathCapacityMetrics::addRemoteUsedSize(KeyspaceID keyspace_id, size_t used_bytes)
{
    if (used_bytes == 0)
        return;
    std::unique_lock<std::mutex> lock(mutex);
    auto iter = keyspace_id_to_used_bytes.emplace(keyspace_id, 0);
    iter.first->second += used_bytes;
}

void PathCapacityMetrics::freeRemoteUsedSize(KeyspaceID keyspace_id, size_t used_bytes)
{
    std::unique_lock<std::mutex> lock(mutex);
    auto iter = keyspace_id_to_used_bytes.find(keyspace_id);
    RUNTIME_CHECK(iter != keyspace_id_to_used_bytes.end(), keyspace_id);
    iter->second -= used_bytes;
    RUNTIME_CHECK_MSG(
        iter->second >= 0,
        "Remote size {} is invalid after remove {} bytes for keyspace {}",
        iter->second,
        used_bytes,
        keyspace_id);
    if (iter->second == 0)
        keyspace_id_to_used_bytes.erase(iter);
}

std::unordered_map<KeyspaceID, UInt64> PathCapacityMetrics::getKeyspaceUsedSizes()
{
    size_t local_toal_size = 0;
    for (auto & path_info : path_infos)
    {
        local_toal_size += path_info.used_bytes;
    }

    std::unordered_map<KeyspaceID, UInt64> keyspace_id_to_total_size;

    std::unique_lock<std::mutex> lock(mutex);

    size_t remote_total_size = 0;
    for (auto & [keyspace_id, size] : keyspace_id_to_used_bytes)
    {
        remote_total_size += size;
    }

    for (auto & [keyspace_id, size] : keyspace_id_to_used_bytes)
    {
        // cannot get accurate local used size for each keyspace, so we use a simple way to estimate it.
        keyspace_id_to_total_size.emplace(
            keyspace_id,
            size + static_cast<size_t>(size * 1.0 / remote_total_size * local_toal_size));
    }
    return keyspace_id_to_total_size;
}

std::map<FSID, DiskCapacity> PathCapacityMetrics::getDiskStats()
{
    std::map<FSID, DiskCapacity> disk_stats_map;
    for (auto & path_info : path_infos)
    {
        auto [path_stat, vfs] = path_info.getStats(log);
        if (!path_stat.ok)
        {
            // Disk may be hot remove, Ignore this disk.
            continue;
        }

        auto entry = disk_stats_map.find(vfs.f_fsid);
        if (entry == disk_stats_map.end())
        {
            disk_stats_map.insert(std::pair<FSID, DiskCapacity>(vfs.f_fsid, {vfs, {path_stat}}));
        }
        else
        {
            entry->second.path_stats.emplace_back(path_stat);
        }
    }
    return disk_stats_map;
}

FsStats PathCapacityMetrics::getFsStats(bool finalize_capacity)
{
    // Now we assume the size of `path_infos` will not change, don't acquire heavy lock on `path_infos`.
    FsStats total_stat{};

    // Build the disk stats map
    // which use to measure single disk capacity and available size
    auto disk_stats_map = getDiskStats();

    for (auto & fs_it : disk_stats_map)
    {
        FsStats disk_stat{};

        auto & disk_stat_vec = fs_it.second;
        auto & vfs_info = disk_stat_vec.vfs_info;

        for (const auto & single_path_stats : disk_stat_vec.path_stats)
        {
            disk_stat.capacity_size += single_path_stats.capacity_size;
            disk_stat.used_size += single_path_stats.used_size;
            disk_stat.avail_size += single_path_stats.avail_size;
        }

        const uint64_t disk_capacity_size = vfs_info.f_blocks * vfs_info.f_frsize;
        if (disk_stat.capacity_size == 0 || disk_capacity_size < disk_stat.capacity_size)
            disk_stat.capacity_size = disk_capacity_size;

        // Calculate single disk info
        const uint64_t disk_free_bytes = vfs_info.f_bavail * vfs_info.f_frsize;
        disk_stat.avail_size = std::min(disk_free_bytes, disk_stat.avail_size);

        // sum of all path's capacity and used_size
        total_stat.capacity_size += disk_stat.capacity_size;
        total_stat.used_size += disk_stat.used_size;
        total_stat.avail_size += disk_stat.avail_size;
    }

    // If user set quota on the global quota, set the capacity to the quota.
    if (capacity_quota != 0 && capacity_quota < total_stat.capacity_size)
    {
        total_stat.capacity_size = capacity_quota;
        total_stat.avail_size = std::min(total_stat.avail_size, total_stat.capacity_size - total_stat.used_size);
    }
    // PD get weird if used_size == 0, make it 1 byte at least
    total_stat.used_size = std::max<UInt64>(1, total_stat.used_size);

    const double avail_rate = 1.0 * total_stat.avail_size / total_stat.capacity_size;
    // Default threshold "schedule.low-space-ratio" in PD is 0.8, log warning message if avail ratio is low.
    if (avail_rate <= 0.2)
        LOG_WARNING(
            log,
            "Available space is only {:.2f}% of capacity size. Avail size: {}, used size: {}, capacity size: {}",
            avail_rate * 100.0,
            formatReadableSizeWithBinarySuffix(total_stat.avail_size),
            formatReadableSizeWithBinarySuffix(total_stat.used_size),
            formatReadableSizeWithBinarySuffix(total_stat.capacity_size));

    // Just report local disk capacity and available size is enough
    CurrentMetrics::set(CurrentMetrics::StoreSizeCapacity, total_stat.capacity_size);
    CurrentMetrics::set(CurrentMetrics::StoreSizeAvailable, total_stat.avail_size);
    CurrentMetrics::set(CurrentMetrics::StoreSizeUsed, total_stat.used_size);

    size_t remote_used_size = 0;
    if (finalize_capacity && S3::ClientFactory::instance().isEnabled())
    {
        {
            std::unique_lock<std::mutex> lock(mutex);
            for (const auto & [keyspace_id, used_bytes] : keyspace_id_to_used_bytes)
            {
                UNUSED(keyspace_id);
                remote_used_size += used_bytes;
            }
        }
    }
    CurrentMetrics::set(CurrentMetrics::StoreSizeUsedRemote, remote_used_size);

    total_stat.ok = 1;

    return total_stat;
}

std::tuple<FsStats, struct statvfs> PathCapacityMetrics::getFsStatsOfPath(std::string_view file_path) const
{
    ssize_t path_idx = locatePath(file_path);
    if (unlikely(path_idx == INVALID_INDEX))
    {
        LOG_ERROR(log, "Can not locate path in getFsStatsOfPath. File: {}", file_path);
        return {FsStats{}, {}};
    }

    return path_infos[path_idx].getStats(nullptr);
}

// Return the index of the longest prefix matching path in `path_info`
ssize_t PathCapacityMetrics::locatePath(std::string_view file_path) const
{
    // TODO: maybe build a trie-tree to do this.
    ssize_t max_match_size = 0;
    ssize_t max_match_index = INVALID_INDEX;
    for (size_t i = 0; i < path_infos.size(); ++i)
    {
        const auto & path_info = path_infos[i];
        const auto max_length_compare = std::min(path_info.path.size(), file_path.size());
        ssize_t match_size = 0;
        for (size_t str_idx = 0; str_idx <= max_length_compare; ++str_idx)
        {
            if (str_idx == max_length_compare)
            {
                match_size = str_idx - 1;
                break;
            }
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

std::tuple<FsStats, struct statvfs> PathCapacityMetrics::CapacityInfo::getStats(const LoggerPtr & log) const
{
    FsStats res{};
    /// Get capacity, used, available size for one path.
    /// Similar to `handle_store_heartbeat` in TiKV release-4.0 branch
    /// https://github.com/tikv/tikv/blob/f14e8288f3/components/raftstore/src/store/worker/pd.rs#L593
    struct statvfs vfs
    {
    };
    if (int code = statvfs(path.data(), &vfs); code != 0)
    {
        if (log)
        {
            LOG_ERROR(log, "Could not calculate available disk space (statvfs) of path: {}, errno: {}", path, errno);
        }
        return {};
    }

    // capacity is limited by the actual disk capacity
    uint64_t capacity = 0;
    const uint64_t disk_capacity_size = vfs.f_blocks * vfs.f_frsize;
    if (capacity_bytes == 0 || disk_capacity_size < capacity_bytes)
        capacity = disk_capacity_size;
    else
        capacity = capacity_bytes;
    res.capacity_size = capacity;

    // used
    res.used_size = used_bytes.load();

    // avail
    uint64_t avail = 0;
    if (capacity > res.used_size)
        avail = capacity - res.used_size;
    else if (log)
        LOG_WARNING(
            log,
            "No available space for path: {}, capacity: {}, used: {}",
            path,
            formatReadableSizeWithBinarySuffix(capacity),
            formatReadableSizeWithBinarySuffix(used_bytes));

    const uint64_t disk_free_bytes = vfs.f_bavail * vfs.f_frsize;
    if (avail > disk_free_bytes)
        avail = disk_free_bytes;
    res.avail_size = avail;
    res.ok = 1;

    return {res, vfs};
}

} // namespace DB
