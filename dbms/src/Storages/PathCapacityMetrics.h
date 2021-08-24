#pragma once
#include <Core/Types.h>
#include <common/logger_useful.h>
#include <sys/statvfs.h>

#include <boost/noncopyable.hpp>
#include <string>
#include <vector>

namespace DB
{
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;
using FSID = UInt32;
struct FsStats;

struct DiskCapacity
{
    struct statvfs vfs_info = {};
    std::vector<FsStats> path_stats;
};

class DisksCapacity
{
public:
    struct DiskInfo
    {
        DiskCapacity disk;
        Strings paths;
    };

    void insert(const struct statvfs & vfs, const FsStats & fs_stat, const String & path);

    // Note that caller should never call `insert` after `getBiggestAvailableDisk`, or the
    // iterator of biggest available disk will be invalid.
    using Iterator = std::unordered_map<FSID, DiskInfo>::const_iterator;
    std::tuple<size_t, size_t, Iterator> getBiggestAvailableDisk() const;

    Iterator end() const { return disks_stats.end(); }

private:
    std::unordered_map<FSID, DiskInfo> disks_stats;
};

class PathCapacityMetrics : private boost::noncopyable
{
public:
    PathCapacityMetrics(const size_t capacity_quota_, // will be ignored if `main_capacity_quota` is not empty
        const Strings & main_paths_, const std::vector<size_t> main_capacity_quota_, //
        const Strings & latest_paths_, const std::vector<size_t> latest_capacity_quota_);

    virtual ~PathCapacityMetrics(){};

    void addUsedSize(std::string_view file_path, size_t used_bytes);

    void freeUsedSize(std::string_view file_path, size_t used_bytes);

    FsStats getFsStats();

    virtual std::map<FSID, DiskCapacity> getDiskStats();

    template <typename T>
    std::tuple<DisksCapacity, std::map<String, FsStats>> getDiskStatsForPaths(const std::vector<T> & paths)
    {
        DisksCapacity all_disks;
        std::map<String, FsStats> path_capacity;
        for (size_t i = 0; i < paths.size(); ++i)
        {
            auto [path_stat, vfs] = getFsStatsOfPath(paths[i].path);
            if (!path_stat.ok)
            {
                continue;
            }

            path_capacity[paths[i].path] = path_stat;

            // update all_disks
            all_disks.insert(vfs, path_stat, paths[i].path);
        }
        return {std::move(all_disks), std::move(path_capacity)};
    }

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    std::tuple<FsStats, struct statvfs> getFsStatsOfPath(std::string_view file_path) const;

    static constexpr ssize_t INVALID_INDEX = -1;
    // Return the index of the longest prefix matching path in `path_info`
    ssize_t locatePath(std::string_view file_path) const;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

    struct CapacityInfo
    {
        std::string path;
        // Max quota bytes can be use for this path
        const uint64_t capacity_bytes = 0;
        // Used bytes for this path
        std::atomic<uint64_t> used_bytes = 0;

        std::tuple<FsStats, struct statvfs> getStats(Poco::Logger * log) const;

        CapacityInfo() = default;
        CapacityInfo(String p, uint64_t c) : path(std::move(p)), capacity_bytes(c) {}
        CapacityInfo(const CapacityInfo & rhs) : path(rhs.path), capacity_bytes(rhs.capacity_bytes), used_bytes(rhs.used_bytes.load()) {}
    };

    // Max quota bytes can be use for this TiFlash instance.
    // 0 means no quota, use the whole disk.
    size_t capacity_quota;
    std::vector<CapacityInfo> path_infos;
    Poco::Logger * log;
};

} // namespace DB
