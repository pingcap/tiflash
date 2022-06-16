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

struct FsStats;

class PathCapacityMetrics : private boost::noncopyable
{
public:
    PathCapacityMetrics(const size_t capacity_quota_, // will be ignored if `main_capacity_quota` is not empty
        const Strings & main_paths_, const std::vector<size_t> main_capacity_quota_, //
        const Strings & latest_paths_, const std::vector<size_t> latest_capacity_quota_);

    void addUsedSize(std::string_view file_path, size_t used_bytes);

    void freeUsedSize(std::string_view file_path, size_t used_bytes);

    FsStats getFsStats() const;

    FsStats getFsStatsOfPath(std::string_view file_path) const;

private:
    static constexpr ssize_t INVALID_INDEX = -1;
    // Return the index of the longest prefix matching path in `path_info`
    ssize_t locatePath(std::string_view file_path) const;

private:
    struct CapacityInfo
    {
        std::string path;
        // Max quota bytes can be use for this path
        const uint64_t capacity_bytes = 0;
        // Used bytes for this path
        std::atomic<uint64_t> used_bytes = 0;

        FsStats getStats(Poco::Logger * log) const;

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
