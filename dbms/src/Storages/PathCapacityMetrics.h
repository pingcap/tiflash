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
    PathCapacityMetrics(const std::vector<std::string> & all_paths, const size_t capacity_quota_);

    void addUsedSize(std::string_view file_path, size_t used_bytes);

    void freeUsedSize(std::string_view file_path, size_t used_bytes);

    FsStats getFsStats() const;

private:
    static constexpr ssize_t INVALID_INDEX = -1;
    // Return the index of the longest prefix matching path in `path_info`
    ssize_t locatePath(std::string_view file_path) const;

private:
    struct CapacityInfo
    {
        std::string path;
        // Used bytes for this path
        std::atomic<uint64_t> used_bytes = 0;

        FsStats getStats(Poco::Logger * log) const;

        CapacityInfo() = default;
        CapacityInfo(const CapacityInfo & rhs)
            : path(rhs.path), used_bytes(rhs.used_bytes.load())
        {}
    };

    // Max quota bytes can be use for this TiFlash instance.
    // 0 means no quota, use the whole disk.
    size_t capacity_quota;
    std::vector<CapacityInfo> path_infos;
    Poco::Logger * log;
};

} // namespace DB
