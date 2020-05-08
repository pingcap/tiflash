#pragma once
#include <Core/Types.h>
#include <Storages/Transaction/ProxyFFIType.h>
#include <common/logger_useful.h>
#include <sys/statvfs.h>

#include <boost/noncopyable.hpp>
#include <string>
#include <vector>

namespace DB
{
class PathCapacityMetrics;
using PathCapacityMetricsPtr = std::shared_ptr<PathCapacityMetrics>;

class PathCapacityMetrics : private boost::noncopyable
{
public:
    PathCapacityMetrics(const std::vector<std::string> & all_paths, const std::vector<size_t> & capacities);

    void addUsedSize(const std::string & file_path, size_t used_bytes);

    void freeUsedSize(const std::string & file_path, size_t used_bytes);

    FsStats getFsStats() const;

private:
    static constexpr ssize_t INVALID_INDEX = -1;
    // Return the index of the longest prefix matching path in `path_info`
    ssize_t locatePath(const std::string & file_path) const;

private:
    struct CapacityInfo
    {
        std::string path;
        // Max quota bytes can be use for this path
        std::atomic<uint64_t> capacity_bytes = 0;
        // Used bytes for this path
        std::atomic<uint64_t> used_bytes = 0;

        FsStats getStats(Poco::Logger * log) const;

        CapacityInfo() = default;
        CapacityInfo(const CapacityInfo & rhs)
            : path(rhs.path), capacity_bytes(rhs.capacity_bytes.load()), used_bytes(rhs.used_bytes.load())
        {}
    };

    std::vector<CapacityInfo> path_infos;
    Poco::Logger * log;
};

} // namespace DB
