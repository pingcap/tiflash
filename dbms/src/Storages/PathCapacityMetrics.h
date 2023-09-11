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

#pragma once
#include <Core/Types.h>
#include <Storages/KVStore/Types.h>
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

class PathCapacityMetrics : private boost::noncopyable
{
public:
    PathCapacityMetrics(
        size_t capacity_quota_, // will be ignored if `main_capacity_quota` is not empty
        const Strings & main_paths_,
        const std::vector<size_t> & main_capacity_quota_, //
        const Strings & latest_paths_,
        const std::vector<size_t> & latest_capacity_quota_,
        const Strings & remote_cache_paths = {},
        const std::vector<size_t> & remote_cache_capacity_quota_ = {});

    virtual ~PathCapacityMetrics() = default;

    void addUsedSize(std::string_view file_path, size_t used_bytes);

    void freeUsedSize(std::string_view file_path, size_t used_bytes);

    void addRemoteUsedSize(KeyspaceID keyspace_id, size_t used_bytes);

    void freeRemoteUsedSize(KeyspaceID keyspace_id, size_t used_bytes);

    std::unordered_map<KeyspaceID, UInt64> getKeyspaceUsedSizes();

    FsStats getFsStats(bool finalize_capacity = true);

    virtual std::map<FSID, DiskCapacity> getDiskStats();

    std::tuple<FsStats, struct statvfs> getFsStatsOfPath(std::string_view file_path) const;

#ifndef DBMS_PUBLIC_GTEST
private:
#endif

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

        std::tuple<FsStats, struct statvfs> getStats(const LoggerPtr & log) const;

        CapacityInfo() = default;
        CapacityInfo(String p, uint64_t c)
            : path(std::move(p))
            , capacity_bytes(c)
        {}
        CapacityInfo(const CapacityInfo & rhs)
            : path(rhs.path)
            , capacity_bytes(rhs.capacity_bytes)
            , used_bytes(rhs.used_bytes.load())
        {}
    };

    // Max quota bytes can be use for this TiFlash instance.
    // 0 means no quota, use the whole disk.
    size_t capacity_quota;
    std::vector<CapacityInfo> path_infos;

    // Used to protect `keyspace_id_to_used_bytes`
    std::mutex mutex;
    std::unordered_map<KeyspaceID, UInt64> keyspace_id_to_used_bytes;
    LoggerPtr log;
};

} // namespace DB
