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

#include <Common/Logger.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore_fwd.h>
#include <common/types.h>
#include <fmt/format.h>

#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace DB::PS::V3
{


using RemoteFileValidSizes = std::unordered_map<String, size_t>;

struct CPDataFileStat
{
    Int64 valid_size = 0;
    // total_size < 0 indicate this value is not inited
    Int64 total_size = -1;
    // last_modification_time on remote store
    std::chrono::system_clock::time_point mtime;
};

class CPDataFilesStatCache
{
public:
    // file_id -> CPDataFileStat
    using CacheMap = std::unordered_map<String, CPDataFileStat>;

    /**
     * Update the valid size in the cache. Thread safe.
     *
     * If some file_id exist in `valid_sizes` but not exist in the cache, it
     * will create a new `CPDataFileStat` with total_size == 0.
     * If some file_id exist in the cache but not exist in the `valid_sizes`,
     * it means the data file is not owned by this node any more, so it will
     * be removed from the cache.
     */
    void updateValidSize(const RemoteFileValidSizes & valid_sizes);

    /**
     * Update the total size field in the cache. Thread safe.
     */
    void updateCache(const CacheMap & total_sizes);

    CacheMap getCopy() const
    {
        std::lock_guard lock(mtx);
        return stats;
    }

private:
    mutable std::mutex mtx;
    CacheMap stats;
};

std::unordered_set<String> getRemoteFileIdsNeedCompact(
    PS::V3::CPDataFilesStatCache::CacheMap & stats,
    const DB::DM::Remote::RemoteGCThreshold & gc_threshold,
    const DB::DM::Remote::IDataStorePtr & remote_store,
    const LoggerPtr & log);

} // namespace DB::PS::V3

template <>
struct fmt::formatter<DB::PS::V3::CPDataFileStat>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::PS::V3::CPDataFileStat & value, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return fmt::format_to(ctx.out(), "valid:{},total:{}", value.valid_size, value.total_size);
    }
};
