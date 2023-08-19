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

#include <Common/Exception.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/Page/V3/CheckpointFile/CPDataFileStat.h>

#include <unordered_set>

namespace DB::PS::V3
{

void CPDataFilesStatCache::updateValidSize(const RemoteFileValidSizes & valid_sizes)
{
    std::lock_guard lock(mtx);
    // If the file is not exist in the latest valid_sizes, then we can
    // remove the cache
    for (auto iter = stats.begin(); iter != stats.end(); /*empty*/)
    {
        const auto & file_id = iter->first;
        if (!valid_sizes.contains(file_id))
        {
            iter = stats.erase(iter);
        }
        else
        {
            ++iter;
        }
    }
    // If the file_id exists, write down the valid size, keep total_size unchanged.
    // Else create a cache entry to write down valid size but leave total_size < 0.
    for (const auto & [file_id, valid_size] : valid_sizes)
    {
        auto res = stats.try_emplace(file_id, CPDataFileStat{});
        res.first->second.valid_size = valid_size;
    }
}

void CPDataFilesStatCache::updateTotalSize(const CPDataFilesStatCache::CacheMap & total_sizes)
{
    std::lock_guard lock(mtx);
    for (const auto & [file_id, new_stat] : total_sizes)
    {
        // Some file_ids may be erased before `updateTotalSize`, but it is
        // safe to ignore them.
        // Ony update the file_ids that still valid is OK.
        if (auto iter = stats.find(file_id); iter != stats.end())
        {
            iter->second.total_size = new_stat.total_size;
        }
    }
}

struct FileInfo
{
    String file_id;
    Int64 total_size;
    double valid_rate;
};

std::unordered_set<String> getRemoteFileIdsNeedCompact(
    PS::V3::CPDataFilesStatCache::CacheMap & stats, // will be updated
    const DM::Remote::RemoteGCThreshold & gc_threshold,
    const DM::Remote::IDataStorePtr & remote_store,
    const LoggerPtr & log)
{
    {
        std::unordered_set<String> file_ids;
        // If the total_size less than 0, try to get the actual size from S3
        for (const auto & [file_id, stat] : stats)
        {
            if (stat.total_size < 0)
                file_ids.insert(file_id);
        }
        std::unordered_map<String, Int64> file_sizes = remote_store->getDataFileSizes(file_ids);
        for (auto & [file_id, actual_size] : file_sizes)
        {
            // IO error or data file not exist, just skip it
            if (actual_size < 0)
            {
                continue;
            }
            auto iter = stats.find(file_id);
            RUNTIME_CHECK_MSG(iter != stats.end(), "file_id={} stats={}", file_id, stats);
            iter->second.total_size = actual_size;
        }
    }

    std::unordered_set<String> rewrite_files;
    std::vector<FileInfo> rewrite_files_info;
    std::vector<FileInfo> remain_files_info;
    for (const auto & [file_id, stat] : stats)
    {
        if (stat.total_size <= 0)
            continue;
        double valid_rate = 1.0 * stat.valid_size / stat.total_size;
        if (valid_rate < gc_threshold.valid_rate
            || stat.total_size < static_cast<Int64>(gc_threshold.min_file_threshold))
        {
            rewrite_files.insert(file_id);
            rewrite_files_info.emplace_back(FileInfo{.file_id = file_id, .total_size = stat.total_size, .valid_rate = valid_rate});
        }
        else
        {
            remain_files_info.emplace_back(FileInfo{.file_id = file_id, .total_size = stat.total_size, .valid_rate = valid_rate});
        }
    }
    LOG_IMPL(
        log,
        (rewrite_files_info.empty() ? Poco::Message::PRIO_DEBUG : Poco::Message::PRIO_INFORMATION),
        "CheckpointData pick for compaction={} unchanged={}",
        rewrite_files_info,
        remain_files_info);
    return rewrite_files;
}

} // namespace DB::PS::V3

template <>
struct fmt::formatter<DB::PS::V3::FileInfo>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::PS::V3::FileInfo & value, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return format_to(ctx.out(), "{{key={} size={} rate={:2.2f}%}}", value.file_id, value.total_size, value.valid_rate * 100);
    }
};
