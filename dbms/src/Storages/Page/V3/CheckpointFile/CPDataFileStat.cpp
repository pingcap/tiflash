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
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/Remote/DataStore/DataStore.h>
#include <Storages/Page/V3/CheckpointFile/CPDataFileStat.h>
#include <fmt/chrono.h>

#include <chrono>
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

void CPDataFilesStatCache::updateCache(const CPDataFilesStatCache::CacheMap & total_sizes)
{
    std::lock_guard lock(mtx);
    for (const auto & [file_id, new_stat] : total_sizes)
    {
        // Some file_ids may be erased before `updateCache`, but it is
        // safe to ignore them.
        // Ony update the file_ids that still valid is OK.
        if (auto iter = stats.find(file_id); iter != stats.end())
        {
            iter->second.total_size = new_stat.total_size;
            iter->second.mtime = new_stat.mtime;
        }
    }
}

struct FileInfo
{
    String file_id;
    double age_seconds = 0.0;
    Int64 total_size = 0;
    double valid_rate = 100.0;
};

struct RemoteFilesInfo
{
    struct Stats
    {
        size_t num_files = 0;
        size_t total_size = 0;
        size_t valid_size = 0;
    };

    std::unordered_set<String> getCompactCandidates()
    {
        std::unordered_set<String> candidates;
        for (const auto & f : to_compact)
        {
            candidates.insert(f.file_id);
        }
        return candidates;
    }

    void addUnchanged(FileInfo && f)
    {
        summary_stats.num_files += 1;
        summary_stats.total_size += f.total_size;
        summary_stats.valid_size += f.total_size * f.valid_rate;
        unchanged.emplace_back(f);
    }

    void addToCompact(FileInfo && f)
    {
        summary_stats.num_files += 1;
        summary_stats.total_size += f.total_size;
        summary_stats.valid_size += f.total_size * f.valid_rate;
        to_compact.emplace_back(f);
    }

    // private:
    std::vector<FileInfo> unchanged;
    std::vector<FileInfo> to_compact;

    Stats summary_stats;
};

std::unordered_set<String> getRemoteFileIdsNeedCompact(
    PS::V3::CPDataFilesStatCache::CacheMap & stats, // will be updated
    const DM::Remote::RemoteGCThreshold & gc_threshold,
    const DM::Remote::IDataStorePtr & remote_store,
    const LoggerPtr & log)
{
    {
        std::unordered_set<String> file_ids;
        // If the total_size less than 0, it means the size has not been fetch from
        // the remote store successfully, try to get the actual size from S3
        for (const auto & [file_id, stat] : stats)
        {
            if (stat.total_size < 0)
                file_ids.insert(file_id);
        }
        const auto files_info = remote_store->getDataFilesInfo(file_ids);
        for (const auto & [file_id, info] : files_info)
        {
            // IO error or data file not exist, just skip it
            if (info.size < 0)
            {
                continue;
            }
            auto iter = stats.find(file_id);
            RUNTIME_CHECK_MSG(iter != stats.end(), "file_id={} stats={}", file_id, stats);
            iter->second.total_size = info.size;
            iter->second.mtime = info.mtime;
        }
    }

    const auto now_timepoint = std::chrono::system_clock::now();

    RemoteFilesInfo remote_infos;
    for (const auto & [file_id, stat] : stats)
    {
        if (stat.total_size <= 0)
            continue;

        auto age_seconds
            = std::chrono::duration_cast<std::chrono::milliseconds>(now_timepoint - stat.mtime).count() / 1000.0;
        double valid_rate = 1.0 * stat.valid_size / stat.total_size;
        if (static_cast<Int64>(age_seconds) > gc_threshold.min_age_seconds
            && (valid_rate < gc_threshold.valid_rate
                || stat.total_size < static_cast<Int64>(gc_threshold.min_file_threshold)))
        {
            remote_infos.addToCompact(FileInfo{
                .file_id = file_id,
                .age_seconds = age_seconds,
                .total_size = stat.total_size,
                .valid_rate = valid_rate});
        }
        else
        {
            remote_infos.addUnchanged(FileInfo{
                .file_id = file_id,
                .age_seconds = age_seconds,
                .total_size = stat.total_size,
                .valid_rate = valid_rate});
        }
    }

    const auto summary = remote_infos.summary_stats;
    GET_METRIC(tiflash_storage_remote_stats, type_num_files).Set(summary.num_files);
    GET_METRIC(tiflash_storage_remote_stats, type_total_size).Set(summary.total_size);
    GET_METRIC(tiflash_storage_remote_stats, type_valid_size).Set(summary.valid_size);

    auto compact_files = remote_infos.getCompactCandidates();
    LOG_IMPL(
        log,
        (compact_files.empty() ? Poco::Message::PRIO_DEBUG : Poco::Message::PRIO_INFORMATION),
        "CheckpointData stats {} {}",
        remote_infos,
        gc_threshold);
    return compact_files;
}

} // namespace DB::PS::V3

template <>
struct fmt::formatter<DB::PS::V3::FileInfo>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::PS::V3::FileInfo & value, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return fmt::format_to(
            ctx.out(),
            "{{key={} age={:.3f} size={} rate={:2.2f}%}}",
            value.file_id,
            value.age_seconds,
            value.total_size,
            value.valid_rate * 100);
    }
};

template <>
struct fmt::formatter<DB::PS::V3::RemoteFilesInfo>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::PS::V3::RemoteFilesInfo & v, FormatContext & ctx) const -> decltype(ctx.out())
    {
        return fmt::format_to(ctx.out(), "{{compaction={} unchanged={}}}", v.to_compact, v.unchanged);
    }
};
