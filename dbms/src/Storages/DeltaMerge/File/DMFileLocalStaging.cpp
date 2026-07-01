// Copyright 2026 PingCAP, Inc.
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
#include <Common/typeid_cast.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileLocalStaging.h>
#include <Storages/DeltaMerge/File/DMFileMetaV2.h>
#include <Storages/DeltaMerge/File/DMFileUtil.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/S3Filename.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB::DM
{
namespace
{
struct LogicalReadFile
{
    ColId col_id;
    String filename;
};

std::optional<UInt64> getMergedFileSize(const DMFileMetaV2 & dmfile_meta, UInt32 number)
{
    for (const auto & merged_file : dmfile_meta.merged_files)
    {
        if (merged_file.number == number)
            return merged_file.size;
    }
    return std::nullopt;
}

void tryCollectS3Object(
    std::unordered_map<String, LocalReadObject> & objects_by_key,
    const String & file_path,
    UInt64 file_size,
    const DMFilePtr & dmfile,
    const LoggerPtr & log,
    const String & tracing_id,
    const String & logical_filename)
{
    if (file_size == 0)
        return;

    const auto s3_fname = S3::S3FilenameView::fromKeyWithPrefix(file_path);
    if (!s3_fname.isValid())
    {
        LOG_DEBUG(
            log,
            "Skip local staging collection for non-S3 path, tracing_id={} dmfile={} logical_file={} path={}",
            tracing_id,
            dmfile->parentPath(),
            logical_filename,
            file_path);
        return;
    }

    objects_by_key.emplace(
        s3_fname.toFullKey(),
        LocalReadObject{
            .s3_key = s3_fname.toFullKey(),
            .file_size = file_size,
        });
}
} // namespace

std::vector<LocalReadObject> collectMetaV2MergedFilesForLocalRead(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    const LoggerPtr & log,
    const String & tracing_id)
{
    if (!dmfile->useMetaV2())
        return {};

    const auto * dmfile_meta = typeid_cast<const DMFileMetaV2 *>(dmfile->meta.get());
    if (dmfile_meta == nullptr)
        return {};

    // Logical subfile names (e.g. `c1.dat`, `c1.mrk`) required by read_columns.
    // MetaV2 stores many logical files inside physical `.merged` blobs; this list is
    // the per-column/substream view before resolving to merged file numbers or standalone paths.
    std::vector<LogicalReadFile> logical_files;
    for (const auto & cd : read_columns)
    {
        if (!dmfile->isColumnExist(cd.id))
            continue;

        const auto data_type = dmfile->getColumnStat(cd.id).type;
        data_type->enumerateStreams(
            [&](const IDataType::SubstreamPath & substream) {
                const auto stream_name = DMFile::getFileNameBase(cd.id, substream);
                logical_files.push_back({cd.id, colDataFileName(stream_name)});
                logical_files.push_back({cd.id, colMarkFileName(stream_name)});
            },
            {});
    }

    // Physical S3 objects to stage locally, keyed by full S3 key.
    // Multiple logical subfiles may map to the same merged blob; dedup here so
    // each object is downloaded at most once.
    std::unordered_map<String, LocalReadObject> objects_by_key;
    for (const auto & logical_file : logical_files)
    {
        const auto & logical_filename = logical_file.filename;
        const auto info_iter = dmfile_meta->merged_sub_file_infos.find(logical_filename);
        if (info_iter == dmfile_meta->merged_sub_file_infos.end())
        {
            // Large column subfiles (e.g. `.dat`) are kept as standalone S3 objects when they
            // exceed `small_file_size_threshold`; only their marks/indexes may live in `.merged`.
            const auto file_path = dmfile->subFilePath(logical_filename);
            const auto file_size = dmfile->getReadFileSize(logical_file.col_id, logical_filename);
            tryCollectS3Object(objects_by_key, file_path, file_size, dmfile, log, tracing_id, logical_filename);
            continue;
        }

        const auto & merged_file_info = info_iter->second;
        const auto merged_file_size = getMergedFileSize(*dmfile_meta, merged_file_info.number);
        if (!merged_file_size.has_value())
        {
            LOG_DEBUG(
                log,
                "Skip local staging collection for unknown merged file, tracing_id={} dmfile={} logical_file={} "
                "merged_number={}",
                tracing_id,
                dmfile->parentPath(),
                logical_filename,
                merged_file_info.number);
            continue;
        }

        const auto merged_path = dmfile_meta->mergedPath(merged_file_info.number);
        tryCollectS3Object(
            objects_by_key,
            merged_path,
            merged_file_size.value(),
            dmfile,
            log,
            tracing_id,
            logical_filename);
    }

    std::vector<LocalReadObject> objects;
    objects.reserve(objects_by_key.size());
    for (auto & [_, object] : objects_by_key)
        objects.emplace_back(std::move(object));
    return objects;
}

namespace
{
constexpr Int32 local_staging_download_retry_count = 3;
} // namespace

std::vector<FileSegmentPtr> tryDownloadMetaV2MergedFilesForLocalRead(
    const DMFilePtr & dmfile,
    const ColumnDefines & read_columns,
    bool enable_write_filecache_local_read,
    const LoggerPtr & log,
    const String & tracing_id)
{
    if (!enable_write_filecache_local_read)
        return {};

    auto * file_cache = FileCache::instance();
    if (file_cache == nullptr)
        return {};

    const auto objects = collectMetaV2MergedFilesForLocalRead(dmfile, read_columns, log, tracing_id);
    if (objects.empty())
        return {};

    GET_METRIC(tiflash_storage_write_filecache_staging, type_attempt).Increment();
    GET_METRIC(tiflash_storage_write_filecache_staging, type_object).Increment(objects.size());

    std::vector<FileSegmentPtr> local_read_files;
    local_read_files.reserve(objects.size());

    size_t downloaded_count = 0;
    size_t failed_count = 0;
    for (const auto & object : objects)
    {
        const auto s3_fname = S3::S3FilenameView::fromKey(object.s3_key);
        if (!s3_fname.isValid())
        {
            ++failed_count;
            LOG_DEBUG(
                log,
                "Skip write FileCache local staging for invalid S3 key, tracing_id={} dmfile={} key={}",
                tracing_id,
                dmfile->parentPath(),
                object.s3_key);
            continue;
        }

        try
        {
            auto [file_seg, has_s3_download] = file_cache->downloadFileForLocalReadWithRetry(
                s3_fname,
                object.file_size,
                local_staging_download_retry_count);
            (void)has_s3_download;
            if (!file_seg)
            {
                ++failed_count;
                LOG_WARNING(
                    log,
                    "Write FileCache local staging download returned empty segment, tracing_id={} dmfile={} key={}",
                    tracing_id,
                    dmfile->parentPath(),
                    object.s3_key);
                continue;
            }
            local_read_files.emplace_back(std::move(file_seg));
            GET_METRIC(tiflash_storage_write_filecache_staging, type_download_ok).Increment();
            // Cumulative bytes of physical objects successfully staged (metadata file_size).
            // Counter only increases; reader pin release does not decrement it. For live FileCache
            // occupancy, use tiflash_storage_remote_cache_bytes instead.
            GET_METRIC(tiflash_storage_write_filecache_staging_bytes, type_staged).Increment(object.file_size);
            ++downloaded_count;
        }
        catch (...)
        {
            ++failed_count;
            tryLogCurrentException(
                log,
                fmt::format(
                    "Write FileCache local staging download failed, tracing_id={} dmfile={} key={}",
                    tracing_id,
                    dmfile->parentPath(),
                    object.s3_key));
        }
    }

    if (failed_count > 0)
        GET_METRIC(tiflash_storage_write_filecache_staging, type_download_failed).Increment(failed_count);

    LOG_DEBUG(
        log,
        "Write FileCache local staging finished, tracing_id={} dmfile={} objects={} downloaded={} failed={}",
        tracing_id,
        dmfile->parentPath(),
        objects.size(),
        downloaded_count,
        failed_count);

    return local_read_files;
}

} // namespace DB::DM
