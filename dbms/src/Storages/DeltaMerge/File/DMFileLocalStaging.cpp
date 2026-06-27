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

#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileLocalStaging.h>
#include <Storages/DeltaMerge/File/DMFileMetaV2.h>
#include <Storages/DeltaMerge/File/DMFileUtil.h>
#include <Storages/S3/S3Filename.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>

#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace DB::DM
{
namespace
{
std::optional<UInt64> getMergedFileSize(const DMFileMetaV2 & dmfile_meta, UInt32 number)
{
    for (const auto & merged_file : dmfile_meta.merged_files)
    {
        if (merged_file.number == number)
            return merged_file.size;
    }
    return std::nullopt;
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
    // MetaV2 stores many logical files inside physical `.merged` blobs; this set is
    // the per-column/substream view before resolving to merged file numbers.
    std::unordered_set<String> logical_filenames;
    for (const auto & cd : read_columns)
    {
        if (!dmfile->isColumnExist(cd.id))
            continue;

        const auto data_type = dmfile->getColumnStat(cd.id).type;
        data_type->enumerateStreams(
            [&](const IDataType::SubstreamPath & substream) {
                const auto stream_name = DMFile::getFileNameBase(cd.id, substream);
                logical_filenames.insert(colDataFileName(stream_name));
                logical_filenames.insert(colMarkFileName(stream_name));
            },
            {});
    }

    // Physical `.merged` S3 objects to stage locally, keyed by full S3 key.
    // Multiple logical subfiles may map to the same merged blob; dedup here so
    // each object is downloaded at most once.
    std::unordered_map<String, LocalReadObject> objects_by_key;
    for (const auto & logical_filename : logical_filenames)
    {
        const auto info_iter = dmfile_meta->merged_sub_file_infos.find(logical_filename);
        if (info_iter == dmfile_meta->merged_sub_file_infos.end())
        {
            // Standalone column subfile (not merged into `.merged`). Direct read for now.
            // TODO: collect `dmfile->colDataPath` / `colMarkPath` and pre-download via FileCache.
            LOG_DEBUG(
                log,
                "Skip local staging collection for unknown logical file, tracing_id={} dmfile={} logical_file={}",
                tracing_id,
                dmfile->parentPath(),
                logical_filename);
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
        const auto s3_fname = S3::S3FilenameView::fromKeyWithPrefix(merged_path);
        if (!s3_fname.isValid())
        {
            LOG_DEBUG(
                log,
                "Skip local staging collection for non-S3 merged path, tracing_id={} dmfile={} logical_file={} path={}",
                tracing_id,
                dmfile->parentPath(),
                logical_filename,
                merged_path);
            continue;
        }

        objects_by_key.emplace(
            s3_fname.toFullKey(),
            LocalReadObject{
                .s3_key = s3_fname.toFullKey(),
                .file_size = merged_file_size.value(),
            });
    }

    std::vector<LocalReadObject> objects;
    objects.reserve(objects_by_key.size());
    for (auto & [_, object] : objects_by_key)
        objects.emplace_back(std::move(object));
    return objects;
}

} // namespace DB::DM
