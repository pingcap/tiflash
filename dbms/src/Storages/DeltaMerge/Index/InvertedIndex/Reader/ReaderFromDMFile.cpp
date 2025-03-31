// Copyright 2025 PingCAP, Inc.
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

#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/Filter/ColumnRange.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader.h>
#include <Storages/DeltaMerge/Index/InvertedIndex/Reader/ReaderFromDMFile.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/FileCachePerf.h>


namespace DB::ErrorCodes
{
extern const int S3_ERROR;
} // namespace DB::ErrorCodes

namespace DB::DM
{

InvertedIndexReaderFromDMFile::InvertedIndexReaderFromDMFile(
    const ColumnRangePtr & column_range_,
    const DMFilePtr & dmfile_,
    const LocalIndexCachePtr & local_index_cache_)
    : dmfile(dmfile_)
    , column_range(column_range_)
    , local_index_cache(local_index_cache_)
{
    GET_METRIC(tiflash_inverted_index_active_instances, type_disk_reader).Increment();
}

InvertedIndexReaderFromDMFile::~InvertedIndexReaderFromDMFile()
{
    GET_METRIC(tiflash_inverted_index_active_instances, type_disk_reader).Decrement();
}

BitmapFilterPtr InvertedIndexReaderFromDMFile::load()
{
    RUNTIME_CHECK(!loaded);

    auto sorted_results = column_range->check(
        [&](const SingleColumnRangePtr & column_range) { return load(column_range); },
        dmfile->getRows());

    loaded = true;
    return sorted_results;
}

BitmapFilterPtr InvertedIndexReaderFromDMFile::load(const SingleColumnRangePtr & column_range)
{
    Stopwatch w(CLOCK_MONOTONIC_COARSE);

    const auto col_id = column_range->column_id;
    const auto index_id = column_range->index_id;

    RUNTIME_CHECK(dmfile->useMetaV2()); // v3

    // Check vector index exists on the column
    auto local_index = dmfile->getLocalIndex(col_id, index_id);
    if (!local_index)
        return nullptr;
    RUNTIME_CHECK(local_index->index_props().kind() == dtpb::IndexFileKind::INVERTED_INDEX);
    const auto & index_props = local_index->index_props();

    bool has_s3_download = false;
    bool has_load_from_file = false;

    // If local file is invalidated, cache is not valid anymore. So we
    // need to ensure file exists on local fs first.
    RUNTIME_CHECK(index_id > 0);
    const auto index_file_path = dmfile->localIndexPath(index_id, TiDB::ColumnarIndexKind::Inverted);
    String local_index_file_path;
    if (auto s3_file_name = S3::S3FilenameView::fromKeyWithPrefix(index_file_path); s3_file_name.isValid())
    {
        // Disaggregated mode
        auto * file_cache = FileCache::instance();
        RUNTIME_CHECK_MSG(file_cache, "Must enable S3 file cache to use inverted index");
        auto [file_seg, downloaded]
            = file_cache->downloadFileForLocalReadWithRetry(s3_file_name, index_props.file_size(), 3);
        RUNTIME_CHECK(file_seg);
        local_index_file_path = file_seg->getLocalFileName();
        has_s3_download = downloaded;
    }
    else
    {
        // Not disaggregated mode
        local_index_file_path = index_file_path;
    }

    auto load_from_file = [&]() {
        has_load_from_file = true;
        const auto type = dmfile->getColumnStat(col_id).type;
        return InvertedIndexReader::view(type, local_index_file_path);
    };

    InvertedIndexReaderPtr inverted_index;
    if (local_index_cache)
    {
        // Note: must use local_index_file_path as the cache key, because cache
        // will check whether file is still valid and try to remove memory references
        // when file is dropped.
        auto local_index = local_index_cache->getOrSet(local_index_file_path, load_from_file);
        inverted_index = std::dynamic_pointer_cast<InvertedIndexReader>(local_index);
    }
    else
    {
        inverted_index = load_from_file();
    }

    {
        // Statistics
        // TODO: add more statistics to ScanContext
        double elapsed = w.elapsedSecondsFromLastTime();
        if (has_s3_download)
        {
            // it could be possible that s3=true but load_from_file=false, it means we download a file
            // and then reuse the memory cache. The majority time comes from s3 download
            // so we still count it as s3 download.
            GET_METRIC(tiflash_inverted_index_duration, type_load_dmfile_s3).Observe(elapsed);
        }
        else if (has_load_from_file)
        {
            GET_METRIC(tiflash_inverted_index_duration, type_load_dmfile_local).Observe(elapsed);
        }
        else
        {
            GET_METRIC(tiflash_inverted_index_duration, type_load_cache).Observe(elapsed);
        }
    }

    RUNTIME_CHECK(inverted_index != nullptr);
    auto bitmap_filter = column_range->set->search(inverted_index, dmfile->getRows());
    GET_METRIC(tiflash_inverted_index_duration, type_search).Observe(w.elapsedSecondsFromLastTime());
    return bitmap_filter;
}

} // namespace DB::DM
