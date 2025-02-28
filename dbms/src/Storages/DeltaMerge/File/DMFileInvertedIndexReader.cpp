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
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileInvertedIndexReader.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/FileCachePerf.h>


namespace DB::ErrorCodes
{
extern const int S3_ERROR;
} // namespace DB::ErrorCodes

namespace DB::DM
{

String DMFileInvertedIndexReader::PerfStat::toString() const
{
    return fmt::format(
        "index_size={} load={:.2f}s{}{}, search={:.2f}s, selected_rows={}",
        index_size,
        duration_load_index,
        has_s3_download ? " (S3)" : "",
        has_load_from_file ? "" : " cached",
        duration_search,
        selected_nodes);
}

DMFileInvertedIndexReader::~DMFileInvertedIndexReader()
{
    LOG_DEBUG(Logger::get(), "Finish search inverted index over dmfile_{}, {}", dmfile->fileId(), perf_stat.toString());
}

BitmapFilterPtr DMFileInvertedIndexReader::load()
{
    if (loaded)
        return nullptr;

    auto sorted_results = column_value_set->check(
        [&](const ColumnValueSetPtr & set, size_t size) {
            auto inverted_index = loadInvertedIndex(set);
            if (!inverted_index)
                return BitmapFilterPtr();
            return loadSearchResult(inverted_index, set, size);
        },
        dmfile->getRows());

    if (sorted_results)
        perf_stat.selected_nodes = sorted_results->count();
    loaded = true;
    return sorted_results;
}

InvertedIndexViewerPtr DMFileInvertedIndexReader::loadInvertedIndex(const ColumnValueSetPtr & set)
{
    auto single_column_value_set = std::dynamic_pointer_cast<SingleColumnValueSet>(set);
    RUNTIME_CHECK(single_column_value_set != nullptr);
    const auto col_id = single_column_value_set->column_id;
    const auto index_id = single_column_value_set->index_id;

    RUNTIME_CHECK(dmfile->useMetaV2()); // v3

    // Check vector index exists on the column
    auto local_index = dmfile->getLocalIndex(col_id, index_id);
    if (!local_index)
        return nullptr;
    RUNTIME_CHECK(local_index->index_props().kind() == dtpb::IndexFileKind::INVERTED_INDEX);
    const auto & index_props = local_index->index_props();
    perf_stat.index_size = index_props.file_size();

    // If local file is invalidated, cache is not valid anymore. So we
    // need to ensure file exists on local fs first.
    RUNTIME_CHECK(index_id > 0);
    const auto index_file_path = dmfile->localIndexPath(index_id, TiDB::ColumnarIndexKind::Inverted);
    String local_index_file_path;
    if (auto s3_file_name = S3::S3FilenameView::fromKeyWithPrefix(index_file_path); s3_file_name.isValid())
    {
        // Disaggregated mod
        auto * file_cache = FileCache::instance();
        RUNTIME_CHECK_MSG(file_cache, "Must enable S3 file cache to use vector index");

        Stopwatch watch;

        auto perf_begin = PerfContext::file_cache;

        // If download file failed, retry a few times.
        for (auto i = 3; i > 0; --i)
        {
            try
            {
                if (auto file_guard = file_cache->downloadFileForLocalRead(s3_file_name, index_props.file_size());
                    file_guard)
                {
                    local_index_file_path = file_guard->getLocalFileName();
                    break; // Successfully downloaded index into local cache
                }

                throw Exception(ErrorCodes::S3_ERROR, "Failed to download vector index file {}", index_file_path);
            }
            catch (...)
            {
                if (i <= 1)
                    throw;
            }
        }

        if ( //
            PerfContext::file_cache.fg_download_from_s3 > perf_begin.fg_download_from_s3 || //
            PerfContext::file_cache.fg_wait_download_from_s3 > perf_begin.fg_wait_download_from_s3)
            perf_stat.has_s3_download = true;

        auto download_duration = watch.elapsedSeconds();
        perf_stat.duration_load_index += download_duration;
    }
    else
    {
        // Not disaggregated mode
        local_index_file_path = index_file_path;
    }

    auto load_from_file = [&]() {
        perf_stat.has_load_from_file = true;
        const auto type = dmfile->getColumnStat(col_id).type;
        // ReadBufferFromFile read_buf(local_index_file_path);
        // return InvertedIndexViewer::view(type_id, read_buf, index_props.file_size());
        return InvertedIndexViewer::view(type, local_index_file_path);
    };

    Stopwatch watch;
    InvertedIndexViewerPtr inverted_index;
    if (local_index_cache)
    {
        // Note: must use local_index_file_path as the cache key, because cache
        // will check whether file is still valid and try to remove memory references
        // when file is dropped.
        auto local_index = local_index_cache->getOrSet(local_index_file_path, load_from_file);
        inverted_index = std::dynamic_pointer_cast<InvertedIndexViewer>(local_index);
    }
    else
    {
        inverted_index = load_from_file();
    }

    perf_stat.duration_load_index += watch.elapsedSeconds();
    RUNTIME_CHECK(inverted_index != nullptr);
    return inverted_index;
}

BitmapFilterPtr DMFileInvertedIndexReader::loadSearchResult(
    const InvertedIndexViewerPtr & inverted_index,
    const ColumnValueSetPtr & set,
    size_t size)
{
    Stopwatch watch;

    auto single_column_value_set = std::dynamic_pointer_cast<SingleColumnValueSet>(set);
    RUNTIME_CHECK(single_column_value_set != nullptr);

    auto search_results = single_column_value_set->set->search(inverted_index, size);

    perf_stat.duration_search += watch.elapsedSeconds();
    return search_results;
}

} // namespace DB::DM
