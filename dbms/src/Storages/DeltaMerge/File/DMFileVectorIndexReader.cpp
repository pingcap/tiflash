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

#include <Common/TiFlashMetrics.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileVectorIndexReader.h>
#include <Storages/DeltaMerge/Index/VectorIndexCache.h>
#include <Storages/DeltaMerge/Index/VectorSearchPerf.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/FileCachePerf.h>


namespace DB::ErrorCodes
{
extern const int S3_ERROR;
} // namespace DB::ErrorCodes

namespace DB::DM
{

String DMFileVectorIndexReader::PerfStat::toString() const
{
    return fmt::format(
        "index_size={} load={:.2f}s{}{}, search={:.2f}s, read_vec_column={:.2f}s",
        index_size,
        duration_load_index,
        has_s3_download ? " (S3)" : "",
        has_load_from_file ? " (LoadFile)" : "",
        duration_search,
        duration_read_vec_column);
}

std::vector<VectorIndexViewer::Key> DMFileVectorIndexReader::load()
{
    if (loaded)
        return {};

    loadVectorIndex();
    auto sorted_results = loadVectorSearchResult();

    perf_stat.selected_nodes = sorted_results.size();
    loaded = true;
    return sorted_results;
}

void DMFileVectorIndexReader::loadVectorIndex()
{
    const auto col_id = ann_query_info->column_id();
    const auto index_id = ann_query_info->index_id() > 0 ? ann_query_info->index_id() : EmptyIndexID;

    RUNTIME_CHECK(dmfile->useMetaV2()); // v3

    // Check vector index exists on the column
    auto vector_index = dmfile->getLocalIndex(col_id, index_id);
    RUNTIME_CHECK(vector_index.has_value(), col_id, index_id);
    perf_stat.index_size = vector_index->index_bytes();

    // If local file is invalidated, cache is not valid anymore. So we
    // need to ensure file exists on local fs first.
    const auto index_file_path = index_id > 0 //
        ? dmfile->vectorIndexPath(index_id) //
        : dmfile->colIndexPath(DMFile::getFileNameBase(col_id));
    String local_index_file_path;
    if (auto s3_file_name = S3::S3FilenameView::fromKeyWithPrefix(index_file_path); s3_file_name.isValid())
    {
        // Disaggregated mode
        auto * file_cache = FileCache::instance();
        RUNTIME_CHECK_MSG(file_cache, "Must enable S3 file cache to use vector index");

        Stopwatch watch;

        auto perf_begin = PerfContext::file_cache;

        // If download file failed, retry a few times.
        for (auto i = 3; i > 0; --i)
        {
            try
            {
                if (auto file_guard = file_cache->downloadFileForLocalRead(s3_file_name, vector_index->index_bytes());
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

        GET_METRIC(tiflash_vector_index_duration, type_download).Observe(download_duration);
    }
    else
    {
        // Not disaggregated mode
        local_index_file_path = index_file_path;
    }

    auto load_from_file = [&]() {
        perf_stat.has_load_from_file = true;
        return VectorIndexViewer::view(*vector_index, local_index_file_path);
    };

    Stopwatch watch;
    if (vec_index_cache)
        // Note: must use local_index_file_path as the cache key, because cache
        // will check whether file is still valid and try to remove memory references
        // when file is dropped.
        vec_index = vec_index_cache->getOrSet(local_index_file_path, load_from_file);
    else
        vec_index = load_from_file();

    perf_stat.duration_load_index += watch.elapsedSeconds();
    RUNTIME_CHECK(vec_index != nullptr);

    scan_context->total_vector_idx_load_time_ms += static_cast<UInt64>(perf_stat.duration_load_index * 1000);
    if (perf_stat.has_s3_download)
        // it could be possible that s3=true but load_from_file=false, it means we download a file
        // and then reuse the memory cache. The majority time comes from s3 download
        // so we still count it as s3 download.
        scan_context->total_vector_idx_load_from_s3++;
    else if (perf_stat.has_load_from_file)
        scan_context->total_vector_idx_load_from_disk++;
    else
        scan_context->total_vector_idx_load_from_cache++;
}

DMFileVectorIndexReader::~DMFileVectorIndexReader()
{
    scan_context->total_vector_idx_read_vec_time_ms += static_cast<UInt64>(perf_stat.duration_read_vec_column * 1000);
}

String DMFileVectorIndexReader::perfStat() const
{
    return fmt::format(
        "{} top_k_[query/visited/discarded/result]={}/{}/{}/{}",
        perf_stat.toString(),
        ann_query_info->top_k(),
        perf_stat.visited_nodes,
        perf_stat.discarded_nodes,
        perf_stat.selected_nodes);
}

std::vector<VectorIndexViewer::Key> DMFileVectorIndexReader::loadVectorSearchResult()
{
    Stopwatch watch;

    auto perf_begin = PerfContext::vector_search;

    RUNTIME_CHECK(valid_rows.size() >= dmfile->getRows(), valid_rows.size(), dmfile->getRows());
    auto sorted_results = vec_index->search(ann_query_info, valid_rows);
    std::sort(sorted_results.begin(), sorted_results.end());
    // results must not contain duplicates. Usually there should be no duplicates.
    sorted_results.erase(std::unique(sorted_results.begin(), sorted_results.end()), sorted_results.end());

    perf_stat.discarded_nodes = PerfContext::vector_search.discarded_nodes - perf_begin.discarded_nodes;
    perf_stat.visited_nodes = PerfContext::vector_search.visited_nodes - perf_begin.visited_nodes;

    perf_stat.duration_search = watch.elapsedSeconds();
    scan_context->total_vector_idx_search_time_ms += static_cast<UInt64>(perf_stat.duration_search * 1000);
    scan_context->total_vector_idx_search_discarded_nodes += perf_stat.discarded_nodes;
    scan_context->total_vector_idx_search_visited_nodes += perf_stat.visited_nodes;

    return sorted_results;
}

void DMFileVectorIndexReader::read(
    MutableColumnPtr & vec_column,
    const std::span<const VectorIndexViewer::Key> & selected_rows,
    size_t start_offset,
    size_t column_size)
{
    Stopwatch watch;
    RUNTIME_CHECK(loaded);

    vec_column->reserve(column_size);
    std::vector<Float32> value;
    size_t current_rowid = start_offset;
    for (auto rowid : selected_rows)
    {
        vec_index->get(rowid, value);
        if (rowid > current_rowid)
        {
            UInt32 nulls = rowid - current_rowid;
            // Insert [] if column is Not Null, or NULL if column is Nullable
            vec_column->insertManyDefaults(nulls);
        }
        vec_column->insertData(reinterpret_cast<const char *>(value.data()), value.size() * sizeof(Float32));
        current_rowid = rowid + 1;
    }
    if (current_rowid < start_offset + column_size)
    {
        UInt32 nulls = column_size + start_offset - current_rowid;
        vec_column->insertManyDefaults(nulls);
    }
    perf_stat.duration_read_vec_column += watch.elapsedSeconds();
}

} // namespace DB::DM
