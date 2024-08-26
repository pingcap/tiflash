// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/File/DMFileWithVectorIndexBlockInputStream.h>
#include <Storages/DeltaMerge/File/VectorColumnFromIndexReader.h>
#include <Storages/DeltaMerge/Index/VectorIndexCache.h>
#include <Storages/DeltaMerge/Index/VectorSearchPerf.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/S3/FileCache.h>
#include <Storages/S3/FileCachePerf.h>

#include <algorithm>


namespace DB::ErrorCodes
{
extern const int S3_ERROR;
} // namespace DB::ErrorCodes

namespace DB::DM
{

DMFileWithVectorIndexBlockInputStream::DMFileWithVectorIndexBlockInputStream(
    const ANNQueryInfoPtr & ann_query_info_,
    const DMFilePtr & dmfile_,
    Block && header_layout_,
    DMFileReader && reader_,
    ColumnDefine && vec_cd_,
    const FileProviderPtr & file_provider_,
    const ReadLimiterPtr & read_limiter_,
    const ScanContextPtr & scan_context_,
    const VectorIndexCachePtr & vec_index_cache_,
    const BitmapFilterView & valid_rows_,
    const String & tracing_id)
    : log(Logger::get(tracing_id))
    , ann_query_info(ann_query_info_)
    , dmfile(dmfile_)
    , header_layout(std::move(header_layout_))
    , reader(std::move(reader_))
    , vec_cd(std::move(vec_cd_))
    , file_provider(file_provider_)
    , read_limiter(read_limiter_)
    , scan_context(scan_context_)
    , vec_index_cache(vec_index_cache_)
    , valid_rows(valid_rows_)
{
    RUNTIME_CHECK(ann_query_info);
    RUNTIME_CHECK(vec_cd.id == ann_query_info->column_id());
    for (const auto & cd : reader.read_columns)
    {
        RUNTIME_CHECK(header_layout.has(cd.name), cd.name);
        RUNTIME_CHECK(cd.id != vec_cd.id);
    }
    RUNTIME_CHECK(header_layout.has(vec_cd.name));
    RUNTIME_CHECK(header_layout.columns() == reader.read_columns.size() + 1);

    // Fill start_offset_to_pack_id
    const auto & pack_stats = dmfile->getPackStats();
    start_offset_to_pack_id.reserve(pack_stats.size());
    UInt32 start_offset = 0;
    for (size_t pack_id = 0; pack_id < pack_stats.size(); ++pack_id)
    {
        start_offset_to_pack_id[start_offset] = pack_id;
        start_offset += pack_stats[pack_id].rows;
    }

    // Fill header
    header = toEmptyBlock(reader.read_columns);
    addColumnToBlock(header, vec_cd.id, vec_cd.name, vec_cd.type, vec_cd.type->createColumn(), vec_cd.default_value);
}

DMFileWithVectorIndexBlockInputStream::~DMFileWithVectorIndexBlockInputStream()
{
    if (!vec_column_reader)
        return;

    scan_context->total_vector_idx_read_vec_time_ms += static_cast<UInt64>(duration_read_from_vec_index_seconds * 1000);
    scan_context->total_vector_idx_read_others_time_ms
        += static_cast<UInt64>(duration_read_from_other_columns_seconds * 1000);

    LOG_DEBUG( //
        log,
        "Finished read DMFile with vector index for column dmf_{}/{}(id={}), "
        "query_top_k={} load_index+result={:.2f}s read_from_index={:.2f}s read_from_others={:.2f}s",
        dmfile->fileId(),
        vec_cd.name,
        vec_cd.id,
        ann_query_info->top_k(),
        duration_load_vec_index_and_results_seconds,
        duration_read_from_vec_index_seconds,
        duration_read_from_other_columns_seconds);
}


Block DMFileWithVectorIndexBlockInputStream::read(FilterPtr & res_filter, bool return_filter)
{
    if (return_filter)
        return readImpl(res_filter);

    // If return_filter == false, we must filter by ourselves.

    FilterPtr filter = nullptr;
    auto res = readImpl(filter);
    if (filter != nullptr)
    {
        for (auto & col : res)
            col.column = col.column->filter(*filter, -1);
    }
    // filter == nullptr means all rows are valid and no need to filter.

    return res;
}

Block DMFileWithVectorIndexBlockInputStream::readImpl(FilterPtr & res_filter)
{
    load();

    Block res;
    if (!reader.read_columns.empty())
        res = readByFollowingOtherColumns();
    else
        res = readByIndexReader();

    if (!res)
        return {};

    // Assign output filter according to sorted_results.
    //
    // For example, if sorted_results is [3, 10], the complete filter array is:
    // [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]
    // And we should only return filter array starting from res.startOffset(), like:
    // [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1]
    //              ^startOffset   ^startOffset+rows
    //     filter: [0, 0, 0, 0, 0]
    //
    // We will use startOffset as lowerBound (inclusive), ans startOffset+rows
    // as upperBound (exclusive) to find whether this range has a match in sorted_results.

    const auto start_offset = res.startOffset();
    const auto max_rowid_exclusive = start_offset + res.rows();

    filter.clear();
    filter.resize_fill(res.rows(), 0);

    auto it = std::lower_bound(sorted_results.begin(), sorted_results.end(), start_offset);
    while (it != sorted_results.end())
    {
        auto rowid = *it;
        if (rowid >= max_rowid_exclusive)
            break;
        filter[rowid - start_offset] = 1;
        ++it;
    }

    res_filter = &filter;
    return res;
}

Block DMFileWithVectorIndexBlockInputStream::readByIndexReader()
{
    const auto & pack_stats = dmfile->getPackStats();
    size_t all_packs = pack_stats.size();
    const auto & pack_res = reader.pack_filter.getPackResConst();

    RUNTIME_CHECK(pack_res.size() == all_packs);

    // Skip as many packs as possible according to Pack Filter
    while (index_reader_next_pack_id < all_packs)
    {
        if (pack_res[index_reader_next_pack_id].isUse())
            break;
        index_reader_next_row_id += pack_stats[index_reader_next_pack_id].rows;
        index_reader_next_pack_id++;
    }

    if (index_reader_next_pack_id >= all_packs)
        // Finished
        return {};

    auto read_pack_id = index_reader_next_pack_id;
    auto block_start_row_id = index_reader_next_row_id;
    auto read_rows = pack_stats[read_pack_id].rows;

    index_reader_next_pack_id++;
    index_reader_next_row_id += read_rows;

    Block block;
    block.setStartOffset(block_start_row_id);

    auto vec_column = vec_cd.type->createColumn();
    vec_column->reserve(read_rows);

    Stopwatch w;
    vec_column_reader->read(vec_column, read_pack_id, read_rows);
    duration_read_from_vec_index_seconds += w.elapsedSeconds();

    block.insert(ColumnWithTypeAndName{std::move(vec_column), vec_cd.type, vec_cd.name, vec_cd.id});

    return block;
}

Block DMFileWithVectorIndexBlockInputStream::readByFollowingOtherColumns()
{
    // First read other columns.
    Stopwatch w;
    auto block_others = reader.read();
    duration_read_from_other_columns_seconds += w.elapsedSeconds();

    if (!block_others)
        return {};

    auto read_rows = block_others.rows();

    // Using vec_cd.type to construct a Column directly instead of using
    // the type from dmfile, so that we don't need extra transforms
    // (e.g. wrap with a Nullable). vec_column_reader is compatible with
    // both Nullable and NotNullable.
    auto vec_column = vec_cd.type->createColumn();
    vec_column->reserve(read_rows);

    // Then read from vector index for the same pack.
    w.restart();

    vec_column_reader->read(vec_column, getPackIdFromBlock(block_others), read_rows);
    duration_read_from_vec_index_seconds += w.elapsedSeconds();

    // Re-assemble block using the same layout as header_layout.
    Block res = header_layout.cloneEmpty();
    // Note: the start offset counts from the beginning of THIS dmfile. It
    // is not a global offset.
    res.setStartOffset(block_others.startOffset());
    for (const auto & elem : block_others)
    {
        RUNTIME_CHECK(res.has(elem.name));
        res.getByName(elem.name).column = std::move(elem.column);
    }
    RUNTIME_CHECK(res.has(vec_cd.name));
    res.getByName(vec_cd.name).column = std::move(vec_column);

    return res;
}

void DMFileWithVectorIndexBlockInputStream::load()
{
    if (loaded)
        return;

    Stopwatch w;

    loadVectorIndex();
    loadVectorSearchResult();

    duration_load_vec_index_and_results_seconds = w.elapsedSeconds();

    loaded = true;
}

void DMFileWithVectorIndexBlockInputStream::loadVectorIndex()
{
    bool has_s3_download = false;
    bool has_load_from_file = false;

    double duration_load_index = 0; // include download from s3 and load from fs

    auto col_id = ann_query_info->column_id();

    RUNTIME_CHECK(dmfile->useMetaV2()); // v3

    // Check vector index exists on the column
    const auto & column_stat = dmfile->getColumnStat(col_id);
    RUNTIME_CHECK(column_stat.index_bytes > 0);
    RUNTIME_CHECK(column_stat.vector_index.has_value());

    // If local file is invalidated, cache is not valid anymore. So we
    // need to ensure file exists on local fs first.
    const auto file_name_base = DMFile::getFileNameBase(col_id);
    const auto index_file_path = dmfile->colIndexPath(file_name_base);
    String local_index_file_path;
    FileSegmentPtr file_guard = nullptr;
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
                file_guard = file_cache->downloadFileForLocalRead( //
                    s3_file_name,
                    column_stat.index_bytes);
                if (file_guard)
                {
                    local_index_file_path = file_guard->getLocalFileName();
                    break; // Successfully downloaded index into local cache
                }

                throw Exception( //
                    ErrorCodes::S3_ERROR,
                    "Failed to download vector index file {}",
                    index_file_path);
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
            has_s3_download = true;

        auto download_duration = watch.elapsedSeconds();
        duration_load_index += download_duration;

        GET_METRIC(tiflash_vector_index_duration, type_download).Observe(download_duration);
    }
    else
    {
        // Not disaggregated mode
        local_index_file_path = index_file_path;
    }

    auto load_from_file = [&]() {
        has_load_from_file = true;
        return VectorIndexViewer::view(*column_stat.vector_index, local_index_file_path);
    };

    Stopwatch watch;
    if (vec_index_cache)
        // Note: must use local_index_file_path as the cache key, because cache
        // will check whether file is still valid and try to remove memory references
        // when file is dropped.
        vec_index = vec_index_cache->getOrSet(local_index_file_path, load_from_file);
    else
        vec_index = load_from_file();

    duration_load_index += watch.elapsedSeconds();
    RUNTIME_CHECK(vec_index != nullptr);

    scan_context->total_vector_idx_load_time_ms += static_cast<UInt64>(duration_load_index * 1000);
    if (has_s3_download)
        // it could be possible that s3=true but load_from_file=false, it means we download a file
        // and then reuse the memory cache. The majority time comes from s3 download
        // so we still count it as s3 download.
        scan_context->total_vector_idx_load_from_s3++;
    else if (has_load_from_file)
        scan_context->total_vector_idx_load_from_disk++;
    else
        scan_context->total_vector_idx_load_from_cache++;

    LOG_DEBUG( //
        log,
        "Loaded vector index for column dmf_{}/{}(id={}), index_size={} kind={} cost={:.2f}s {} {}",
        dmfile->fileId(),
        vec_cd.name,
        vec_cd.id,
        column_stat.index_bytes,
        column_stat.vector_index->index_kind(),
        duration_load_index,
        has_s3_download ? "(S3)" : "",
        has_load_from_file ? "(LoadFile)" : "");
}

void DMFileWithVectorIndexBlockInputStream::loadVectorSearchResult()
{
    Stopwatch watch;

    auto perf_begin = PerfContext::vector_search;

    RUNTIME_CHECK(valid_rows.size() >= dmfile->getRows(), valid_rows.size(), dmfile->getRows());
    sorted_results = vec_index->search(ann_query_info, valid_rows);
    std::sort(sorted_results.begin(), sorted_results.end());
    // results must not contain duplicates. Usually there should be no duplicates.
    sorted_results.erase(std::unique(sorted_results.begin(), sorted_results.end()), sorted_results.end());

    auto discarded_nodes = PerfContext::vector_search.discarded_nodes - perf_begin.discarded_nodes;
    auto visited_nodes = PerfContext::vector_search.visited_nodes - perf_begin.visited_nodes;

    double search_duration = watch.elapsedSeconds();
    scan_context->total_vector_idx_search_time_ms += static_cast<UInt64>(search_duration * 1000);
    scan_context->total_vector_idx_search_discarded_nodes += discarded_nodes;
    scan_context->total_vector_idx_search_visited_nodes += visited_nodes;

    vec_column_reader = std::make_shared<VectorColumnFromIndexReader>(dmfile, vec_index, sorted_results);

    // Vector index is very likely to filter out some packs. For example,
    // if we query for Top 1, then only 1 pack will be remained. So we
    // update the pack filter used by the DMFileReader to avoid reading
    // unnecessary data for other columns.
    size_t valid_packs_before_search = 0;
    size_t valid_packs_after_search = 0;
    const auto & pack_stats = dmfile->getPackStats();
    auto & pack_res = reader.pack_filter.getPackRes();

    size_t results_it = 0;
    const size_t results_it_max = sorted_results.size();

    UInt32 pack_start = 0;

    for (size_t pack_id = 0, pack_id_max = dmfile->getPacks(); pack_id < pack_id_max; pack_id++)
    {
        if (pack_res[pack_id].isUse())
            ++valid_packs_before_search;

        bool pack_has_result = false;

        UInt32 pack_end = pack_start + pack_stats[pack_id].rows;
        while (results_it < results_it_max //
               && sorted_results[results_it] >= pack_start //
               && sorted_results[results_it] < pack_end)
        {
            pack_has_result = true;
            results_it++;
        }

        if (!pack_has_result)
            pack_res[pack_id] = RSResult::None;

        if (pack_res[pack_id].isUse())
            ++valid_packs_after_search;

        pack_start = pack_end;
    }

    RUNTIME_CHECK_MSG(results_it == results_it_max, "All packs has been visited but not all results are consumed");

    LOG_DEBUG( //
        log,
        "Finished vector search over column dmf_{}/{}(id={}), cost={:.2f}s "
        "top_k_[query/visited/discarded/result]={}/{}/{}/{} "
        "rows_[file/after_search]={}/{} "
        "pack_[total/before_search/after_search]={}/{}/{}",

        dmfile->fileId(),
        vec_cd.name,
        vec_cd.id,
        search_duration,

        ann_query_info->top_k(),
        visited_nodes, // Visited nodes will be larger than query_top_k when there are MVCC rows
        discarded_nodes, // How many nodes are skipped by MVCC
        sorted_results.size(),

        dmfile->getRows(),
        sorted_results.size(),

        pack_stats.size(),
        valid_packs_before_search,
        valid_packs_after_search);
}

UInt32 DMFileWithVectorIndexBlockInputStream::getPackIdFromBlock(const Block & block)
{
    // The start offset of a block is ensured to be aligned with the pack.
    // This is how we know which pack the block comes from.
    auto start_offset = block.startOffset();
    auto it = start_offset_to_pack_id.find(start_offset);
    RUNTIME_CHECK(it != start_offset_to_pack_id.end());
    return it->second;
}

} // namespace DB::DM
