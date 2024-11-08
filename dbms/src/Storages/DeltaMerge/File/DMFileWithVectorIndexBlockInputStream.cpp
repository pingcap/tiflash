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
#include <Storages/DeltaMerge/ScanContext.h>


namespace DB::DM
{

DMFileWithVectorIndexBlockInputStream::DMFileWithVectorIndexBlockInputStream(
    const ANNQueryInfoPtr & ann_query_info_,
    const DMFilePtr & dmfile_,
    Block && header_,
    DMFileReader && reader_,
    ColumnDefine && vec_cd_,
    const ScanContextPtr & scan_context_,
    const VectorIndexCachePtr & vec_index_cache_,
    const BitmapFilterView & valid_rows_,
    const String & tracing_id)
    : log(Logger::get(tracing_id))
    , ann_query_info(ann_query_info_)
    , dmfile(dmfile_)
    , header(std::move(header_))
    , reader(std::move(reader_))
    , vec_cd(std::move(vec_cd_))
    , scan_context(scan_context_)
    , vec_index_reader(std::make_shared<DMFileVectorIndexReader>(
          ann_query_info,
          dmfile,
          valid_rows_,
          scan_context,
          vec_index_cache_))
{}

DMFileWithVectorIndexBlockInputStream::~DMFileWithVectorIndexBlockInputStream()
{
    scan_context->total_vector_idx_read_others_time_ms
        += static_cast<UInt64>(duration_read_from_other_columns_seconds * 1000);

    LOG_DEBUG(
        log,
        "Finished vector search over column dmf_{}/{}(id={}), index_id={} {} "
        "pack_[total/before_search/after_search]={}/{}/{}",
        dmfile->fileId(),
        vec_cd.name,
        vec_cd.id,
        ann_query_info->index_id(),

        vec_index_reader->perfStat(),

        dmfile->getPackStats().size(),
        valid_packs_before_search,
        valid_packs_after_search);
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
    internalLoad();

    auto [res, real_rows] = reader.read_columns.empty() ? readByIndexReader() : readByFollowingOtherColumns();

    if (!res)
        return {};

    // If all rows are valid, res_filter is nullptr.
    if (real_rows == res.rows())
    {
        res_filter = nullptr;
        return res;
    }

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

std::tuple<Block, size_t> DMFileWithVectorIndexBlockInputStream::readByIndexReader()
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
        ++index_reader_next_pack_id;
    }

    if (index_reader_next_pack_id >= all_packs)
        // Finished
        return {};

    auto block_start_row_id = index_reader_next_row_id;
    while (index_reader_next_pack_id < all_packs)
    {
        if (!pack_res[index_reader_next_pack_id].isUse())
            break;
        index_reader_next_row_id += pack_stats[index_reader_next_pack_id].rows;
        ++index_reader_next_pack_id;
    }

    Block block;
    block.setStartOffset(block_start_row_id);

    size_t read_rows = index_reader_next_row_id - block_start_row_id;
    auto vec_column = vec_cd.type->createColumn();

    auto begin = std::lower_bound(sorted_results.cbegin(), sorted_results.cend(), block_start_row_id);
    auto end = std::lower_bound(begin, sorted_results.cend(), index_reader_next_row_id);
    const std::span<const VectorIndexViewer::Key> block_selected_rows{
        &*begin,
        static_cast<size_t>(std::distance(begin, end))};
    vec_index_reader->read(vec_column, block_selected_rows, block_start_row_id, read_rows);

    block.insert(ColumnWithTypeAndName{std::move(vec_column), vec_cd.type, vec_cd.name, vec_cd.id});
    return {block, block_selected_rows.size()};
}

std::tuple<Block, size_t> DMFileWithVectorIndexBlockInputStream::readByFollowingOtherColumns()
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
    // (e.g. wrap with a Nullable). vec_index_reader is compatible with
    // both Nullable and NotNullable.
    auto vec_column = vec_cd.type->createColumn();

    // Then read from vector index for the same pack.
    auto begin = std::lower_bound(sorted_results.cbegin(), sorted_results.cend(), block_others.startOffset());
    auto end = std::lower_bound(begin, sorted_results.cend(), block_others.startOffset() + read_rows);
    const std::span<const VectorIndexViewer::Key> block_selected_rows{
        &*begin,
        static_cast<size_t>(std::distance(begin, end))};
    vec_index_reader->read(vec_column, block_selected_rows, block_others.startOffset(), read_rows);

    // Re-assemble block using the same layout as header.
    // Insert the vector column into the block.
    auto index = header.getPositionByName(vec_cd.name);
    block_others.insert(index, ColumnWithTypeAndName(std::move(vec_column), vec_cd.type, vec_cd.name));
    return {block_others, block_selected_rows.size()};
}

std::vector<VectorIndexViewer::SearchResult> DMFileWithVectorIndexBlockInputStream::load()
{
    if (loaded)
        return {};

    auto search_results = vec_index_reader->load();
    return search_results;
}

void DMFileWithVectorIndexBlockInputStream::internalLoad()
{
    if (loaded)
        return;

    auto search_results = vec_index_reader->load();
    sorted_results.reserve(search_results.size());
    for (const auto & row : search_results)
        sorted_results.push_back(row.key);

    updateRSResult();
}

void DMFileWithVectorIndexBlockInputStream::updateRSResult()
{
    // Vector index is very likely to filter out some packs. For example,
    // if we query for Top 1, then only 1 pack will be remained. So we
    // update the pack filter used by the DMFileReader to avoid reading
    // unnecessary data for other columns.
    const auto & pack_stats = dmfile->getPackStats();
    auto & pack_res = reader.pack_filter.getPackRes();

    auto results_it = sorted_results.begin();
    UInt32 pack_start = 0;
    for (size_t pack_id = 0; pack_id < dmfile->getPacks(); ++pack_id)
    {
        if (pack_res[pack_id].isUse())
            ++valid_packs_before_search;

        bool pack_has_result = false;
        UInt32 pack_end = pack_start + pack_stats[pack_id].rows;
        while (results_it != sorted_results.end() && *results_it >= pack_start && *results_it < pack_end)
        {
            pack_has_result = true;
            ++results_it;
        }

        if (!pack_has_result)
            pack_res[pack_id] = RSResult::None;

        if (pack_res[pack_id].isUse())
            ++valid_packs_after_search;

        pack_start = pack_end;
    }

    RUNTIME_CHECK_MSG(
        results_it == sorted_results.end(),
        "All packs has been visited but not all results are consumed");

    loaded = true;
}

void DMFileWithVectorIndexBlockInputStream::setSelectedRows(const std::span<const UInt32> & selected_rows)
{
    sorted_results.clear();
    sorted_results.reserve(selected_rows.size());
    std::copy(selected_rows.begin(), selected_rows.end(), std::back_inserter(sorted_results));

    updateRSResult();
}

} // namespace DB::DM
