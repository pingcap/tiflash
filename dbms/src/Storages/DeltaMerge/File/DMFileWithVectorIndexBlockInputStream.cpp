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

Block DMFileWithVectorIndexBlockInputStream::read()
{
    internalLoad();

    if (reader.read_block_infos.empty())
        return {};

    const auto [start_pack_id, pack_count, rs_result, read_rows] = reader.read_block_infos.front();
    const auto start_row_offset = reader.pack_offset[start_pack_id];

    auto vec_column = vec_cd.type->createColumn();
    auto begin = std::lower_bound(sorted_results.cbegin(), sorted_results.cend(), start_row_offset);
    auto end = std::lower_bound(begin, sorted_results.cend(), start_row_offset + read_rows);
    const std::span block_selected_rows{begin, end};
    if (block_selected_rows.empty())
        return {};

    // read vector column
    vec_index_reader->read(vec_column, block_selected_rows);

    Block block;

    // read other columns if needed
    if (!reader.read_columns.empty())
    {
        Stopwatch w;

        filter.clear();
        filter.resize_fill(read_rows, 0);
        for (const auto rowid : block_selected_rows)
            filter[rowid - start_row_offset] = 1;

        block = reader.read();
        for (auto & col : block)
            col.column = col.column->filter(filter, block_selected_rows.size());
        duration_read_from_other_columns_seconds += w.elapsedSeconds();
    }
    else
    {
        // Since we do not call `reader.read()` here, we need to pop the read_block_infos manually.
        reader.read_block_infos.pop_front();
    }

    auto index = header.getPositionByName(vec_cd.name);
    block.insert(index, ColumnWithTypeAndName{std::move(vec_column), vec_cd.type, vec_cd.name, vec_cd.id});

    block.setStartOffset(start_row_offset);
    block.setRSResult(rs_result);
    return block;
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

    updateReadBlockInfos();
}

void DMFileWithVectorIndexBlockInputStream::updateReadBlockInfos()
{
    // Vector index is very likely to filter out some packs. For example,
    // if we query for Top 1, then only 1 pack will be remained. So we
    // update the reader's read_block_infos to avoid reading unnecessary data for other columns.

    // The following logic is nearly the same with DMFileReader::initReadBlockInfos.

    auto & read_block_infos = reader.read_block_infos;
    const auto & pack_offset = reader.pack_offset;

    read_block_infos.clear();
    const auto & pack_stats = dmfile->getPackStats();
    const auto & pack_res = reader.pack_filter.getPackResConst();

    // Update valid_packs_before_search
    for (const auto res : pack_res)
        valid_packs_before_search += res.isUse();

    // Update read_block_infos
    size_t start_pack_id = 0;
    size_t read_rows = 0;
    auto prev_block_pack_res = RSResult::All;
    auto sorted_results_it = sorted_results.cbegin();
    size_t pack_id = 0;
    for (; pack_id < pack_stats.size(); ++pack_id)
    {
        if (sorted_results_it == sorted_results.cend())
            break;
        auto begin = std::lower_bound(sorted_results_it, sorted_results.cend(), pack_offset[pack_id]);
        auto end = std::lower_bound(begin, sorted_results.cend(), pack_offset[pack_id] + pack_stats[pack_id].rows);
        bool is_use = begin != end;
        bool reach_limit = read_rows >= reader.rows_threshold_per_read;
        bool break_all_match = prev_block_pack_res.allMatch() && !pack_res[pack_id].allMatch()
            && read_rows >= reader.rows_threshold_per_read / 2;

        if (!is_use)
        {
            if (read_rows > 0)
                read_block_infos.emplace_back(start_pack_id, pack_id - start_pack_id, prev_block_pack_res, read_rows);
            start_pack_id = pack_id + 1;
            read_rows = 0;
            prev_block_pack_res = RSResult::All;
        }
        else if (reach_limit || break_all_match)
        {
            if (read_rows > 0)
                read_block_infos.emplace_back(start_pack_id, pack_id - start_pack_id, prev_block_pack_res, read_rows);
            start_pack_id = pack_id;
            read_rows = pack_stats[pack_id].rows;
            prev_block_pack_res = pack_res[pack_id];
        }
        else
        {
            prev_block_pack_res = prev_block_pack_res && pack_res[pack_id];
            read_rows += pack_stats[pack_id].rows;
        }

        sorted_results_it = end;
    }
    if (read_rows > 0)
        read_block_infos.emplace_back(start_pack_id, pack_id - start_pack_id, prev_block_pack_res, read_rows);

    // Update valid_packs_after_search
    for (const auto & block_info : read_block_infos)
        valid_packs_after_search += block_info.pack_count;

    RUNTIME_CHECK_MSG(sorted_results_it == sorted_results.cend(), "All results are not consumed");
    loaded = true;
}

void DMFileWithVectorIndexBlockInputStream::setSelectedRows(const std::span<const UInt32> & selected_rows)
{
    sorted_results.clear();
    sorted_results.reserve(selected_rows.size());
    std::copy(selected_rows.begin(), selected_rows.end(), std::back_inserter(sorted_results));

    updateReadBlockInfos();
}

} // namespace DB::DM
