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
#include <Storages/DeltaMerge/Index/VectorIndex/Perf.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Reader.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/DMFileInputStream.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/ReaderFromDMFile.h>


namespace DB::DM
{

DMFileInputStreamProvideVectorIndex::DMFileInputStreamProvideVectorIndex(
    const VectorIndexStreamCtxPtr & ctx_,
    const DMFilePtr & dmfile_,
    DMFileReader && rest_col_reader_)
    : ctx(ctx_)
    , dmfile(dmfile_)
    , rest_col_reader(std::move(rest_col_reader_))
{
    RUNTIME_CHECK(dmfile != nullptr);
}

Block DMFileInputStreamProvideVectorIndex::read()
{
    // We expect setReturnRows() is called before doing any read().
    RUNTIME_CHECK(sorted_results.owner != nullptr);
    RUNTIME_CHECK(vec_index != nullptr);

    const auto sorted_results_view = sorted_results.view;

    if (rest_col_reader.read_block_infos.empty())
        return {};

    const auto [start_pack_id, pack_count, rs_result, read_rows] = rest_col_reader.read_block_infos.front();
    const auto start_row_offset = rest_col_reader.pack_offset[start_pack_id];

    auto begin = std::lower_bound( //
        sorted_results_view.begin(),
        sorted_results_view.end(),
        start_row_offset,
        [](const auto & lhs, const auto & rhs) { return lhs.rowid < rhs; });
    auto end = std::lower_bound( //
        begin,
        sorted_results_view.end(),
        start_row_offset + read_rows,
        [](const auto & lhs, const auto & rhs) { return lhs.rowid < rhs; });
    const std::span block_selected_rows{begin, end};
    if (block_selected_rows.empty())
        return {};

    Stopwatch w(CLOCK_MONOTONIC_COARSE);

    auto vec_column = ctx->vec_cd.type->createColumn();
    vec_column->reserve(block_selected_rows.size());
    for (const auto & row : block_selected_rows)
    {
        vec_index->get(row.rowid, ctx->vector_value);
        vec_column->insertData(
            reinterpret_cast<const char *>(ctx->vector_value.data()),
            ctx->vector_value.size() * sizeof(Float32));
    }

    ctx->perf->n_dm_reads += 1;
    ctx->perf->total_dm_read_vec_ms += w.elapsedMillisecondsFromLastTime();

    Block block;

    // read other columns if needed
    if (!rest_col_reader.read_columns.empty())
    {
        ctx->filter.clear();
        ctx->filter.resize_fill(read_rows, 0);
        for (const auto & row : block_selected_rows)
            ctx->filter[row.rowid - start_row_offset] = 1;

        block = rest_col_reader.read();
        for (auto & col : block)
            col.column = col.column->filter(ctx->filter, block_selected_rows.size());
    }
    else
    {
        // Since we do not call `reader.read()` here, we need to pop the read_block_infos manually.
        rest_col_reader.read_block_infos.pop_front();
    }

    ctx->perf->total_dm_read_others_ms += w.elapsedMillisecondsFromLastTime();

    const auto vec_col_pos = ctx->header.getPositionByName(ctx->vec_cd.name);
    block.insert(
        vec_col_pos,
        ColumnWithTypeAndName{std::move(vec_column), ctx->vec_cd.type, ctx->vec_cd.name, ctx->vec_cd.id});
    block.setStartOffset(start_row_offset);
    block.setRSResult(rs_result);
    return block;
}

VectorIndexReaderPtr DMFileInputStreamProvideVectorIndex::getVectorIndexReader()
{
    if (vec_index != nullptr)
        return vec_index;
    vec_index = VectorIndexReaderFromDMFile::load(ctx, dmfile);
    return vec_index;
}

void DMFileInputStreamProvideVectorIndex::setReturnRows(IProvideVectorIndex::SearchResultView sorted_results_)
{
    sorted_results = sorted_results_;
    const auto sorted_results_view = sorted_results.view;

    // Vector index is very likely to filter out some packs. For example,
    // if we query for Top 1, then only 1 pack will be remained. So we
    // update the reader's read_block_infos to avoid reading unnecessary data for other columns.

    // The following logic is nearly the same with DMFileReader::initReadBlockInfos.

    auto & read_block_infos = rest_col_reader.read_block_infos;
    const auto & pack_offset = rest_col_reader.pack_offset;

    read_block_infos.clear();
    const auto & pack_stats = dmfile->getPackStats();
    const auto & pack_res = rest_col_reader.pack_filter->getPackRes();

    // Update valid_packs_before_search
    {
        ctx->perf->n_dm_searches += 1;
        ctx->perf->dm_packs_in_file += pack_stats.size();
        for (const auto res : pack_res)
            ctx->perf->dm_packs_before_search += res.isUse();
    }

    // Update read_block_infos
    size_t start_pack_id = 0;
    size_t read_rows = 0;
    auto prev_block_pack_res = RSResult::All;
    auto sorted_results_it = sorted_results_view.begin();
    size_t pack_id = 0;
    for (; pack_id < pack_stats.size(); ++pack_id)
    {
        if (sorted_results_it == sorted_results_view.end())
            break;
        const auto begin = std::lower_bound( //
            sorted_results_it,
            sorted_results_view.end(),
            pack_offset[pack_id],
            [](const auto & lhs, const auto & rhs) { return lhs.rowid < rhs; });
        const auto end = std::lower_bound( //
            begin,
            sorted_results_view.end(),
            pack_offset[pack_id] + pack_stats[pack_id].rows,
            [](const auto & lhs, const auto & rhs) { return lhs.rowid < rhs; });
        bool is_use = begin != end;
        bool reach_limit = read_rows >= rest_col_reader.rows_threshold_per_read;
        bool break_all_match = prev_block_pack_res.allMatch() && !pack_res[pack_id].allMatch()
            && read_rows >= rest_col_reader.rows_threshold_per_read / 2;

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
    {
        for (const auto & block_info : read_block_infos)
            ctx->perf->dm_packs_after_search += block_info.pack_count;
    }

    RUNTIME_CHECK_MSG(sorted_results_it == sorted_results_view.end(), "All results are not consumed");
}

Block DMFileInputStreamProvideVectorIndex::getHeader() const
{
    return ctx->header;
}


} // namespace DB::DM
