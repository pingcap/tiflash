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
#include <Storages/DeltaMerge/Index/FullTextIndex/Perf.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Reader.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/DMFileInputStream.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/ReaderFromDMFile.h>


namespace DB::DM
{

DMFileInputStreamProvideFullTextIndex::DMFileInputStreamProvideFullTextIndex(
    const FullTextIndexStreamCtxPtr & ctx_,
    const DMFilePtr & dmfile_,
    DMFileReader && rest_col_reader_)
    : ctx(ctx_)
    , dmfile(dmfile_)
    , rest_col_reader(std::move(rest_col_reader_))
{
    RUNTIME_CHECK(dmfile != nullptr);
}

Block DMFileInputStreamProvideFullTextIndex::read()
{
    // We expect setReturnRows() is called before doing any read().
    RUNTIME_CHECK(sorted_results.owner != nullptr);
    RUNTIME_CHECK(fts_index != nullptr);

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

    // Read fts column from index
    // Not that it is possible that the fts column is not needed in the output,
    // so fts_col_p could be nullptr.
    MutableColumnPtr fts_col_p = nullptr;
    if (ctx->fts_idx_in_schema.has_value())
    {
        RUNTIME_CHECK(ctx->fts_cd_in_schema.has_value());
        fts_col_p = ctx->fts_cd_in_schema->type->createColumn();
        fts_col_p->reserve(block_selected_rows.size());
        for (const auto & row : block_selected_rows)
        {
            fts_index->get(row.rowid, ctx->text_value);
            fts_col_p->insertData(ctx->text_value.data(), ctx->text_value.size());
        }
    }
    // Fill score column
    MutableColumnPtr score_col_p = nullptr;
    {
        score_col_p = ColumnFloat32::create();
        auto * score_col = typeid_cast<ColumnFloat32 *>(score_col_p.get());
        RUNTIME_CHECK(score_col != nullptr);
        score_col->reserve(block_selected_rows.size());
        for (const auto & row : block_selected_rows)
            score_col->insert(row.score);
    }

    ctx->perf->idx_dm_search_rows += block_selected_rows.size();
    ctx->perf->idx_dm_total_read_fts_ms += w.elapsedMillisecondsFromLastTime();

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

    ctx->perf->idx_dm_total_read_others_ms += w.elapsedMillisecondsFromLastTime();

    if (ctx->fts_idx_in_schema.has_value())
    {
        RUNTIME_CHECK(ctx->fts_cd_in_schema.has_value());
        block.insert(
            ctx->fts_idx_in_schema.value(),
            ColumnWithTypeAndName{//
                                  std::move(fts_col_p),
                                  ctx->fts_cd_in_schema->type,
                                  ctx->fts_cd_in_schema->name,
                                  ctx->fts_cd_in_schema->id});
    }
    {
        block.insert(ColumnWithTypeAndName( //
            std::move(score_col_p),
            ctx->score_cd_in_schema.type,
            ctx->score_cd_in_schema.name,
            ctx->score_cd_in_schema.id));
    }

    block.setStartOffset(start_row_offset);
    block.setRSResult(rs_result);
    return block;
}

FullTextIndexReaderPtr DMFileInputStreamProvideFullTextIndex::getFullTextIndexReader()
{
    if (fts_index != nullptr)
        return fts_index;
    fts_index = FullTextIndexReaderFromDMFile::load(ctx, dmfile);
    return fts_index;
}

void DMFileInputStreamProvideFullTextIndex::setReturnRows(IProvideFullTextIndex::SearchResultView sorted_results_)
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

    RUNTIME_CHECK_MSG(sorted_results_it == sorted_results_view.end(), "All results are not consumed");
}

Block DMFileInputStreamProvideFullTextIndex::getHeader() const
{
    return ctx->schema_as_header;
}


} // namespace DB::DM
