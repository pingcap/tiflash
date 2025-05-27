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
#include <Functions/FunctionHelpers.h>
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

    // read vector or distance column from index
    MutableColumnPtr vec_column = nullptr;
    if (ctx->vec_cd.has_value())
    {
        vec_column = ctx->vec_cd->type->createColumn();
        vec_column->reserve(block_selected_rows.size());
    }

    MutableColumnPtr dis_column = nullptr;
    if (ctx->dis_ctx.has_value())
    {
        RUNTIME_CHECK(ctx->ann_query_info->enable_distance_proj());
        dis_column = ctx->dis_ctx->dis_cd.type->createColumn();
        dis_column->reserve(block_selected_rows.size());
    }

    // The index stores non-squared L2 distances. To ensure consistent behavior between indexed and non-indexed queries,
    // we square the distances from the index when using L2 metric when distance-proj is enable.
    if (ctx->ann_query_info->enable_distance_proj())
    {
        RUNTIME_CHECK(dis_column->isColumnNullable());

        auto * dis_nullable = typeid_cast<ColumnNullable *>(dis_column.get());
        RUNTIME_CHECK(dis_nullable != nullptr);

        auto * dis_float32 = typeid_cast<ColumnFloat32 *>(&dis_nullable->getNestedColumn());
        RUNTIME_CHECK(dis_float32 != nullptr);

        auto & null_data = dis_nullable->getNullMapData();

        if (ctx->ann_query_info->distance_metric() == tipb::VectorDistanceMetric::L2)
        {
            for (const auto & row : block_selected_rows)
            {
                dis_float32->insert(std::sqrt(row.distance));
            }
        }
        else
        {
            for (const auto & row : block_selected_rows)
            {
                dis_float32->insert(row.distance);
            }
        }
        null_data.resize_fill(block_selected_rows.size(), 0);
    }
    else
    {
        RUNTIME_CHECK(vec_column != nullptr);
        for (const auto & row : block_selected_rows)
        {
            vec_index->get(row.rowid, ctx->vector_value);
            vec_column->insertData(
                reinterpret_cast<const char *>(ctx->vector_value.data()),
                ctx->vector_value.size() * sizeof(Float32));
        }
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

    // vec_column and dis_column are exclusive to each other.
    RUNTIME_CHECK((vec_column != nullptr) != (dis_column != nullptr));

    if (vec_column != nullptr)
    {
        RUNTIME_CHECK(ctx->vec_col_idx.has_value());
        block.insert(
            ctx->vec_col_idx.value(),
            ColumnWithTypeAndName{
                std::move(vec_column),
                ctx->vec_cd->type,
                ctx->vec_cd->name,
                ctx->vec_cd->id,
            });
    }
    if (dis_column != nullptr)
    {
        // Note that distance column is ensured to be the last column in schema (and we have checked that in Ctx)
        block.insert(ColumnWithTypeAndName{
            std::move(dis_column),
            ctx->dis_ctx->dis_cd.type,
            ctx->dis_ctx->dis_cd.name,
            ctx->dis_ctx->dis_cd.id,
        });
    }
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
    rest_col_reader.read_block_infos = ReadBlockInfo::createWithRowIDs(
        sorted_results_view,
        rest_col_reader.pack_offset,
        rest_col_reader.pack_filter->getPackRes(),
        dmfile->getPackStats(),
        rest_col_reader.rows_threshold_per_read);

    // Update valid_packs_after_search
    for (const auto & block_info : rest_col_reader.read_block_infos)
        ctx->perf->dm_packs_after_search += block_info.pack_count;
}

Block DMFileInputStreamProvideVectorIndex::getHeader() const
{
    return ctx->header;
}


} // namespace DB::DM
