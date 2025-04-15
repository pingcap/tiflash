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
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInputStream.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Perf.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Reader.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/ColumnFileInputStream.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/ReaderFromColumnFileTiny.h>

namespace DB::DM
{

SkippableBlockInputStreamPtr ColumnFileProvideVectorIndexInputStream::createOrFallback(
    const VectorIndexStreamCtxPtr & ctx,
    const ColumnFilePtr & column_file)
{
    RUNTIME_CHECK(ctx->data_provider != nullptr);
    RUNTIME_CHECK(ctx->dm_context != nullptr);

    auto fallback = [&] {
        ctx->perf->n_from_cf_noindex += 1;
        return ColumnFileInputStream::create(
            *ctx->dm_context,
            column_file,
            ctx->data_provider,
            ctx->col_defs,
            ctx->read_tag);
    };

    const auto tiny_file = std::dynamic_pointer_cast<ColumnFileTiny>(column_file);
    if (!tiny_file)
        return fallback();
    const auto * index_info = tiny_file->findIndexInfo(ctx->ann_query_info->index_id());
    if (!index_info)
        return fallback();

    ctx->perf->n_from_cf_index += 1;
    return std::make_shared<ColumnFileProvideVectorIndexInputStream>(ctx, tiny_file);
}

VectorIndexReaderPtr ColumnFileProvideVectorIndexInputStream::getVectorIndexReader()
{
    if (vec_index != nullptr)
        return vec_index;
    vec_index = VectorIndexReaderFromColumnFileTiny::load(ctx, *tiny_file);
    return vec_index;
}

Block ColumnFileProvideVectorIndexInputStream::read()
{
    // We require setReturnRows to be called before read.
    // See ConcatInputStream for a caller, e.g.: VectorIndexInputStream{ConcatInputStream{ColumnFileProvideVectorIndexInputStream}}
    RUNTIME_CHECK(sorted_results.owner != nullptr);
    RUNTIME_CHECK(vec_index != nullptr);

    // All rows are filtered out (or there is already a read())
    if (sorted_results.view.empty())
        return {};

    Stopwatch w(CLOCK_MONOTONIC_COARSE);

    // read vector column from index
    auto vec_column = ctx->vec_cd.type->createColumn();
    vec_column->reserve(sorted_results.view.size());
    for (const auto & row : sorted_results.view)
    {
        vec_index->get(row.rowid, ctx->vector_value);
        vec_column->insertData(
            reinterpret_cast<const char *>(ctx->vector_value.data()),
            ctx->vector_value.size() * sizeof(Float32));
    }

    ctx->perf->n_cf_reads += 1;
    ctx->perf->total_cf_read_vec_ms += w.elapsedMillisecondsFromLastTime();

    // read other column from ColumnFileTinyReader
    // TODO: Optimize: We should be able to only read a few rows, instead of reading all rows then filter out.
    Block block;
    if (!ctx->rest_col_defs->empty())
    {
        auto reader = tiny_file->getReader(*ctx->dm_context, ctx->data_provider, ctx->rest_col_defs, ctx->read_tag);
        block = reader->readNextBlock();

        ctx->filter.clear();
        ctx->filter.resize_fill(tiny_file->getRows(), 0);
        for (const auto & row : sorted_results.view)
            ctx->filter[row.rowid] = 1;
        for (auto & col : block)
            col.column = col.column->filter(ctx->filter, sorted_results.view.size());

        RUNTIME_CHECK(block.rows() == sorted_results.view.size());
    }

    ctx->perf->total_cf_read_others_ms += w.elapsedMillisecondsFromLastTime();

    auto index = ctx->header.getPositionByName(ctx->vec_cd.name);
    block.insert(
        index,
        ColumnWithTypeAndName( //
            std::move(vec_column),
            ctx->vec_cd.type,
            ctx->vec_cd.name,
            ctx->vec_cd.id));

    // After a successful read, clear out the ordered_return_rows so that
    // the next read will just return an empty block.
    sorted_results.view = {};

    return block;
}

Block ColumnFileProvideVectorIndexInputStream::getHeader() const
{
    return ctx->header;
}

} // namespace DB::DM
