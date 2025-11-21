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

#include <Common/config.h>

#if ENABLE_CLARA
#include <Common/Stopwatch.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileInputStream.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Perf.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Reader.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/BruteScoreInputStream.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/ColumnFileInputStream.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/ReaderFromColumnFileTiny.h>


namespace DB::DM
{

SkippableBlockInputStreamPtr ColumnFileProvideFullTextIndexInputStream::createOrFallback(
    const FullTextIndexStreamCtxPtr & ctx,
    const ColumnFilePtr & column_file)
{
    RUNTIME_CHECK(ctx->data_provider != nullptr);
    RUNTIME_CHECK(ctx->dm_context != nullptr);

    auto fallback = [&] {
        auto full_col_stream = ColumnFileInputStream::create(
            *ctx->dm_context,
            column_file,
            ctx->data_provider,
            ctx->noindex_read_schema,
            ctx->read_tag);
        RUNTIME_CHECK(full_col_stream != nullptr);

        return FullTextBruteScoreInputStream::create(ctx, full_col_stream);
    };

    const auto tiny_file = std::dynamic_pointer_cast<ColumnFileTiny>(column_file);
    if (!tiny_file)
    {
        // Actually it could also be a CFDeleteRange or CFBig. But anyway this is rare.
        ctx->perf->n_from_inmemory_noindex += 1;
        ctx->perf->rows_from_inmemory_noindex += column_file->getRows();
        return fallback();
    }
    const auto * index_info = tiny_file->findIndexInfo(ctx->fts_query_info->index_id());
    if (!index_info)
    {
        ctx->perf->n_from_tiny_noindex += 1;
        ctx->perf->rows_from_tiny_noindex += column_file->getRows();
        return fallback();
    }

    ctx->perf->n_from_tiny_index += 1;
    ctx->perf->rows_from_tiny_index += column_file->getRows();
    return std::make_shared<ColumnFileProvideFullTextIndexInputStream>(ctx, tiny_file);
}

FullTextIndexReaderPtr ColumnFileProvideFullTextIndexInputStream::getFullTextIndexReader()
{
    if (fts_index != nullptr)
        return fts_index;
    fts_index = FullTextIndexReaderFromColumnFileTiny::load(ctx, *tiny_file);
    return fts_index;
}

Block ColumnFileProvideFullTextIndexInputStream::read()
{
    // We require setReturnRows to be called before read.
    // See ConcatInputStream for a caller, e.g.: FullTextIndexInputStream{ConcatInputStream{ColumnFileProvideFullTextIndexInputStream}}
    RUNTIME_CHECK(sorted_results.owner != nullptr);
    RUNTIME_CHECK(fts_index != nullptr);

    // All rows are filtered out (or there is already a read())
    if (sorted_results.view.empty())
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
        fts_col_p->reserve(sorted_results.view.size());
        for (const auto & row : sorted_results.view)
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
        score_col->reserve(sorted_results.view.size());
        for (const auto & row : sorted_results.view)
            score_col->insert(row.score);
    }

    ctx->perf->idx_tiny_search_rows += sorted_results.view.size();
    ctx->perf->idx_tiny_total_read_fts_ms += w.elapsedMillisecondsFromLastTime();

    // read other column from ColumnFileTinyReader
    // TODO: Optimize: We should be able to only read a few rows, instead of reading all rows then filter out.
    Block block;
    if (!ctx->rest_col_schema->empty())
    {
        auto reader = tiny_file->getReader(*ctx->dm_context, ctx->data_provider, ctx->rest_col_schema, ctx->read_tag);
        block = reader->readNextBlock();

        ctx->filter.clear();
        ctx->filter.resize_fill(tiny_file->getRows(), 0);
        for (const auto & row : sorted_results.view)
            ctx->filter[row.rowid] = 1;
        for (auto & col : block)
            col.column = col.column->filter(ctx->filter, sorted_results.view.size());

        RUNTIME_CHECK(block.rows() == sorted_results.view.size());
    }

    ctx->perf->idx_tiny_total_read_others_ms += w.elapsedMillisecondsFromLastTime();

    if (ctx->fts_idx_in_schema.has_value())
    {
        RUNTIME_CHECK(ctx->fts_cd_in_schema.has_value());
        block.insert(
            ctx->fts_idx_in_schema.value(),
            ColumnWithTypeAndName( //
                std::move(fts_col_p),
                ctx->fts_cd_in_schema->type,
                ctx->fts_cd_in_schema->name,
                ctx->fts_cd_in_schema->id));
    }
    {
        block.insert(ColumnWithTypeAndName( //
            std::move(score_col_p),
            ctx->score_cd_in_schema.type,
            ctx->score_cd_in_schema.name,
            ctx->score_cd_in_schema.id));
    }

    // After a successful read, clear out the ordered_return_rows so that
    // the next read will just return an empty block.
    sorted_results.view = {};

    return block;
}

Block ColumnFileProvideFullTextIndexInputStream::getHeader() const
{
    return ctx->schema_as_header;
}

} // namespace DB::DM
#endif
