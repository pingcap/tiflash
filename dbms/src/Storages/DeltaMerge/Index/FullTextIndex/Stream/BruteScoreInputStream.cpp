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
#include <Columns/ColumnString.h>
#include <Common/Stopwatch.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Perf.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/BruteScoreInputStream.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx.h>

namespace DB::DM
{

Block FullTextBruteScoreInputStream::read()
{
    Stopwatch w(CLOCK_MONOTONIC_COARSE);

    // Note: Currently score is only calculated per-block.
    // It's quality is not as high as calculating the score for whole data.
    auto block = children[0]->read();
    if (!block)
        return {};

    RUNTIME_CHECK(bitmap_filter.has_value()); // Must be initialized by a FullTextIndexInputStream.
    RUNTIME_CHECK(block.columns() == ctx->noindex_read_schema->size());

    ctx->perf->brute_total_read_ms += w.elapsedMillisecondsFromLastTime();

    // The last column of the inner stream is ensured to be the FTS column.
    {
        const auto fts_column = block.safeGetByPosition(block.columns() - 1).column;
        if (fts_column->isColumnNullable())
            checkAndGetNestedColumn<ColumnString>(fts_column.get());
        else
            checkAndGetColumn<ColumnString>(fts_column.get());

        auto & brute_searcher = ctx->ensureBruteScoredSearcher();
        brute_searcher.clear(); // Possibly reused from last read
        brute_searcher.reserve(block.rows());

        for (size_t i = 0, i_max = block.rows(); i < i_max; ++i)
        {
            // Even if the row is filtered, we still need to feed it into the searcher, as if it is not filtered out.
            // Otherwise it will cause different scores compared as the indexed one. (For indexed one, it includes all
            // data in the index, because it does not know which data will be filtered out when building the index).
            if (fts_column->isNullAt(i))
            {
                brute_searcher.add_null();
            }
            else
            {
                const auto fts_str = fts_column->getDataAt(i);
                brute_searcher.add_document(::rust::Str(fts_str.data, fts_str.size));
            }
        }

        // We filter rows only at the search time.
        const auto raw_filter = bitmap_filter->getRawSubFilter(block.startOffset(), block.rows());
        ClaraFTS::BitmapFilter filter;
        filter.match_partial = rust::Slice<const UInt8>(raw_filter.data(), raw_filter.size());
        filter.match_all = false;
        brute_searcher.search(filter, ctx->results);
    }

    ctx->perf->brute_total_search_ms += w.elapsedMillisecondsFromLastTime();

    if (ctx->results.empty())
    {
        return {};
    }

    // Try to reduce what we will produce to the upper layer. This could reduce RPC cost, because FullTextInputStream
    // will not do a TopK again for unindexed data.
    {
        const auto top_k = ctx->fts_query_info->top_k();
        if (top_k < ctx->results.size())
        {
            std::nth_element( //
                ctx->results.begin(),
                ctx->results.begin() + top_k,
                ctx->results.end(),
                [](const auto & lhs, const auto & rhs) { return lhs.score > rhs.score; });
            ctx->results.truncate(top_k);
        }
    }
    std::sort( //
        ctx->results.begin(),
        ctx->results.end(),
        [](const auto & lhs, const auto & rhs) { return lhs.doc_id < rhs.doc_id; });

    if (ctx->results.size() < block.rows())
    {
        ctx->filter.clear();
        ctx->filter.resize_fill(block.rows(), 0);
        for (size_t i = 0, i_max = ctx->results.size(); i < i_max; ++i)
        {
            RUNTIME_CHECK(ctx->results[i].doc_id < ctx->filter.size(), ctx->results[i].doc_id, ctx->filter.size());
            ctx->filter[ctx->results[i].doc_id] = 1;
        }
        for (auto & col : block)
            col.column = col.column->filter(ctx->filter, /* size_hint */ ctx->results.size());
    }

    // Finally, reassemble the block.
    // The current layout of block is `noindex_read_schema` which is `rest_col_schema + fts_col`.
    // The target layout of block is `schema` which is `rest_col_schema + (possibly fts column somewhere) + score_col`.
    {
        // 1. Remove the FTS column at last
        const auto fts_column_filtered = block.safeGetByPosition(block.columns() - 1).column;
        block.erase(block.columns() - 1);
        // 2. Add FTS column (if in the schema) at the correct position.
        // Note that we must use `ctx->fts_cd_in_schema` to ensure a correct column name, because fts_cd_in_schema comes from schema
        // while fts column in noindex_read_schema always use an arbitrary name.
        if (ctx->fts_idx_in_schema.has_value())
        {
            RUNTIME_CHECK(ctx->fts_cd_in_schema.has_value());
            block.insert(
                /* position */ *ctx->fts_idx_in_schema,
                ColumnWithTypeAndName( //
                    fts_column_filtered,
                    ctx->fts_cd_in_schema->type,
                    ctx->fts_cd_in_schema->name,
                    ctx->fts_cd_in_schema->id));
        }
        // 3. Add Score column at last
        auto score_column = ctx->score_cd_in_schema.type->createColumn();
        score_column->reserve(ctx->results.size());
        // ctx->results are already sorted by rowid, so let's just insert them sequentially.
        // An alternative is to first create a score Column with full length, then assign the scores according to rowid
        // and finally filter the column. This does not need to sort the `results` by `doc_id`. However this requires
        // allocating large bulk of memory and could be slower then the current approach.
        for (const auto & result : ctx->results)
            score_column->insert(static_cast<Float64>(result.score));
        block.insert(ColumnWithTypeAndName( //
            std::move(score_column),
            ctx->score_cd_in_schema.type,
            ctx->score_cd_in_schema.name,
            ctx->score_cd_in_schema.id));
    }

    RUNTIME_CHECK(block.columns() == ctx->schema->size());

    return block;
}

Block FullTextBruteScoreInputStream::getHeader() const
{
    // We are returning the schema as header, instead of the inner stream's header.
    return ctx->schema_as_header;
}

} // namespace DB::DM
#endif
