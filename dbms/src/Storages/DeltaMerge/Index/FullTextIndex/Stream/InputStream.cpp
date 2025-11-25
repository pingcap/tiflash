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
#include <Columns/countBytesInFilter.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Perf.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Reader.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/BruteScoreInputStream.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/IProvideFullTextIndex.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/InputStream.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <clara_fts/src/index_reader.rs.h>

namespace DB::DM
{

void FullTextIndexInputStream::initSearchResults()
{
    if (searchResultsInited)
        return;

    UInt32 precedes_rows = 0;
    auto search_results = std::make_shared<std::vector<IProvideFullTextIndex::SearchResult>>();
    // Note, we do not reserve size=topN, because we first append all matching results, then perform the topn.
    search_results->reserve(bitmap_filter->size() / 2);

    // 1. Do full text search for all index streams.
    for (size_t i = 0, i_max = stream->children.size(); i < i_max; ++i)
    {
        if (auto * index_stream = index_streams[i]; index_stream)
        {
            auto reader = index_stream->getFullTextIndexReader();
            RUNTIME_CHECK(reader != nullptr);
            auto current_filter = BitmapFilterView(bitmap_filter, precedes_rows, stream->rows[i]);
            reader->searchScored(ctx->perf, ctx->fts_query_info->query_text(), current_filter, ctx->results);
            const size_t results_n = ctx->results.size();
            for (size_t i = 0; i < results_n; ++i)
            {
                search_results->emplace_back(IProvideFullTextIndex::SearchResult{
                    // We need to sort globally so convert it to a global offset temporarily.
                    // We will convert it back to local offset when we feed it back to substreams.
                    .rowid = ctx->results[i].doc_id + precedes_rows,
                    .score = ctx->results[i].score,
                });
            }
        }
        precedes_rows += stream->rows[i];
    }

    // 2. Keep the top k minimum scored rows.
    // [0, top_k) will be the top k largest score rows. (However it is not sorted)
    const auto top_k = ctx->fts_query_info->top_k();
    if (top_k < search_results->size())
    {
        std::nth_element( //
            search_results->begin(),
            search_results->begin() + top_k,
            search_results->end(),
            [](const auto & lhs, const auto & rhs) { return lhs.score > rhs.score; });
        search_results->resize(top_k);
    }

    // 3. Sort by rowid for the first K rows.
    std::sort( //
        search_results->begin(),
        search_results->end(),
        [](const auto & lhs, const auto & rhs) { return lhs.rowid < rhs.rowid; });

    // 4. Finally, notify all index streams to only return these rows.
    precedes_rows = 0;
    auto sr_it = search_results->begin();
    for (size_t i = 0, i_max = stream->children.size(); i < i_max; ++i)
    {
        if (auto * index_stream = index_streams[i]; index_stream)
        {
            auto reader = index_stream->getFullTextIndexReader();
            RUNTIME_CHECK(reader != nullptr);
            auto begin = std::lower_bound( //
                sr_it,
                search_results->end(),
                precedes_rows,
                [](const auto & lhs, const auto & rhs) { return lhs.rowid < rhs; });
            auto end = std::lower_bound( //
                begin,
                search_results->end(),
                precedes_rows + stream->rows[i],
                [](const auto & lhs, const auto & rhs) { return lhs.rowid < rhs; });
            // Convert back to local offset.
            for (auto it = begin; it != end; ++it)
                it->rowid -= precedes_rows;
            index_stream->setReturnRows(IProvideFullTextIndex::SearchResultView{
                .owner = search_results,
                .view = std::span<IProvideFullTextIndex::SearchResult>{
                    &*begin,
                    static_cast<size_t>(std::distance(begin, end))}});
            sr_it = end;
        }
        precedes_rows += stream->rows[i];
    }

    searchResultsInited = true;
}

Block FullTextIndexInputStream::read()
{
    initSearchResults();

    auto block = stream->read();
    if (!block)
    {
        onReadFinished();
        return {};
    }

    // The block may either comes from IProvideFullTextIndex, or BruteScoreInputStream.
    // Both streams are already producing MVCC filtered results.

    return block;
}

void FullTextIndexInputStream::onReadFinished()
{
    if (isReadFinished)
        return;
    isReadFinished = true;

    // For some reason it is too late if we report this in the destructor.
    if (ctx->dm_context != nullptr && ctx->dm_context->scan_context != nullptr)
    {
        auto scan_context = ctx->dm_context->scan_context;
        scan_context->fts_n_from_inmemory_noindex += ctx->perf->n_from_inmemory_noindex;
        scan_context->fts_n_from_tiny_index += ctx->perf->n_from_tiny_index;
        scan_context->fts_n_from_tiny_noindex += ctx->perf->n_from_tiny_noindex;
        scan_context->fts_n_from_dmf_index += ctx->perf->n_from_dmf_index;
        scan_context->fts_n_from_dmf_noindex += ctx->perf->n_from_dmf_noindex;
        scan_context->fts_rows_from_inmemory_noindex += ctx->perf->rows_from_inmemory_noindex;
        scan_context->fts_rows_from_tiny_index += ctx->perf->rows_from_tiny_index;
        scan_context->fts_rows_from_tiny_noindex += ctx->perf->rows_from_tiny_noindex;
        scan_context->fts_rows_from_dmf_index += ctx->perf->rows_from_dmf_index;
        scan_context->fts_rows_from_dmf_noindex += ctx->perf->rows_from_dmf_noindex;
        scan_context->fts_idx_load_total_ms += ctx->perf->idx_load_total_ms;
        scan_context->fts_idx_load_from_cache += ctx->perf->idx_load_from_cache;
        scan_context->fts_idx_load_from_column_file += ctx->perf->idx_load_from_column_file;
        scan_context->fts_idx_load_from_stable_s3 += ctx->perf->idx_load_from_stable_s3;
        scan_context->fts_idx_load_from_stable_disk += ctx->perf->idx_load_from_stable_disk;
        scan_context->fts_idx_search_n += ctx->perf->idx_search_n;
        scan_context->fts_idx_search_total_ms += ctx->perf->idx_search_total_ms;
        scan_context->fts_idx_dm_search_rows += ctx->perf->idx_dm_search_rows;
        scan_context->fts_idx_dm_total_read_fts_ms += ctx->perf->idx_dm_total_read_fts_ms;
        scan_context->fts_idx_dm_total_read_others_ms += ctx->perf->idx_dm_total_read_others_ms;
        scan_context->fts_idx_tiny_search_rows += ctx->perf->idx_tiny_search_rows;
        scan_context->fts_idx_tiny_total_read_fts_ms += ctx->perf->idx_tiny_total_read_fts_ms;
        scan_context->fts_idx_tiny_total_read_others_ms += ctx->perf->idx_tiny_total_read_others_ms;
        scan_context->fts_brute_total_read_ms += ctx->perf->brute_total_read_ms;
        scan_context->fts_brute_total_search_ms += ctx->perf->brute_total_search_ms;
    }
}

FullTextIndexInputStream::FullTextIndexInputStream(
    const FullTextIndexStreamCtxPtr & ctx_,
    const BitmapFilterPtr & bitmap_filter_,
    std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream_)
    : ctx(ctx_)
    , bitmap_filter(bitmap_filter_)
    , stream(stream_)
    , log(Logger::get(ctx->tracing_id))
{
    RUNTIME_CHECK(bitmap_filter != nullptr);
    RUNTIME_CHECK(stream != nullptr);

    UInt32 precedes_rows = 0;
    index_streams.reserve(stream->children.size());
    for (size_t i = 0, i_max = stream->children.size(); i < i_max; ++i)
    {
        auto * child = stream->children[i].get();

        if (auto * index_stream = dynamic_cast<IProvideFullTextIndex *>(child); index_stream)
        {
            index_streams.push_back(index_stream);
        }
        else if (auto * brute_stream = dynamic_cast<FullTextBruteScoreInputStream *>(child); brute_stream)
        {
            index_streams.push_back(nullptr);
            brute_stream->setBitmapFilter(BitmapFilterView(bitmap_filter, precedes_rows, stream->rows[i]));
        }
        else
        {
            RUNTIME_CHECK_MSG(
                false,
                "Unexpected stream type in FullTextIndexInputStream, only support IProvideFullTextIndex and "
                "FullTextBruteScoreInputStream inside ConcatSkippableBlockInputStream");
        }
        precedes_rows += stream->rows[i];
    }
}

} // namespace DB::DM
#endif
