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

#include <Columns/countBytesInFilter.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Perf.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Reader.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/Ctx.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/IProvideVectorIndex.h>
#include <Storages/DeltaMerge/Index/VectorIndex/Stream/InputStream.h>
#include <Storages/DeltaMerge/ScanContext.h>

namespace DB::DM
{

void VectorIndexInputStream::initSearchResults()
{
    if (searchResultsInited)
        return;

    UInt32 precedes_rows = 0;
    auto search_results = std::make_shared<std::vector<IProvideVectorIndex::SearchResult>>();
    search_results->reserve(ctx->ann_query_info->top_k());

    // 1. Do vector search for all index streams.
    for (size_t i = 0, i_max = stream->children.size(); i < i_max; ++i)
    {
        if (auto * index_stream = index_streams[i]; index_stream)
        {
            auto reader = index_stream->getVectorIndexReader();
            RUNTIME_CHECK(reader != nullptr);
            auto current_filter = BitmapFilterView(bitmap_filter, precedes_rows, stream->rows[i]);
            auto results = reader->search(ctx->ann_query_info, current_filter);
            const size_t results_n = results.size();
            VectorIndexReader::Key last_rowid = std::numeric_limits<VectorIndexReader::Key>::max();
            for (size_t i = 0; i < results_n; ++i)
            {
                const auto rowid = results[i].member.key;
                if (rowid == last_rowid)
                    continue; // Perform a very simple deduplicate
                last_rowid = rowid;
                // The result from usearch may contain filtered out rows so we filter again
                if (current_filter[rowid])
                    search_results->emplace_back(IProvideVectorIndex::SearchResult{
                        // We need to sort globally so convert it to a global offset temporarily.
                        // We will convert it back to local offset when we feed it back to substreams.
                        .rowid = rowid + precedes_rows,
                        .distance = results[i].distance,
                    });
            }
        }
        precedes_rows += stream->rows[i];
    }

    // 2. Keep the top k minimum distances rows.
    // [0, top_k) will be the top k minimum distances rows. (However it is not sorted)
    const auto top_k = ctx->ann_query_info->top_k();
    if (top_k < search_results->size())
    {
        std::nth_element( //
            search_results->begin(),
            search_results->begin() + top_k,
            search_results->end(),
            [](const auto & lhs, const auto & rhs) { return lhs.distance < rhs.distance; });
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
            auto reader = index_stream->getVectorIndexReader();
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
            index_stream->setReturnRows(IProvideVectorIndex::SearchResultView{
                .owner = search_results,
                .view = std::span<IProvideVectorIndex::SearchResult>{
                    &*begin,
                    static_cast<size_t>(std::distance(begin, end))}});
            sr_it = end;
        }
        precedes_rows += stream->rows[i];
    }

    if (ctx->dm_context != nullptr && ctx->dm_context->scan_context != nullptr)
    {
        auto scan_context = ctx->dm_context->scan_context;
        scan_context->vector_idx_load_from_s3 += ctx->perf->load_from_stable_s3;
        scan_context->vector_idx_load_from_disk += ctx->perf->load_from_stable_disk + ctx->perf->load_from_column_file;
        scan_context->vector_idx_load_from_cache += ctx->perf->load_from_cache;
        scan_context->vector_idx_load_time_ms += ctx->perf->total_load_ms;
        scan_context->vector_idx_search_time_ms += ctx->perf->total_search_ms;
        scan_context->vector_idx_search_visited_nodes += ctx->perf->visited_nodes;
        scan_context->vector_idx_search_discarded_nodes += ctx->perf->discarded_nodes;
    }

    searchResultsInited = true;
}

VectorIndexInputStream::~VectorIndexInputStream()
{
    LOG_DEBUG(
        log,
        "Vector search reading finished, "
        "load_index={:.3f}s (from:[cf/dmf]={}/{} noindex:[cf/dmf]={}/{} [cached/cf_data/dmf_disk/dmf_s3]={}/{}/{}/{}), "
        "vec_search={:.3f}s, "
        "vec_get_[cf/dmf]={:.3f}s/{:.3f}s, "
        "other_get_[cf/dmf]={:.3f}s/{:.3f}s, "
        "pack_[before/after]={}/{}, "
        "top_k_[query/visited/discarded/result]={}/{}/{}/{}",
        static_cast<double>(ctx->perf->total_load_ms) / 1000.0,
        ctx->perf->n_from_cf_index,
        ctx->perf->n_from_dmf_index,
        ctx->perf->n_from_cf_noindex,
        ctx->perf->n_from_dmf_noindex,
        ctx->perf->load_from_cache,
        ctx->perf->load_from_column_file,
        ctx->perf->load_from_stable_disk,
        ctx->perf->load_from_stable_s3,
        static_cast<double>(ctx->perf->total_search_ms) / 1000.0,
        static_cast<double>(ctx->perf->total_cf_read_vec_ms) / 1000.0,
        static_cast<double>(ctx->perf->total_dm_read_vec_ms) / 1000.0,
        static_cast<double>(ctx->perf->total_cf_read_others_ms) / 1000.0,
        static_cast<double>(ctx->perf->total_dm_read_others_ms) / 1000.0,
        ctx->perf->dm_packs_before_search,
        ctx->perf->dm_packs_after_search,
        ctx->ann_query_info->top_k(),
        ctx->perf->visited_nodes,
        ctx->perf->discarded_nodes,
        ctx->perf->returned_nodes);
}

Block VectorIndexInputStream::read()
{
    initSearchResults();

    auto block = stream->read();
    if (!block)
    {
        onReadFinished();
        return {};
    }

    // The block read from `IProvideVectorIndex` will only return the selected rows after global TopK. Return it directly.
    // MVCC has already been done when searching the vector index.
    // For streams which are not `IProvideVectorIndex`, the block should be filtered by MVCC bitmap.
    if (auto idx = std::distance(stream->children.begin(), stream->current_stream); !index_streams[idx])
    {
        ctx->filter.resize(block.rows());
        if (bool all_match = bitmap_filter->get(ctx->filter, block.startOffset(), block.rows()); all_match)
            return block;

        size_t passed_count = countBytesInFilter(ctx->filter);
        for (auto & col : block)
            col.column = col.column->filter(ctx->filter, passed_count);
    }

    return block;
}

void VectorIndexInputStream::onReadFinished()
{
    if (isReadFinished)
        return;
    isReadFinished = true;

    // For some reason it is too late if we report this in the destructor.
    if (ctx->dm_context != nullptr && ctx->dm_context->scan_context != nullptr)
    {
        auto scan_context = ctx->dm_context->scan_context;
        scan_context->vector_idx_read_vec_time_ms += ctx->perf->total_cf_read_vec_ms + ctx->perf->total_dm_read_vec_ms;
        scan_context->vector_idx_read_others_time_ms
            += ctx->perf->total_cf_read_others_ms + ctx->perf->total_dm_read_others_ms;
    }
}

VectorIndexInputStream::VectorIndexInputStream(
    const VectorIndexStreamCtxPtr & ctx_,
    const BitmapFilterPtr & bitmap_filter_,
    std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream_)
    : ctx(ctx_)
    , bitmap_filter(bitmap_filter_)
    , stream(stream_)
    , log(Logger::get(ctx->tracing_id))
{
    RUNTIME_CHECK(bitmap_filter != nullptr);
    RUNTIME_CHECK(stream != nullptr);

    index_streams.reserve(stream->children.size());
    for (const auto & sub_stream : stream->children)
    {
        if (auto * index_stream = dynamic_cast<IProvideVectorIndex *>(sub_stream.get()); index_stream)
            index_streams.push_back(index_stream);
        else
            index_streams.push_back(nullptr);
    }
}

} // namespace DB::DM
