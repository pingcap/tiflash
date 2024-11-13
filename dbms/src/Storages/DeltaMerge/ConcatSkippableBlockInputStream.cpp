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

#include <Storages/DeltaMerge/ConcatSkippableBlockInputStream.h>
#include <Storages/DeltaMerge/ScanContext.h>

#include <algorithm>

namespace DB::DM
{

template <bool need_row_id>
ConcatSkippableBlockInputStream<need_row_id>::ConcatSkippableBlockInputStream(
    SkippableBlockInputStreams inputs_,
    const ScanContextPtr & scan_context_)
    : rows(inputs_.size(), 0)
    , precede_stream_rows(0)
    , scan_context(scan_context_)
    , lac_bytes_collector(scan_context_ ? scan_context_->resource_group_name : "")
{
    children.insert(children.end(), inputs_.begin(), inputs_.end());
    current_stream = children.begin();
}

template <bool need_row_id>
ConcatSkippableBlockInputStream<need_row_id>::ConcatSkippableBlockInputStream(
    SkippableBlockInputStreams inputs_,
    std::vector<size_t> && rows_,
    const ScanContextPtr & scan_context_)
    : rows(std::move(rows_))
    , precede_stream_rows(0)
    , scan_context(scan_context_)
    , lac_bytes_collector(scan_context_ ? scan_context_->resource_group_name : "")
{
    children.insert(children.end(), inputs_.begin(), inputs_.end());
    current_stream = children.begin();
}

template <bool need_row_id>
void ConcatSkippableBlockInputStream<need_row_id>::appendChild(SkippableBlockInputStreamPtr child, size_t rows_)
{
    children.emplace_back(std::move(child));
    rows.push_back(rows_);
    current_stream = children.begin();
}

template <bool need_row_id>
bool ConcatSkippableBlockInputStream<need_row_id>::getSkippedRows(size_t & skip_rows)
{
    skip_rows = 0;
    while (current_stream != children.end())
    {
        auto * skippable_stream = dynamic_cast<SkippableBlockInputStream *>((*current_stream).get());

        size_t skip;
        bool has_next_block = skippable_stream->getSkippedRows(skip);
        skip_rows += skip;

        if (has_next_block)
        {
            return true;
        }
        else
        {
            (*current_stream)->readSuffix();
            precede_stream_rows += rows[current_stream - children.begin()];
            ++current_stream;
        }
    }

    return false;
}

template <bool need_row_id>
size_t ConcatSkippableBlockInputStream<need_row_id>::skipNextBlock()
{
    while (current_stream != children.end())
    {
        auto * skippable_stream = dynamic_cast<SkippableBlockInputStream *>((*current_stream).get());

        size_t skipped_rows = skippable_stream->skipNextBlock();

        if (skipped_rows > 0)
        {
            return skipped_rows;
        }
        else
        {
            (*current_stream)->readSuffix();
            precede_stream_rows += rows[current_stream - children.begin()];
            ++current_stream;
        }
    }
    return 0;
}

template <bool need_row_id>
Block ConcatSkippableBlockInputStream<need_row_id>::readWithFilter(const IColumn::Filter & filter)
{
    Block res;

    while (current_stream != children.end())
    {
        auto * skippable_stream = dynamic_cast<SkippableBlockInputStream *>((*current_stream).get());
        res = skippable_stream->readWithFilter(filter);

        if (res)
        {
            res.setStartOffset(res.startOffset() + precede_stream_rows);
            addReadBytes(res.bytes());
            break;
        }
        else
        {
            (*current_stream)->readSuffix();
            precede_stream_rows += rows[current_stream - children.begin()];
            ++current_stream;
        }
    }
    return res;
}

template <bool need_row_id>
Block ConcatSkippableBlockInputStream<need_row_id>::read(FilterPtr & res_filter, bool return_filter)
{
    Block res;

    while (current_stream != children.end())
    {
        res = (*current_stream)->read(res_filter, return_filter);

        if (res)
        {
            res.setStartOffset(res.startOffset() + precede_stream_rows);
            if constexpr (need_row_id)
            {
                res.setSegmentRowIdCol(createSegmentRowIdCol(res.startOffset(), res.rows()));
            }
            addReadBytes(res.bytes());
            break;
        }
        else
        {
            (*current_stream)->readSuffix();
            precede_stream_rows += rows[current_stream - children.begin()];
            ++current_stream;
        }
    }

    return res;
}

template <bool need_row_id>
ColumnPtr ConcatSkippableBlockInputStream<need_row_id>::createSegmentRowIdCol(UInt64 start, UInt64 limit)
{
    auto seg_row_id_col = ColumnUInt32::create();
    ColumnUInt32::Container & res = seg_row_id_col->getData();
    res.resize(limit);
    for (UInt64 i = 0; i < limit; ++i)
    {
        res[i] = i + start;
    }
    return seg_row_id_col;
}

template <bool need_row_id>
void ConcatSkippableBlockInputStream<need_row_id>::addReadBytes(UInt64 bytes)
{
    if (likely(scan_context != nullptr))
    {
        scan_context->user_read_bytes += bytes;
        if constexpr (!need_row_id)
        {
            lac_bytes_collector.collect(bytes);
        }
    }
}

template class ConcatSkippableBlockInputStream<false>;
template class ConcatSkippableBlockInputStream<true>;

void ConcatVectorIndexBlockInputStream::load()
{
    if (loaded || topk == 0)
        return;

    UInt32 precedes_rows = 0;
    std::vector<VectorIndexViewer::SearchResult> search_results;
    for (size_t i = 0; i < stream->children.size(); ++i)
    {
        if (auto * index_stream = index_streams[i]; index_stream)
        {
            auto sr = index_stream->load();
            for (auto & row : sr)
                row.key += precedes_rows;
            search_results.insert(search_results.end(), sr.begin(), sr.end());
        }
        precedes_rows += stream->rows[i];
    }

    // Keep the top k minimum distances rows.
    auto select_size = search_results.size() > topk ? topk : search_results.size();
    auto top_k_end = search_results.begin() + select_size;
    std::nth_element(search_results.begin(), top_k_end, search_results.end(), [](const auto & lhs, const auto & rhs) {
        return lhs.distance < rhs.distance;
    });
    search_results.resize(select_size);
    std::vector<UInt32> selected_rows;
    selected_rows.reserve(search_results.size());
    for (const auto & row : search_results)
        selected_rows.push_back(row.key);
    // Sort by key again.
    std::sort(selected_rows.begin(), selected_rows.end());

    precedes_rows = 0;
    auto sr_it = selected_rows.begin();
    for (size_t i = 0; i < stream->children.size(); ++i)
    {
        auto begin = std::lower_bound(sr_it, selected_rows.end(), precedes_rows);
        auto end = std::lower_bound(begin, selected_rows.end(), precedes_rows + stream->rows[i]);
        // Convert to local offset.
        for (auto it = begin; it != end; ++it)
            *it -= precedes_rows;
        if (auto * index_stream = index_streams[i]; index_stream)
            index_stream->setSelectedRows({begin, end});
        else
            RUNTIME_CHECK(begin == end);
        precedes_rows += stream->rows[i];
        sr_it = end;
    }

    // Not used anymore, release memory.
    index_streams.clear();
    loaded = true;
}

SkippableBlockInputStreamPtr ConcatVectorIndexBlockInputStream::build(
    std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream,
    const ANNQueryInfoPtr & ann_query_info)
{
    if (!ann_query_info)
        return stream;
    bool has_vector_index_stream = false;
    std::vector<VectorIndexBlockInputStream *> index_streams;
    index_streams.reserve(stream->children.size());
    for (const auto & sub_stream : stream->children)
    {
        if (auto * index_stream = dynamic_cast<VectorIndexBlockInputStream *>(sub_stream.get()); index_stream)
        {
            has_vector_index_stream = true;
            index_streams.push_back(index_stream);
            continue;
        }
        index_streams.push_back(nullptr);
    }
    if (!has_vector_index_stream)
        return stream;

    return std::make_shared<ConcatVectorIndexBlockInputStream>(
        stream,
        std::move(index_streams),
        ann_query_info->top_k());
}

} // namespace DB::DM
