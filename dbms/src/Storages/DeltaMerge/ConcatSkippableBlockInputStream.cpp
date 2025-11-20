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

#include <Columns/countBytesInFilter.h>
#include <Storages/DeltaMerge/ConcatSkippableBlockInputStream.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/SegmentRowID.h>
#include <Storages/KVStore/Types.h>

namespace DB::DM
{

template <bool need_row_id>
ConcatSkippableBlockInputStream<need_row_id>::ConcatSkippableBlockInputStream(
    SkippableBlockInputStreams && inputs_,
    std::vector<size_t> && rows_,
    const ScanContextPtr & scan_context_,
    ReadTag read_tag_)
    : rows(std::move(rows_))
    , precede_stream_rows(0)
    , scan_context(scan_context_)
    , lac_bytes_collector(scan_context_ ? scan_context_->newLACBytesCollector(read_tag_) : std::nullopt)
    , read_tag(read_tag_)
{
    assert(rows.size() == inputs_.size());
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
        auto * skippable_stream = dynamic_cast<SkippableBlockInputStream *>(current_stream->get());

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
        auto * skippable_stream = dynamic_cast<SkippableBlockInputStream *>(current_stream->get());
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
Block ConcatSkippableBlockInputStream<need_row_id>::read()
{
    Block res;

    while (current_stream != children.end())
    {
        res = (*current_stream)->read();
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
void ConcatSkippableBlockInputStream<need_row_id>::addReadBytes(UInt64 bytes)
{
    if (likely(scan_context != nullptr))
        scan_context->addUserReadBytes(bytes, read_tag, lac_bytes_collector);
}

template class ConcatSkippableBlockInputStream<false>;
template class ConcatSkippableBlockInputStream<true>;

} // namespace DB::DM
