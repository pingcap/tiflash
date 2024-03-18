// Copyright 2023 PingCAP, Inc.
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

#pragma once

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/ScanContext.h>

namespace DB
{
namespace DM
{
class SkippableBlockInputStream : public IBlockInputStream
{
public:
    ~SkippableBlockInputStream() override = default;

    /// Return false if it is the end of stream.
    virtual bool getSkippedRows(size_t & skip_rows) = 0;

    /// Skip next block in the stream.
    /// Return the number of rows of the next block.
    /// Return 0 if failed to skip or the end of stream.
    virtual size_t skipNextBlock() = 0;

    /// Read specific rows of next block in the stream according to the filter.
    /// Return empty block if failed to read or the end of stream.
    /// Note: filter can not be all false.
    /// Only used in Late Materialization.
    virtual Block readWithFilter(const IColumn::Filter & filter) = 0;
};

using SkippableBlockInputStreamPtr = std::shared_ptr<SkippableBlockInputStream>;
using SkippableBlockInputStreams = std::vector<SkippableBlockInputStreamPtr>;

class EmptySkippableBlockInputStream : public SkippableBlockInputStream
{
public:
    explicit EmptySkippableBlockInputStream(const ColumnDefines & read_columns_)
        : read_columns(read_columns_)
    {}

    String getName() const override { return "EmptySkippable"; }

    Block getHeader() const override { return toEmptyBlock(read_columns); }

    bool getSkippedRows(size_t &) override { return false; }

    size_t skipNextBlock() override { return 0; }

    Block readWithFilter(const IColumn::Filter &) override { return {}; }

    Block read() override { return {}; }

private:
    ColumnDefines read_columns{};
};

template <bool need_row_id = false>
class ConcatSkippableBlockInputStream : public SkippableBlockInputStream
{
public:
    ConcatSkippableBlockInputStream(SkippableBlockInputStreams inputs_, const ScanContextPtr & scan_context_)
        : rows(inputs_.size(), 0)
        , precede_stream_rows(0)
        , scan_context(scan_context_)
        , lac_bytes_collector(scan_context_ ? scan_context_->resource_group_name : "")
    {
        children.insert(children.end(), inputs_.begin(), inputs_.end());
        current_stream = children.begin();
    }

    ConcatSkippableBlockInputStream(
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

    String getName() const override { return "ConcatSkippable"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

    bool getSkippedRows(size_t & skip_rows) override
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

    size_t skipNextBlock() override
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

    Block readWithFilter(const IColumn::Filter & filter) override
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

    Block read() override
    {
        FilterPtr filter = nullptr;
        return read(filter, false);
    }

    Block read(FilterPtr & res_filter, bool return_filter) override
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

private:
    ColumnPtr createSegmentRowIdCol(UInt64 start, UInt64 limit)
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
    void addReadBytes(UInt64 bytes)
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
    BlockInputStreams::iterator current_stream;
    std::vector<size_t> rows;
    size_t precede_stream_rows;
    const ScanContextPtr scan_context;
    LACBytesCollector lac_bytes_collector;
};

} // namespace DM
} // namespace DB
