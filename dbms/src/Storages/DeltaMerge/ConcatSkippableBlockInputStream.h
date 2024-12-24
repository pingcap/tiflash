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

#pragma once

#include <Flash/ResourceControl/LocalAdmissionController.h>
#include <Storages/DeltaMerge/Index/VectorIndex_fwd.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>
#include <Storages/DeltaMerge/VectorIndexBlockInputStream.h>


namespace DB::DM
{

template <bool need_row_id = false>
class ConcatSkippableBlockInputStream : public SkippableBlockInputStream
{
public:
    ConcatSkippableBlockInputStream(SkippableBlockInputStreams inputs_, const ScanContextPtr & scan_context_);

    ConcatSkippableBlockInputStream(
        SkippableBlockInputStreams inputs_,
        std::vector<size_t> && rows_,
        const ScanContextPtr & scan_context_);

    void appendChild(SkippableBlockInputStreamPtr child, size_t rows_);

    String getName() const override { return "ConcatSkippable"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

    bool getSkippedRows(size_t & skip_rows) override;

    size_t skipNextBlock() override;

    Block readWithFilter(const IColumn::Filter & filter) override;

    Block read() override;

private:
    friend class ConcatVectorIndexBlockInputStream;
    ColumnPtr createSegmentRowIdCol(UInt64 start, UInt64 limit);

    void addReadBytes(UInt64 bytes);

    BlockInputStreams::iterator current_stream;
    std::vector<size_t> rows;
    size_t precede_stream_rows;
    const ScanContextPtr scan_context;
    LACBytesCollector lac_bytes_collector;
};

class ConcatVectorIndexBlockInputStream : public SkippableBlockInputStream
{
public:
    // Only return the rows that match `bitmap_filter_`
    ConcatVectorIndexBlockInputStream(
        const BitmapFilterPtr & bitmap_filter_,
        std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream,
        std::vector<VectorIndexBlockInputStream *> && index_streams,
        UInt32 topk_)
        : stream(std::move(stream))
        , index_streams(std::move(index_streams))
        , topk(topk_)
        , bitmap_filter(bitmap_filter_)
    {}

    static std::tuple<SkippableBlockInputStreamPtr, bool> build(
        const BitmapFilterPtr & bitmap_filter,
        std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream,
        const ANNQueryInfoPtr & ann_query_info);

    String getName() const override { return "ConcatVectorIndex"; }

    Block getHeader() const override { return stream->getHeader(); }

    bool getSkippedRows(size_t &) override { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

    size_t skipNextBlock() override { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

    Block readWithFilter(const IColumn::Filter &) override
    {
        throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    Block read() override;

private:
    void load();

    std::shared_ptr<ConcatSkippableBlockInputStream<false>> stream;
    // Pointers to stream's children, nullptr if the child is not a VectorIndexBlockInputStream.
    std::vector<VectorIndexBlockInputStream *> index_streams;
    UInt32 topk = 0;
    bool loaded = false;

    BitmapFilterPtr bitmap_filter;
    IColumn::Filter filter; // reuse the memory allocated among all `read`
};

} // namespace DB::DM
