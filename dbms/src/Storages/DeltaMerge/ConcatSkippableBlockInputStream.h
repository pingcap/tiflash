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
#include <Storages/DeltaMerge/ConcatSkippableBlockInputStream_fwd.h>
#include <Storages/DeltaMerge/ScanContext_fwd.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>


namespace DB::DM
{

template <bool need_row_id = false>
class ConcatSkippableBlockInputStream : public SkippableBlockInputStream
{
public:
    static auto create(
        SkippableBlockInputStreams && inputs_,
        std::vector<size_t> && rows_,
        const ScanContextPtr & scan_context_)
    {
        return std::make_shared<ConcatSkippableBlockInputStream<need_row_id>>(
            std::move(inputs_),
            std::move(rows_),
            scan_context_);
    }

    ConcatSkippableBlockInputStream(
        SkippableBlockInputStreams && inputs_,
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
    friend class VectorIndexInputStream;
    friend class FullTextIndexInputStream;

    void addReadBytes(UInt64 bytes);

    BlockInputStreams::iterator current_stream;
    std::vector<size_t> rows;
    size_t precede_stream_rows;
    const ScanContextPtr scan_context;
    LACBytesCollector lac_bytes_collector;
};

} // namespace DB::DM
