// Copyright 2022 PingCAP, Ltd.
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

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <Storages/DeltaMerge/ReadUtil.h>

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
    /// Return false if failed to skip or the end of stream.
    virtual bool skipNextBlock() = 0;
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

    bool skipNextBlock() override { return false; }

    Block read() override { return {}; }

private:
    ColumnDefines read_columns{};
};

class ConcatSkippableBlockInputStream : public SkippableBlockInputStream
{
public:
    explicit ConcatSkippableBlockInputStream(SkippableBlockInputStreams inputs_)
        : rows(inputs_.size(), 0)
        , precede_stream_rows(0)
    {
        children.insert(children.end(), inputs_.begin(), inputs_.end());
        current_stream = children.begin();
    }

    ConcatSkippableBlockInputStream(SkippableBlockInputStreams inputs_, std::vector<size_t> && rows_)
        : rows(std::move(rows_))
        , precede_stream_rows(0)
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

    bool skipNextBlock() override
    {
        while (current_stream != children.end())
        {
            auto * skippable_stream = dynamic_cast<SkippableBlockInputStream *>((*current_stream).get());

            bool skipped = skippable_stream->skipNextBlock();

            if (skipped)
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

    Block read() override
    {
        Block res;

        while (current_stream != children.end())
        {
            res = (*current_stream)->read();

            if (res)
            {
                res.setStartOffset(res.startOffset() + precede_stream_rows);
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
    BlockInputStreams::iterator current_stream;
    std::vector<size_t> rows;
    size_t precede_stream_rows;
};

class ColumnOrderedSkippableBlockInputStream : public SkippableBlockInputStream
{
    static constexpr auto NAME = "ColumnOrderedSkippableBlockInputStream";

public:
    explicit ColumnOrderedSkippableBlockInputStream(
        const ColumnDefines & columns_to_read_,
        SkippableBlockInputStreamPtr stable_,
        BlockInputStreamPtr delta_,
        size_t stable_rows_,
        const String & req_id_)
        : header(toEmptyBlock(columns_to_read_))
        , stable(stable_)
        , delta(delta_)
        , stable_rows(stable_rows_)
        , log(Logger::get(NAME, req_id_))
    {}

    String getName() const override { return NAME; }

    Block getHeader() const override { return header; }

    bool getSkippedRows(size_t & skip_rows) override
    {
        if (cur_read_rows > stable_rows)
        {
            return false;
        }
        return stable->getSkippedRows(skip_rows);
    }

    bool skipNextBlock() override
    {
        // TODO: support skip rows in delta
        if (cur_read_rows > stable_rows)
        {
            return false;
        }
        return stable->skipNextBlock();
    }

    Block read() override
    {
        auto inner_stable = static_cast<BlockInputStreamPtr>(stable);
        auto [block, from_delta] = readBlock(inner_stable, delta);
        if (block)
        {
            if (from_delta)
            {
                block.setStartOffset(block.startOffset() + stable_rows);
            }
            cur_read_rows += block.rows();
        }
        return block;
    }

private:
    Block header;
    SkippableBlockInputStreamPtr stable;
    BlockInputStreamPtr delta;
    size_t stable_rows;
    size_t cur_read_rows = 0;
    const LoggerPtr log;
    IColumn::Filter filter{};
};

} // namespace DM
} // namespace DB