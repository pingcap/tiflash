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

namespace DB
{
namespace DM
{
class SkippableBlockInputStream : public IBlockInputStream
{
public:
    virtual ~SkippableBlockInputStream() = default;

    /// Return false if it is the end of stream.
    virtual bool getSkippedRows(size_t & skip_rows) = 0;
};

using SkippableBlockInputStreamPtr = std::shared_ptr<SkippableBlockInputStream>;
using SkippableBlockInputStreams = std::vector<SkippableBlockInputStreamPtr>;

class EmptySkippableBlockInputStream : public SkippableBlockInputStream
{
public:
    EmptySkippableBlockInputStream(const ColumnDefines & read_columns_)
        : read_columns(read_columns_)
    {}

    String getName() const override { return "EmptySkippable"; }

    Block getHeader() const override { return toEmptyBlock(read_columns); }

    bool getSkippedRows(size_t &) override { return false; }

    Block read() override { return {}; }

private:
    ColumnDefines read_columns;
};

class ConcatSkippableBlockInputStream : public SkippableBlockInputStream
{
public:
    ConcatSkippableBlockInputStream(SkippableBlockInputStreams inputs_)
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
            auto skippable_stream = dynamic_cast<SkippableBlockInputStream *>((*current_stream).get());

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
                break;
            else
            {
                (*current_stream)->readSuffix();
                ++current_stream;
            }
        }

        return res;
    }

private:
    BlockInputStreams::iterator current_stream;
};

} // namespace DM
} // namespace DB