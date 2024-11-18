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

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>


namespace DB::DM
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
    ColumnDefines read_columns;
};

} // namespace DB::DM
