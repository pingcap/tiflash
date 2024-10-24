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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileSetReader.h>
#include <Storages/DeltaMerge/SkippableBlockInputStream.h>

namespace DB::DM
{

class ColumnFileSetInputStream : public SkippableBlockInputStream
{
protected:
    ColumnFileSetReader reader;

    std::vector<ColumnFileReaderPtr>::iterator cur_column_file_reader;
    size_t read_rows = 0;

public:
    ColumnFileSetInputStream(
        const DMContext & context_,
        const ColumnFileSetSnapshotPtr & delta_snap_,
        const ColumnDefinesPtr & col_defs_,
        const RowKeyRange & segment_range_,
        ReadTag read_tag_)
        : reader(context_, delta_snap_, col_defs_, segment_range_, read_tag_)
    {
        cur_column_file_reader = reader.column_file_readers.begin();
    }

    String getName() const override { return "ColumnFileSet"; }
    Block getHeader() const override { return toEmptyBlock(*(reader.col_defs)); }

    bool getSkippedRows(size_t &) override { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

    size_t skipNextBlock() override;

    Block read() override;

    Block readWithFilter(const IColumn::Filter & filter) override;
};

using ColumnFileSetInputStreamPtr = std::shared_ptr<ColumnFileSetInputStream>;

} // namespace DB::DM
