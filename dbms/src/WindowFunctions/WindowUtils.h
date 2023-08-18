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

#include <Columns/IColumn.h>
#include <Core/ColumnNumbers.h>

namespace DB
{
struct WindowBlock
{
    Columns input_columns;
    MutableColumns output_columns;

    size_t rows = 0;
};

struct RowNumber
{
    UInt64 block = 0;
    UInt64 row = 0;

    bool operator<(const RowNumber & other) const
    {
        return block < other.block || (block == other.block && row < other.row);
    }

    bool operator==(const RowNumber & other) const { return block == other.block && row == other.row; }

    bool operator<=(const RowNumber & other) const { return *this < other || *this == other; }

    String toString() const { return fmt::format("[block={},row={}]", block, row); }
};
} // namespace DB
