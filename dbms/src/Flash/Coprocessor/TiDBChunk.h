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

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/TiDBColumn.h>

namespace DB
{
// `TiDBChunk` stores multiple rows of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
// Values are appended in compact format and can be directly accessed without decoding.
// When the chunk is done processing, we can reuse the allocated memory by resetting it.
class TiDBChunk
{
public:
    TiDBChunk(const std::vector<tipb::FieldType> & field_types);

    void encodeChunk(WriteBuffer & ss)
    {
        for (auto & c : columns)
            c.encodeColumn(ss);
    }

    void buildDAGChunkFromBlock(
        const Block & block,
        const std::vector<tipb::FieldType> & field_types,
        size_t start_index,
        size_t end_index);

    TiDBColumn & getColumn(int index) { return columns[index]; }
    void clear()
    {
        for (auto & c : columns)
            c.clear();
    }

private:
    std::vector<TiDBColumn> columns;
};

} // namespace DB
