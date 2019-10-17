#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/TiDBColumn.h>
#include <Storages/Transaction/TiDB.h>

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

    void encodeChunk(std::stringstream & ss)
    {
        for (auto & c : columns)
            c.encodeColumn(ss);
    }

    void buildDAGChunkFromBlock(
        const Block & block, const std::vector<tipb::FieldType> & field_types, size_t start_index, size_t end_index);

    TiDBColumn & getColumn(int index) { return columns[index]; };
    void clear()
    {
        for (auto & c : columns)
            c.clear();
    }

private:
    std::vector<TiDBColumn> columns;
};

} // namespace DB
