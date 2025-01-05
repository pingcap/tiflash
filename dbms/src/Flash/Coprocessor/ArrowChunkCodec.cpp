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

#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/ArrowColCodec.h>
#include <IO/copyData.h>
#include <TiDB/Decode/TypeMapping.h>

namespace DB
{
class ArrowChunkCodecStream : public ChunkCodecStream
{
public:
    explicit ArrowChunkCodecStream(const std::vector<tipb::FieldType> & field_types)
        : ChunkCodecStream(field_types)
    {
        ti_chunk = std::make_unique<TiDBChunk>(field_types);
    }

    String getString() override
    {
        WriteBufferFromOwnString ss;
        ti_chunk->encodeChunk(ss);
        return ss.releaseStr();
    }
    void clear() override { ti_chunk->clear(); }
    void encode(const Block & block, size_t start, size_t end) override;
    std::unique_ptr<TiDBChunk> ti_chunk;
};

void ArrowChunkCodecStream::encode(const Block & block, size_t start, size_t end)
{
    // Encode data in chunk by arrow encode
    ti_chunk->buildDAGChunkFromBlock(block, field_types, start, end);
}

Block ArrowChunkCodec::decode(const String & row_data, const DAGSchema & schema)
{
    const char * start = row_data.c_str();
    const char * pos = start;
    int column_index = 0;
    ColumnsWithTypeAndName columns;
    while (pos < start + row_data.size())
    {
        UInt32 length = toLittleEndian(*(reinterpret_cast<const UInt32 *>(pos)));
        pos += 4;
        UInt32 null_count = toLittleEndian(*(reinterpret_cast<const UInt32 *>(pos)));
        pos += 4;
        std::vector<UInt8> null_bitmap;
        const auto & field = schema[column_index];
        const auto & name = field.first;
        auto data_type = getDataTypeByColumnInfoForComputingLayer(field.second);
        if (null_count > 0)
        {
            auto bit_map_length = (length + 7) / 8;
            for (UInt32 i = 0; i < bit_map_length; i++)
            {
                null_bitmap.push_back(*pos);
                pos++;
            }
        }
        Int8 field_length = getFieldLengthForArrowEncode(field.second.tp);
        std::vector<UInt64> offsets;
        if (field_length == VAR_SIZE)
        {
            for (UInt32 i = 0; i <= length; i++)
            {
                offsets.push_back(toLittleEndian(*(reinterpret_cast<const UInt64 *>(pos))));
                pos += 8;
            }
        }
        ColumnWithTypeAndName col(data_type, name);
        col.column->assumeMutable()->reserve(length);
        pos = arrowColToFlashCol(pos, field_length, null_count, null_bitmap, offsets, col, field.second, length);
        columns.emplace_back(std::move(col));
        column_index++;
    }
    return Block(columns);
}

std::unique_ptr<ChunkCodecStream> ArrowChunkCodec::newCodecStream(
    const std::vector<tipb::FieldType> & field_types,
    MppVersion)
{
    return std::make_unique<ArrowChunkCodecStream>(field_types);
}

} // namespace DB
