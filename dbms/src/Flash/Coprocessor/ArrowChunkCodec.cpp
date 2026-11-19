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

#include <Common/Exception.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/ArrowColCodec.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <TiDB/Decode/TypeMapping.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}

namespace
{
template <typename T>
T readArrowValue(ReadBuffer & input)
{
    T value;
    readIntBinary(value, input);
    return toLittleEndian(value);
}
} // namespace

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
    if (row_data.empty())
        return {};

    ReadBufferFromString input(row_data);
    ColumnsWithTypeAndName columns;
    UInt32 expected_rows = 0;
    while (!input.eof())
    {
        const size_t column_index = columns.size();
        if (unlikely(column_index >= schema.size()))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow chunk has more columns than its schema");

        const auto length = readArrowValue<UInt32>(input);
        const auto null_count = readArrowValue<UInt32>(input);
        if (unlikely(null_count > length))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow chunk null count exceeds its row count");
        if (columns.empty())
            expected_rows = length;
        else if (unlikely(length != expected_rows))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow chunk columns have different row counts");

        std::vector<UInt8> null_bitmap;
        const auto & field = schema[column_index];
        const auto & name = field.first;
        if (null_count > 0)
        {
            const size_t bitmap_size = (static_cast<UInt64>(length) + 7) / 8;
            if (unlikely(bitmap_size > input.available()))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow chunk null bitmap is truncated");
            null_bitmap.resize(bitmap_size);
            input.readStrict(reinterpret_cast<char *>(null_bitmap.data()), bitmap_size);
        }
        const auto field_length = getFieldLengthForArrowEncode(field.second.tp);
        std::vector<UInt64> offsets;
        size_t data_size = 0;
        if (field_length == static_cast<UInt8>(VAR_SIZE))
        {
            const UInt64 offset_count = static_cast<UInt64>(length) + 1;
            if (unlikely(offset_count > input.available() / sizeof(UInt64)))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow chunk offsets are truncated");
            offsets.reserve(offset_count);
            for (UInt64 i = 0; i < offset_count; ++i)
            {
                const auto offset = readArrowValue<UInt64>(input);
                if (unlikely((i == 0 && offset != 0) || (i > 0 && offset < offsets.back())))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow chunk offsets are invalid");
                offsets.push_back(offset);
            }
            if (unlikely(offsets.back() > input.available()))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow chunk column data is truncated");
            data_size = offsets.back();
        }
        else
        {
            if (unlikely(length > input.available() / field_length))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow chunk column data is truncated");
            data_size = static_cast<size_t>(length) * field_length;
        }

        auto data_type = getDataTypeByColumnInfoForComputingLayer(field.second);
        ColumnWithTypeAndName col(data_type, name);
        col.column->assumeMutable()->reserve(length);
        arrowColToFlashCol(input.position(), field_length, null_count, null_bitmap, offsets, col, field.second, length);
        if (unlikely(col.column->size() != length))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow chunk decoded column has an unexpected row count");
        input.position() += data_size;
        columns.emplace_back(std::move(col));
    }
    if (unlikely(columns.size() != schema.size()))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Arrow chunk has fewer columns than its schema");
    return Block(columns);
}

std::unique_ptr<ChunkCodecStream> ArrowChunkCodec::newCodecStream(const std::vector<tipb::FieldType> & field_types)
{
    return std::make_unique<ArrowChunkCodecStream>(field_types);
}

} // namespace DB
