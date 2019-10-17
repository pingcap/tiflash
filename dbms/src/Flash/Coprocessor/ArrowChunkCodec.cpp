#include <Flash/Coprocessor/ArrowChunkCodec.h>

#include <Flash/Coprocessor/ArrowColCodec.h>
#include <IO/Endian.h>

namespace DB
{

class ArrowChunkCodecStream : public ChunkCodecStream
{
public:
    explicit ArrowChunkCodecStream(std::vector<tipb::FieldType> & field_types) : ChunkCodecStream(field_types)
    {
        ti_chunk = std::make_unique<TiDBChunk>(field_types);
    }

    String getString() override
    {
        std::stringstream ss;
        ti_chunk->encodeChunk(ss);
        return ss.str();
    }
    void clear() override { ti_chunk->clear(); }
    std::unique_ptr<TiDBChunk> ti_chunk;
};

void ArrowChunkCodec::encode(const DB::Block & block, size_t start, size_t end, std::unique_ptr<DB::ChunkCodecStream> & stream)
{
    // Encode data in chunk by arrow encode
    auto * arrow_chunk_codec_stream = dynamic_cast<DB::ArrowChunkCodecStream *>(stream.get());
    arrow_chunk_codec_stream->ti_chunk->buildDAGChunkFromBlock(block, stream->getFieldTypes(), start, end);
}

Block ArrowChunkCodec::decode(const tipb::Chunk & chunk, const DAGSchema & schema)
{
    const String & row_data = chunk.rows_data();
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
        auto data_type = getDataTypeByColumnInfo(field.second);
        if (null_count > 0)
        {
            auto bit_map_length = (length + 7) / 8;
            for (UInt32 i = 0; i < bit_map_length; i++)
            {
                null_bitmap.push_back(*pos);
                pos++;
            }
        }
        Int8 field_length = getFieldLength(field.second.tp);
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

std::unique_ptr<ChunkCodecStream> ArrowChunkCodec::newCodecStream(const std::vector<tipb::FieldType> & field_types)
{
    return std::make_unique<ArrowChunkCodecStream>(field_types);
}

} // namespace DB
