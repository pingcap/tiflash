#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>

#include <Flash/Coprocessor/TiDBChunk.h>

namespace DB
{

class ArrowChunkCodec : public ChunkCodec
{
public:
    ArrowChunkCodec() = default;
    void encode(const DB::Block & block, size_t start, size_t end, std::unique_ptr<DB::ChunkCodecStream> & stream) override;
    Block decode(const tipb::Chunk & chunk, const DAGSchema & schema) override;
    std::unique_ptr<ChunkCodecStream> newCodecStream(const std::vector<tipb::FieldType> & field_types) override;
};

} // namespace DB
