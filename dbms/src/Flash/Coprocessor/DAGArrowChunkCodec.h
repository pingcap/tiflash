#pragma once

#include <Flash/Coprocessor/DAGChunkCodec.h>

#include <Flash/Coprocessor/TiDBChunk.h>

namespace DB
{

class DAGArrowChunkCodec : public DAGChunkCodec
{
public:
    explicit DAGArrowChunkCodec(const std::vector<tipb::FieldType> & result_field_types) : DAGChunkCodec(result_field_types) {}
    void encode(const DB::Block & block, size_t start, size_t end, std::unique_ptr<DB::DAGChunkCodecStream> & stream) override;
    Block decode(const tipb::Chunk & chunk) override;
};

} // namespace DB
