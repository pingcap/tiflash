#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>

namespace DB
{

class CHBlockChunkCodec : public ChunkCodec
{
public:
    CHBlockChunkCodec() = default;
    Block decode(const tipb::Chunk & , const DAGSchema & ) override { throw Exception("Not supported"); };
    std::unique_ptr<ChunkCodecStream> newCodecStream(const std::vector<tipb::FieldType> & field_types) override;
};

} // namespace DB
