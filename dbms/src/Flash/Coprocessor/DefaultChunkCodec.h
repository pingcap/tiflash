#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>

namespace DB
{

class DefaultChunkCodec : public ChunkCodec
{
public:
    DefaultChunkCodec() = default;

    Block decode(const tipb::Chunk & chunk, const DAGSchema & schema) override;
    std::unique_ptr<ChunkCodecStream> newCodecStream(const std::vector<tipb::FieldType> & field_types) override;
};

} // namespace DB
