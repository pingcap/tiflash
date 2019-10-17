#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>

namespace DB
{

class DefaultChunkCodec : public ChunkCodec
{
public:
    DefaultChunkCodec() = default;

    void encode(const DB::Block & block, size_t start, size_t end, const std::vector<tipb::FieldType> & result_field_types,
        std::unique_ptr<DB::ChunkCodecStream> & stream) override;
    Block decode(const tipb::Chunk & chunk, const DAGSchema & schema) override;
};

} // namespace DB
