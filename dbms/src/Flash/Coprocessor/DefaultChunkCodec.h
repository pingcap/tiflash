#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>

namespace DB
{
class DefaultChunkCodec : public ChunkCodec
{
public:
    DefaultChunkCodec() = default;

    Block decode(const String &, const DAGSchema &) override;
    std::unique_ptr<ChunkCodecStream> newCodecStream(const std::vector<tipb::FieldType> & field_types) override;
};

} // namespace DB
