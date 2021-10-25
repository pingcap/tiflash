#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/TiDBChunk.h>

namespace DB
{
class ArrowChunkCodec : public ChunkCodec
{
public:
    ArrowChunkCodec() = default;
    Block decode(const String &, const DAGSchema &) override;
    std::unique_ptr<ChunkCodecStream> newCodecStream(const std::vector<tipb::FieldType> & field_types) override;
};

} // namespace DB
