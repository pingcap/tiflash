#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>

namespace DB
{
class CHBlockChunkCodec : public ChunkCodec
{
public:
    CHBlockChunkCodec() = default;
    Block decode(const String &, const DAGSchema & schema) override;
    static Block decode(const String &, const Block & header);

    static Block decodeWithCompression(const String &, const Block & header, bool enable_compression);

    std::unique_ptr<ChunkCodecStream> newCodecStream(const std::vector<tipb::FieldType> & field_types) override;
};

} // namespace DB
