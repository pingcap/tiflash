#pragma once

#include <Flash/Coprocessor/ChunkCodec.h>

namespace DB
{
class CHBlockChunkCodec : public ChunkCodec
{
public:
    CHBlockChunkCodec() = default;
    explicit CHBlockChunkCodec(const Block & header_);
    // schema will be ignored if header is set
    Block decode(const String &, const DAGSchema & schema) override;
    std::unique_ptr<ChunkCodecStream> newCodecStream(const std::vector<tipb::FieldType> & field_types) override;

private:
    Block header;
};

} // namespace DB
