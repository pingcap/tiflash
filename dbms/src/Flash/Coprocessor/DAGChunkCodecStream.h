#pragma once

#include <Flash/Coprocessor/TiDBChunk.h>
#include <tipb/select.pb.h>

namespace DB
{

class DAGChunkCodecStream
{
public:
    virtual String getString() = 0;
    virtual void clear() = 0;
    virtual ~DAGChunkCodecStream() = default;
};

class DAGDefaultChunkCodecStream : public DAGChunkCodecStream
{
public:
    std::stringstream ss;
    String getString() override { return ss.str(); }
    void clear() override { ss.str(""); }
};

class DAGArrowChunkCodecStream : public DAGChunkCodecStream
{
public:
    explicit DAGArrowChunkCodecStream(std::vector<tipb::FieldType> & field_types) : DAGChunkCodecStream()
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

} // namespace DB
