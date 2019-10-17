#pragma once

#include <Core/Block.h>
#include <Flash/Coprocessor/DAGChunkCodecStream.h>
#include <tipb/select.pb.h>

namespace DB
{

class DAGChunkCodec
{
public:
    explicit DAGChunkCodec(const std::vector<tipb::FieldType> & result_field_types_) : result_field_types(result_field_types_){};

    virtual void encode(const Block & block, size_t start, size_t end, std::unique_ptr<DAGChunkCodecStream> & stream) = 0;
    virtual Block decode(const tipb::Chunk & chunk) = 0;

    virtual ~DAGChunkCodec(){};

protected:
    const std::vector<tipb::FieldType> & result_field_types;
};

} // namespace DB
