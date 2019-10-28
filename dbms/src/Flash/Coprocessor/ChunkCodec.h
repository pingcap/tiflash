#pragma once

#include <Core/Block.h>
#include <Storages/Transaction/TypeMapping.h>
#include <tipb/select.pb.h>

namespace DB
{

using DAGColumnInfo = std::pair<String, ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;

class ChunkCodecStream
{
public:
    explicit ChunkCodecStream(const std::vector<tipb::FieldType> & field_types_) : field_types(field_types_) {}
    virtual String getString() = 0;
    virtual void clear() = 0;
    virtual void encode(const Block & block, size_t start, size_t end) = 0;
    virtual ~ChunkCodecStream() = default;

protected:
    const std::vector<tipb::FieldType> & field_types;
};

class ChunkCodec
{
public:
    ChunkCodec() = default;
    virtual Block decode(const tipb::Chunk & chunk, const DAGSchema & schema) = 0;

    virtual std::unique_ptr<ChunkCodecStream> newCodecStream(const std::vector<tipb::FieldType> & result_field_types) = 0;

    virtual ~ChunkCodec() = default;
};

} // namespace DB
