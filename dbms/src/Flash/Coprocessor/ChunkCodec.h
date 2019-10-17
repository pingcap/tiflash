#pragma once

#include <Core/Block.h>
#include <Flash/Coprocessor/ChunkCodecStream.h>
#include <Storages/Transaction/TypeMapping.h>
#include <tipb/select.pb.h>

namespace DB
{

using DAGColumnInfo = std::pair<String, ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;

class ChunkCodec
{
public:
    ChunkCodec() = default;
    virtual void encode(const Block & block, size_t start, size_t end, const std::vector<tipb::FieldType> & result_field_types,
        std::unique_ptr<ChunkCodecStream> & stream)
        = 0;
    virtual Block decode(const tipb::Chunk & chunk, const DAGSchema & schema) = 0;

    virtual ~ChunkCodec() = default;
};

} // namespace DB
