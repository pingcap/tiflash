#pragma once

#include <Core/Block.h>
#include <tipb/select.pb.h>

namespace DB
{

class DAGChunkBuilder
{
public:
    DAGChunkBuilder(
        tipb::SelectResponse & dag_response_, const Int64 records_per_chunk_, const std::vector<tipb::FieldType> & result_field_types_)
        : dag_response(dag_response_), records_per_chunk(records_per_chunk_), result_field_types(result_field_types_){};

    virtual void build(const Block & block) = 0;

    virtual void buildSuffix() = 0;

    virtual ~DAGChunkBuilder(){};

protected:
    tipb::SelectResponse & dag_response;
    const Int64 records_per_chunk;
    const std::vector<tipb::FieldType> & result_field_types;
};

} // namespace DB
