#pragma once

#include <Flash/Coprocessor/DAGChunkBuilder.h>

#include <Flash/Coprocessor/TiDBChunk.h>

namespace DB
{

class DAGArrowChunkBuilder : public DAGChunkBuilder
{
public:
    DAGArrowChunkBuilder(
        tipb::SelectResponse & dag_response, const Int64 records_per_chunk, const std::vector<tipb::FieldType> & result_field_types)
        : DAGChunkBuilder(dag_response, records_per_chunk, result_field_types)
    {
        dag_response.set_encode_type(tipb::EncodeType::TypeArrow);
        ti_chunk = std::make_unique<TiDBChunk>(result_field_types);
    };

    void build(const Block & block) override;

    void buildSuffix() override;

private:
    std::unique_ptr<TiDBChunk> ti_chunk;
};

} // namespace DB
