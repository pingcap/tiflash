#pragma once

#include <Flash/Coprocessor/DAGChunkBuilder.h>

namespace DB
{

class DAGDefaultChunkBuilder : public DAGChunkBuilder
{
public:
    DAGDefaultChunkBuilder(
        tipb::SelectResponse & dag_response, const Int64 records_per_chunk, const std::vector<tipb::FieldType> & result_field_types)
        : DAGChunkBuilder(dag_response, records_per_chunk, result_field_types)
    {
        current_chunk = nullptr;
        current_records_num = 0;
        dag_response.set_encode_type(tipb::EncodeType::TypeDefault);
    };

    void build(const Block & block) override;

    void buildSuffix() override;

private:
    tipb::Chunk * current_chunk;
    Int64 current_records_num;
    std::stringstream current_ss;
};

} // namespace DB
