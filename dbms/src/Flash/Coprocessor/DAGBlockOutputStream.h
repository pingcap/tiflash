#pragma once

#include <Core/Types.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/TiDBChunk.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

/// Serializes the stream of blocks in TiDB DAG response format.
/// TODO: May consider using some parallelism.
/// TODO: Consider using output schema in DAG request, do some conversion or checking between DAG schema and block schema.
class DAGBlockOutputStream : public IBlockOutputStream
{
public:
    DAGBlockOutputStream(tipb::SelectResponse & response_, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
        std::vector<tipb::FieldType> && result_field_types, Block header_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void writePrefix() override;
    void writeSuffix() override;
    void encodeWithDefaultEncodeType(const Block & block);
    void encodeWithArrowEncodeType(const Block & block);

private:
    tipb::SelectResponse & dag_response;

    const Int64 records_per_chunk;
    tipb::EncodeType encodeType;
    std::vector<tipb::FieldType> result_field_types;

    Block header;

    tipb::Chunk * chunk_for_default_encode;
    std::unique_ptr<TiDBChunk> chunk_for_arrow_encode;
    Int64 current_records_num;
    std::stringstream current_ss;
};

} // namespace DB
