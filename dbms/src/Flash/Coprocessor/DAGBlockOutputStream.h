#pragma once

#include <Core/Types.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

/// Serializes the stream of blocks in TiDB DAG response format.
/// TODO: May consider using some parallelism.
/// TODO: Consider using output schema in DAG request, do some conversion or checking between DAG schema and block schema.
template<bool streaming>
class DAGBlockOutputStream : public IBlockOutputStream
{
public:
    DAGBlockOutputStream(tipb::SelectResponse * response_, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
        std::vector<tipb::FieldType> result_field_types, Block && header_);

    DAGBlockOutputStream(::grpc::ServerWriter<::coprocessor::BatchResponse> * writer, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
                         std::vector<tipb::FieldType> result_field_types, Block && header_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void writePrefix() override;
    void writeSuffix() override;
    void encodeChunkToDAGResponse();

private:
    tipb::SelectResponse * dag_response;
    ::grpc::ServerWriter<::coprocessor::BatchResponse> * writer;
    std::vector<tipb::FieldType> result_field_types;
    Block header;
    Int64 records_per_chunk;
    tipb::EncodeType encode_type;
    std::unique_ptr<ChunkCodecStream> chunk_codec_stream;
    Int64 current_records_num;
};

} // namespace DB
