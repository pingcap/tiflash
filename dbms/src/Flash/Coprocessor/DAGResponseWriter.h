#pragma once

#include <Core/Types.h>
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
template <bool streaming>
class DAGResponseWriter
{
public:
    DAGResponseWriter(tipb::SelectResponse * response_, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
        std::vector<tipb::FieldType> result_field_types, DAGContext & dag_context_, bool collect_execute_summary_);

    DAGResponseWriter(StreamWriterPtr writer, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
        std::vector<tipb::FieldType> result_field_types, DAGContext & dag_context_, bool collect_execute_summary_);

    void write(const Block & block);
    void finishWrite();
    void encodeChunkToDAGResponse();
    void addExecuteSummaries(tipb::SelectResponse * dag_response);

private:
    void init();

    tipb::SelectResponse * dag_response;
    StreamWriterPtr writer;
    std::vector<tipb::FieldType> result_field_types;
    Int64 records_per_chunk;
    tipb::EncodeType encode_type;
    std::unique_ptr<ChunkCodecStream> chunk_codec_stream;
    Int64 current_records_num;
    DAGContext & dag_context;
    bool collect_execute_summary;
    std::vector<std::tuple<UInt64, UInt64, UInt64>> previous_execute_stats;
};

} // namespace DB
