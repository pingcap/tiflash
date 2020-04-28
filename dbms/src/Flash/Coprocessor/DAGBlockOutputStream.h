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
template <bool streaming>
class DAGBlockOutputStream : public std::conditional_t<streaming, IProfilingBlockInputStream, IBlockOutputStream>
{
public:
    DAGBlockOutputStream(tipb::SelectResponse * response_, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
        std::vector<tipb::FieldType> result_field_types, Block && header_, DAGContext & dag_context_, bool collect_execute_summary_);

    DAGBlockOutputStream(BlockInputStreamPtr input_, StreamWriterPtr writer, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
        std::vector<tipb::FieldType> result_field_types, Block && header_, DAGContext & dag_context_, bool collect_execute_summary_);


#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winconsistent-missing-override"
    Block getHeader() const { return header; }
    String getName() const { return "StreamingWriter"; }
    void write(const Block & block);
    void writePrefix();
    void writeSuffix();
    Block readImpl();
    void readPrefix();
    void readSuffix();
#pragma clang diagnostic pop

    void encodeChunkToDAGResponse();
    void addExecuteSummaries(tipb::SelectResponse * dag_response);

private:
    bool finished;
    tipb::SelectResponse * dag_response;
    StreamWriterPtr writer;
    std::vector<tipb::FieldType> result_field_types;
    Block header;
    Int64 records_per_chunk;
    tipb::EncodeType encode_type;
    std::unique_ptr<ChunkCodecStream> chunk_codec_stream;
    Int64 current_records_num;
    DAGContext & dag_context;
    bool collect_execute_summary;
    std::vector<std::tuple<UInt64, UInt64, UInt64>> previous_execute_stats;
};

} // namespace DB
