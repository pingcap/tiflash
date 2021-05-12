#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop

namespace DB
{

/// Serializes the stream of blocks in TiDB DAG response format.
class UnaryDAGResponseWriter : public DAGResponseWriter
{
public:
    UnaryDAGResponseWriter(tipb::SelectResponse * response_, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
        std::vector<tipb::FieldType> result_field_types, DAGContext & dag_context_);

    void write(const Block & block) override;
    void finishWrite() override;
    void encodeChunkToDAGResponse();
    void appendWarningsToDAGResponse();

private:
    tipb::SelectResponse * dag_response;
    std::unique_ptr<ChunkCodecStream> chunk_codec_stream;
    Int64 current_records_num;
    std::unordered_map<String, std::tuple<UInt64, UInt64, UInt64>> previous_execute_stats;
};

} // namespace DB
