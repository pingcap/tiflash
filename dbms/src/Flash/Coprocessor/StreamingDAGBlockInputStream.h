#pragma once

#include <Core/Types.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/IDataType.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop

namespace DB
{

/// Serializes the stream of blocks in TiDB DAG response format.
/// TODO: May consider using some parallelism.
/// TODO: Consider using output schema in DAG request, do some conversion or checking between DAG schema and block schema.
class StreamingDAGBlockInputStream : public IProfilingBlockInputStream
{
public:
    StreamingDAGBlockInputStream(BlockInputStreamPtr input_, StreamWriterPtr writer_, Int64 records_per_chunk_,
        tipb::EncodeType encodeType_, std::vector<tipb::FieldType> && result_field_types, Block && header_, DAGContext & dag_context_,
        bool collect_execute_summary_, bool return_executor_id);

    Block getHeader() const override { return header; }
    Block readImpl() override;
    String getName() const override { return "StreamingWriter"; }
    void readPrefix() override;
    void readSuffix() override;

private:
    bool finished;
    Block header;
    DAGResponseWriter<true> response_writer;
};

} // namespace DB
