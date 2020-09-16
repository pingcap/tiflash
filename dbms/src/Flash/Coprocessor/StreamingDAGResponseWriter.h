#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <common/ThreadPool.h>
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop

namespace DB
{

/// Serializes the stream of blocks in TiDB DAG response format.
class StreamingDAGResponseWriter : public DAGResponseWriter
{
public:
    StreamingDAGResponseWriter(StreamWriterPtr writer_, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
        std::vector<tipb::FieldType> result_field_types, DAGContext & dag_context_, bool collect_execute_summary_,
        bool return_executor_id_);

    void write(const Block & block) override;
    void finishWrite() override;

private:
    void ScheduleEncodeTask();
    ThreadPool::Job getEncodeTask(std::vector<Block> & input_blocks, tipb::SelectResponse & response, StreamWriterPtr stream_writer) const;

    StreamWriterPtr writer;
    std::vector<Block> blocks;
    size_t rows_in_blocks;
    ThreadPool thread_pool;
};

} // namespace DB
