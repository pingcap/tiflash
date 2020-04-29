#pragma once

#include <Core/Types.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop

namespace DB
{

/// Serializes the stream of blocks in TiDB DAG response format.
class DAGBlockOutputStream : public IBlockOutputStream
{
public:
    DAGBlockOutputStream(tipb::SelectResponse * response_, Int64 records_per_chunk_, tipb::EncodeType encodeType_,
        std::vector<tipb::FieldType> result_field_types, Block && header_, DAGContext & dag_context_,
        bool collect_execute_summary_, bool return_executor_id);

    Block getHeader() const { return header; }
    void write(const Block & block);
    void writePrefix();
    void writeSuffix();

private:
    Block header;
    DAGResponseWriter<false> response_writer;
};

} // namespace DB
