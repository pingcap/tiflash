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
    DAGBlockOutputStream(Block && header_, std::unique_ptr<DAGResponseWriter> response_writer_);

    Block getHeader() const { return header; }
    void write(const Block & block);
    void writePrefix();
    void writeSuffix();

private:
    Block header;
    std::unique_ptr<DAGResponseWriter> response_writer;
};

} // namespace DB
