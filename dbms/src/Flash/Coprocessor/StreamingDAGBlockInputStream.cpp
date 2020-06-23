#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/StreamingDAGBlockInputStream.h>

namespace DB
{

StreamingDAGBlockInputStream::StreamingDAGBlockInputStream(BlockInputStreamPtr input, StreamWriterPtr writer, Int64 records_per_chunk,
    tipb::EncodeType encode_type, std::vector<tipb::FieldType> && result_field_types, Block && header_, DAGContext & dag_context,
    bool collect_execute_summary, bool return_executor_id)
    : finished(false),
      header(std::move(header_)),
      response_writer(
          nullptr, writer, records_per_chunk, encode_type, result_field_types, dag_context, collect_execute_summary, return_executor_id)
{
    children.push_back(input);
}

void StreamingDAGBlockInputStream::readPrefix() { children.back()->readPrefix(); }

void StreamingDAGBlockInputStream::readSuffix()
{
    // todo error handle
    response_writer.finishWrite();
    children.back()->readSuffix();
}

Block StreamingDAGBlockInputStream::readImpl()
{
    if (finished)
        return {};
    while (Block block = children.back()->read())
    {
        if (!block)
        {
            finished = true;
            return {};
        }
        response_writer.write(block);
    }
    return {};
}

} // namespace DB
