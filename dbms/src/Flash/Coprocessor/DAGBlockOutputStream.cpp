#include <Flash/Coprocessor/DAGBlockOutputStream.h>

namespace DB
{

DAGBlockOutputStream::DAGBlockOutputStream(tipb::SelectResponse * dag_response, Int64 records_per_chunk, tipb::EncodeType encode_type,
    std::vector<tipb::FieldType> result_field_types, Block && header_, DAGContext & dag_context, bool collect_execute_summary, bool return_executor_id)
    : header(std::move(header_)),
      response_writer(dag_response, nullptr, records_per_chunk, encode_type, result_field_types, dag_context, collect_execute_summary, return_executor_id)
{}

void DAGBlockOutputStream::writePrefix()
{
    //something to do here?
}

void DAGBlockOutputStream::write(const Block & block) { response_writer.write(block); }

void DAGBlockOutputStream::writeSuffix()
{
    // todo error handle
    response_writer.finishWrite();
}

} // namespace DB
