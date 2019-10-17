#include <Flash/Coprocessor/DAGBlockOutputStream.h>

#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

DAGBlockOutputStream::DAGBlockOutputStream(tipb::SelectResponse & dag_response_, Int64 records_per_chunk_, tipb::EncodeType encodeType,
    std::vector<tipb::FieldType> && result_field_types_, Block header_)
    : dag_response(dag_response_),
      result_field_types(result_field_types_),
      header(std::move(header_)),
      records_per_chunk(records_per_chunk_),
      current_records_num(0)
{
    if (encodeType == tipb::EncodeType::TypeDefault)
    {
        chunk_codec_stream = std::make_unique<DefaultChunkCodec>()->newCodecStream(result_field_types);
    }
    else if (encodeType == tipb::EncodeType::TypeArrow)
    {
        chunk_codec_stream = std::make_unique<ArrowChunkCodec>()->newCodecStream(result_field_types);
    }
    else
    {
        throw Exception("Only Default and Arrow encode type is supported in DAGBlockOutputStream.", ErrorCodes::UNSUPPORTED_PARAMETER);
    }
    dag_response.set_encode_type(encodeType);
}


void DAGBlockOutputStream::writePrefix()
{
    //something to do here?
}

void DAGBlockOutputStream::encodeChunkToDAGResponse()
{
    auto dag_chunk = dag_response.add_chunks();
    dag_chunk->set_rows_data(chunk_codec_stream->getString());
    chunk_codec_stream->clear();
    dag_response.add_output_counts(current_records_num);
    current_records_num = 0;
}

void DAGBlockOutputStream::writeSuffix()
{
    // todo error handle
    if (current_records_num > 0)
    {
        encodeChunkToDAGResponse();
    }
}

void DAGBlockOutputStream::write(const Block & block)
{
    if (block.columns() != result_field_types.size())
        throw Exception("Output column size mismatch with field type size", ErrorCodes::LOGICAL_ERROR);
    size_t rows = block.rows();
    for (size_t row_index = 0; row_index < rows;)
    {
        if (current_records_num >= records_per_chunk)
        {
            encodeChunkToDAGResponse();
        }
        const size_t upper = std::min(row_index + (records_per_chunk - current_records_num), rows);
        chunk_codec_stream->encode(block, row_index, upper);
        current_records_num += (upper - row_index);
        row_index = upper;
    }
}

} // namespace DB
