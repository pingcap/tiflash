#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/StreamingDAGBlockOutputStream.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

StreamingDAGBlockInputStream::StreamingDAGBlockInputStream(BlockInputStreamPtr input_, StreamWriterPtr writer_, Int64 records_per_chunk_,
    tipb::EncodeType encode_type_, std::vector<tipb::FieldType> && result_field_types_, Block && header_)
    : finished(false),
      writer(writer_),
      result_field_types(std::move(result_field_types_)),
      header(std::move(header_)),
      records_per_chunk(records_per_chunk_),
      current_records_num(0),
      encode_type(encode_type_)
{
    if (encode_type == tipb::EncodeType::TypeDefault)
    {
        chunk_codec_stream = std::make_unique<DefaultChunkCodec>()->newCodecStream(result_field_types);
    }
    else if (encode_type == tipb::EncodeType::TypeChunk)
    {
        chunk_codec_stream = std::make_unique<ArrowChunkCodec>()->newCodecStream(result_field_types);
    }
    else if (encode_type == tipb::EncodeType::TypeCHBlock)
    {
        chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(result_field_types);
        records_per_chunk = -1;
    }
    else
    {
        throw Exception(
            "Only Default and Arrow encode type is supported in StreamingDAGBlockOutputStream.", ErrorCodes::UNSUPPORTED_PARAMETER);
    }
    children.push_back(input_);
}

void StreamingDAGBlockInputStream::readPrefix() { children.back()->readPrefix(); }

void StreamingDAGBlockInputStream::encodeChunkToDAGResponse()
{
    ::coprocessor::BatchResponse resp;

    tipb::SelectResponse dag_response;
    dag_response.set_encode_type(encode_type);
    auto dag_chunk = dag_response.add_chunks();
    dag_chunk->set_rows_data(chunk_codec_stream->getString());
    chunk_codec_stream->clear();
    dag_response.add_output_counts(current_records_num);
    current_records_num = 0;
    std::string dag_data;
    dag_response.SerializeToString(&dag_data);
    resp.set_data(dag_data);

    writer->write(resp);
}

void StreamingDAGBlockInputStream::readSuffix()
{
    // todo error handle
    if (current_records_num > 0)
    {
        encodeChunkToDAGResponse();
    }
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
        if (block.columns() != result_field_types.size())
            throw Exception("Output column size mismatch with field type size", ErrorCodes::LOGICAL_ERROR);
        if (records_per_chunk == -1)
        {
            current_records_num = 0;
            if (block.rows() > 0)
            {
                chunk_codec_stream->encode(block, 0, block.rows());
                encodeChunkToDAGResponse();
            }
        }
        else
        {
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
    }
    return {};
}

} // namespace DB
