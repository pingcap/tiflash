#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGBlockOutputStream.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <>
DAGBlockOutputStream<false>::DAGBlockOutputStream(tipb::SelectResponse * dag_response_, Int64 records_per_chunk_,
    tipb::EncodeType encode_type_, std::vector<tipb::FieldType> result_field_types_, Block && header_)
    : dag_response(dag_response_),
      result_field_types(std::move(result_field_types_)),
      header(std::move(header_)),
      records_per_chunk(records_per_chunk_),
      encode_type(encode_type_),
      current_records_num(0)
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
        throw Exception("Only Default and Arrow encode type is supported in DAGBlockOutputStream.", ErrorCodes::UNSUPPORTED_PARAMETER);
    }
    dag_response->set_encode_type(encode_type);
}

template <>
DAGBlockOutputStream<true>::DAGBlockOutputStream(::grpc::ServerWriter<::coprocessor::BatchResponse> * writer_, Int64 records_per_chunk_,
    tipb::EncodeType encode_type_, std::vector<tipb::FieldType> result_field_types_, Block && header_)
    : writer(writer_),
      result_field_types(std::move(result_field_types_)),
      header(std::move(header_)),
      records_per_chunk(records_per_chunk_),
      encode_type(encode_type_),
      current_records_num(0)
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
}

template <bool streaming>
void DAGBlockOutputStream<streaming>::writePrefix()
{
    //something to do here?
}

template <>
void DAGBlockOutputStream<false>::encodeChunkToDAGResponse()
{
    auto dag_chunk = dag_response->add_chunks();
    dag_chunk->set_rows_data(chunk_codec_stream->getString());
    chunk_codec_stream->clear();
    dag_response->add_output_counts(current_records_num);
    current_records_num = 0;
}

template <>
void DAGBlockOutputStream<true>::encodeChunkToDAGResponse()
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

    writer->Write(resp);
}

template <bool streaming>
void DAGBlockOutputStream<streaming>::writeSuffix()
{
    // todo error handle
    if (current_records_num > 0)
    {
        encodeChunkToDAGResponse();
    }
}

template <bool streaming>
void DAGBlockOutputStream<streaming>::write(const Block & block)
{
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

} // namespace DB
