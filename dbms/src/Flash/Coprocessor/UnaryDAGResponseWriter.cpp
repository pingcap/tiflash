#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/UnaryDAGResponseWriter.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

UnaryDAGResponseWriter::UnaryDAGResponseWriter(tipb::SelectResponse * dag_response_, Int64 records_per_chunk_,
    tipb::EncodeType encode_type_, std::vector<tipb::FieldType> result_field_types_, DAGContext & dag_context_)
    : DAGResponseWriter(records_per_chunk_, encode_type_, result_field_types_, dag_context_), dag_response(dag_response_)
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
    }
    dag_response->set_encode_type(encode_type);
    current_records_num = 0;
}

void UnaryDAGResponseWriter::encodeChunkToDAGResponse()
{
    auto dag_chunk = dag_response->add_chunks();
    dag_chunk->set_rows_data(chunk_codec_stream->getString());
    chunk_codec_stream->clear();
    current_records_num = 0;
}

void UnaryDAGResponseWriter::appendWarningsToDAGResponse()
{
    std::vector<tipb::Error> warnings{};
    dag_context.consumeWarnings(warnings);
    for (auto & warning : warnings)
    {
        auto warn = dag_response->add_warnings();
        // TODO: consider using allocated warnings to prevent copy?
        warn->CopyFrom(warning);
    }
}

void UnaryDAGResponseWriter::finishWrite()
{
    if (current_records_num > 0)
    {
        encodeChunkToDAGResponse();
        appendWarningsToDAGResponse();
    }
    addExecuteSummaries(*dag_response, false);
}

void UnaryDAGResponseWriter::write(const Block & block)
{
    if (block.columns() != result_field_types.size())
        throw TiFlashException("Output column size mismatch with field type size", Errors::Coprocessor::Internal);
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
