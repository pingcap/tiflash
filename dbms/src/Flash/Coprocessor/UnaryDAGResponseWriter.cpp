// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/UnaryDAGResponseWriter.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

UnaryDAGResponseWriter::UnaryDAGResponseWriter(
    tipb::SelectResponse * dag_response_,
    Int64 records_per_chunk_,
    DAGContext & dag_context_)
    : DAGResponseWriter(records_per_chunk_, dag_context_)
    , dag_response(dag_response_)
{
    const auto packet_version
        = GetMPPDataPacketVersion(static_cast<MppVersion>(dag_context.getMPPTaskMeta().mpp_version()));
    if (dag_context.encode_type == tipb::EncodeType::TypeDefault)
    {
        chunk_codec_stream = std::make_unique<DefaultChunkCodec>()->newCodecStream(dag_context.result_field_types);
    }
    else if (dag_context.encode_type == tipb::EncodeType::TypeChunk)
    {
        chunk_codec_stream = std::make_unique<ArrowChunkCodec>()->newCodecStream(dag_context.result_field_types);
    }
    else if (dag_context.encode_type == tipb::EncodeType::TypeCHBlock)
    {
        chunk_codec_stream
            = std::make_unique<CHBlockChunkCodec>(packet_version)->newCodecStream(dag_context.result_field_types);
    }
    dag_response->set_encode_type(dag_context.encode_type);
    current_records_num = 0;
}

void UnaryDAGResponseWriter::encodeChunkToDAGResponse()
{
    auto * dag_chunk = dag_response->add_chunks();
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
        auto * warn = dag_response->add_warnings();
        // TODO: consider using allocated warnings to prevent copy?
        warn->CopyFrom(warning);
    }
    dag_response->set_warning_count(dag_context.getWarningCount());
}

WriteResult UnaryDAGResponseWriter::flush()
{
    if (current_records_num > 0)
    {
        encodeChunkToDAGResponse();
    }
    // TODO separate from UnaryDAGResponseWriter and support mpp/batchCop.
    dag_context.fillWarnings(*dag_response);

    // Under some test cases, there may be dag response whose size is bigger than INT_MAX, and GRPC can not limit it.
    // Throw exception to prevent receiver from getting wrong response.
    if (unlikely(accurate::greaterOp(dag_response->ByteSizeLong(), std::numeric_limits<int>::max())))
        throw TiFlashException(
            "DAG response is too big, please check config about region size or region merge scheduler",
            Errors::Coprocessor::Internal);
    return WriteResult::Done;
}

WriteResult UnaryDAGResponseWriter::write(const Block & block)
{
    assert(has_pending_flush == false);
    if (block.columns() != dag_context.result_field_types.size())
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
    return WriteResult::Done;
}
} // namespace DB
