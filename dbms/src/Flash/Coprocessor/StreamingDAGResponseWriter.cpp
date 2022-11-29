// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/StreamWriter.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED_PARAMETER;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

template <class StreamWriterPtr>
StreamingDAGResponseWriter<StreamWriterPtr>::StreamingDAGResponseWriter(
    StreamWriterPtr writer_,
    Int64 records_per_chunk_,
    Int64 batch_send_min_limit_,
    DAGContext & dag_context_)
    : DAGResponseWriter(records_per_chunk_, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , writer(writer_)
{
    rows_in_blocks = 0;
    switch (dag_context.encode_type)
    {
    case tipb::EncodeType::TypeDefault:
        chunk_codec_stream = std::make_unique<DefaultChunkCodec>()->newCodecStream(dag_context.result_field_types);
        break;
    case tipb::EncodeType::TypeChunk:
        chunk_codec_stream = std::make_unique<ArrowChunkCodec>()->newCodecStream(dag_context.result_field_types);
        break;
    case tipb::EncodeType::TypeCHBlock:
        chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(dag_context.result_field_types);
        break;
    default:
        throw TiFlashException("Unsupported EncodeType", Errors::Coprocessor::Internal);
    }
    /// For other encode types, we will use records_per_chunk to control the batch size sent.
    batch_send_min_limit = dag_context.encode_type == tipb::EncodeType::TypeCHBlock
        ? batch_send_min_limit
        : (records_per_chunk - 1);
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::flush()
{
    if (rows_in_blocks > 0)
        encodeThenWriteBlocks();
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::write(const Block & block)
{
    RUNTIME_CHECK_MSG(
        block.columns() == dag_context.result_field_types.size(),
        "Output column size mismatch with field type size");
    size_t rows = block.rows();
    if (rows > 0)
    {
        rows_in_blocks += rows;
        blocks.push_back(block);
    }

    if (static_cast<Int64>(rows_in_blocks) > batch_send_min_limit)
        encodeThenWriteBlocks();
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::encodeThenWriteBlocks()
{
    if (unlikely(blocks.empty()))
        return;

    TrackedSelectResp response;
    response.setEncodeType(dag_context.encode_type);
    if (dag_context.encode_type == tipb::EncodeType::TypeCHBlock)
    {
        /// passthrough data to a non-TiFlash node, like sending data to TiSpark
        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            chunk_codec_stream->encode(block, 0, block.rows());
            blocks.pop_back();
            response.addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
    }
    else /// passthrough data to a TiDB node
    {
        Int64 current_records_num = 0;
        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            size_t rows = block.rows();
            for (size_t row_index = 0; row_index < rows;)
            {
                if (current_records_num >= records_per_chunk)
                {
                    response.addChunk(chunk_codec_stream->getString());
                    chunk_codec_stream->clear();
                    current_records_num = 0;
                }
                const size_t upper = std::min(row_index + (records_per_chunk - current_records_num), rows);
                chunk_codec_stream->encode(block, row_index, upper);
                current_records_num += (upper - row_index);
                row_index = upper;
            }
            blocks.pop_back();
        }

        if (current_records_num > 0)
        {
            response.addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
    }

    assert(blocks.empty());
    rows_in_blocks = 0;
    writer->write(response.getResponse());
}

template class StreamingDAGResponseWriter<StreamWriterPtr>;
template class StreamingDAGResponseWriter<MPPTunnelSetPtr>;
} // namespace DB
