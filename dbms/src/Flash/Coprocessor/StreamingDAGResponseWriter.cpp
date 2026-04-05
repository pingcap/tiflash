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

#include <Common/Logger.h>
#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Flash/Coprocessor/StreamWriter.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/MPPTunnelSetWriter.h>

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
    UInt64 max_buffered_bytes_,
    DAGContext & dag_context_)
    : DAGResponseWriter(records_per_chunk_, dag_context_)
    , writer(writer_)
{
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
    auto batch_send_min_limit
        = dag_context.encode_type == tipb::EncodeType::TypeCHBlock ? batch_send_min_limit_ : (records_per_chunk - 1);
    if (batch_send_min_limit <= 0)
        max_buffered_rows = 1;
    else
        max_buffered_rows = static_cast<UInt64>(batch_send_min_limit);
    max_buffered_bytes = max_buffered_bytes_;
}

template <class StreamWriterPtr>
WriteResult StreamingDAGResponseWriter<StreamWriterPtr>::flush()
{
    has_pending_flush = false;
    if (buffered_rows > 0)
    {
        auto wait_res = waitForWritable();
        if (wait_res == WaitResult::Ready)
        {
            encodeThenWriteBlocks();
            return WriteResult::Done;
        }
        // set has_pending_flush to true since current flush is not done
        has_pending_flush = true;
        return wait_res == WaitResult::WaitForPolling ? WriteResult::NeedWaitForPolling
                                                      : WriteResult::NeedWaitForNotify;
    }
    return WriteResult::Done;
}

template <class StreamWriterPtr>
WaitResult StreamingDAGResponseWriter<StreamWriterPtr>::waitForWritable() const
{
    return writer->waitForWritable();
}

template <class StreamWriterPtr>
WriteResult StreamingDAGResponseWriter<StreamWriterPtr>::write(const Block & block)
{
    assert(has_pending_flush == false);
    RUNTIME_CHECK_MSG(
        block.columns() == dag_context.result_field_types.size(),
        "Output column size mismatch with field type size");
    size_t rows = block.rows();
    if (rows > 0)
    {
        buffered_rows += rows;
        buffered_bytes += block.allocatedBytes();
        blocks.push_back(block);
    }

    if (needFlush())
    {
        return flush();
    }
    return WriteResult::Done;
}

template <class StreamWriterPtr>
void StreamingDAGResponseWriter<StreamWriterPtr>::encodeThenWriteBlocks()
{
    assert(!blocks.empty());

    TrackedSelectResp response;
    response.setEncodeType(dag_context.encode_type);
    if (dag_context.encode_type == tipb::EncodeType::TypeCHBlock)
    {
        /// passthrough data to a non-TiFlash node, like sending data to TiSpark
        for (auto & block : blocks)
        {
            chunk_codec_stream->encode(block, 0, block.rows());
            block.clear();
            response.addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
        blocks.clear();
    }
    else /// passthrough data to a TiDB node
    {
        Int64 current_records_num = 0;
        for (auto & block : blocks)
        {
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
            block.clear();
        }
        blocks.clear();

        if (current_records_num > 0)
        {
            response.addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
    }

    assert(blocks.empty());
    buffered_rows = 0;
    buffered_bytes = 0;
    writer->write(response.getResponse());
}

template class StreamingDAGResponseWriter<CopStreamWriterPtr>;
template class StreamingDAGResponseWriter<BatchCopStreamWriterPtr>;
template class StreamingDAGResponseWriter<SyncMPPTunnelSetWriterPtr>;
template class StreamingDAGResponseWriter<AsyncMPPTunnelSetWriterPtr>;
} // namespace DB
