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

template <class StreamWriterPtr, bool is_mpp>
StreamingDAGResponseWriter<StreamWriterPtr, is_mpp>::StreamingDAGResponseWriter(
    StreamWriterPtr writer_,
    Int64 records_per_chunk_,
    Int64 batch_send_min_limit_,
    bool should_send_exec_summary_at_last_,
    DAGContext & dag_context_)
    : DAGResponseWriter(records_per_chunk_, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
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

template <class StreamWriterPtr, bool is_mpp>
void StreamingDAGResponseWriter<StreamWriterPtr, is_mpp>::finishWrite()
{
    if (should_send_exec_summary_at_last)
    {
        encodeThenWriteBlocks<true>();
    }
    else
    {
        encodeThenWriteBlocks<false>();
    }
}

template <class StreamWriterPtr, bool is_mpp>
void StreamingDAGResponseWriter<StreamWriterPtr, is_mpp>::write(const Block & block)
{
    RUNTIME_CHECK_MSG(
        block.columns() == dag_context.result_field_types.size(),
        "Output column size mismatch with field type size");
    size_t rows = block.rows();
    rows_in_blocks += rows;
    if (rows > 0)
    {
        blocks.push_back(block);
    }

    if (static_cast<Int64>(rows_in_blocks) > batch_send_min_limit)
        encodeThenWriteBlocks<false>();
}

template <class StreamWriterPtr, bool is_mpp>
void StreamingDAGResponseWriter<StreamWriterPtr, is_mpp>::encode(std::function<void(String &&)> add_chunk)
{
    if (dag_context.encode_type == tipb::EncodeType::TypeCHBlock)
    {
        /// passthrough data to a non-TiFlash node, like sending data to TiSpark
        while (!blocks.empty())
        {
            const auto & block = blocks.back();
            chunk_codec_stream->encode(block, 0, block.rows());
            blocks.pop_back();
            add_chunk(chunk_codec_stream->getString());
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
                    add_chunk(chunk_codec_stream->getString());
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
            add_chunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }
    }
    assert(blocks.empty());
    rows_in_blocks = 0;
}

template <class StreamWriterPtr, bool is_mpp>
template <bool send_exec_summary_at_last>
void StreamingDAGResponseWriter<StreamWriterPtr, is_mpp>::encodeThenWriteBlocks()
{
    if (!blocks.empty())
    {
        if constexpr (is_mpp)
        {
            // for mpp, we can send `TrackedMppDataPacket` directly without converting the `SelectResp` to `TrackedMppDataPacket` in MPPTunnelSetBase.
            auto tracked_packet = std::make_shared<TrackedMppDataPacket>();
            encode([&tracked_packet](String && chunk) {
                tracked_packet->addChunk(std::move(chunk));
            });
            writer->write(tracked_packet);
        }
        else
        {
            TrackedSelectResp response;
            response.setEncodeType(dag_context.encode_type);
            encode([&response](String && chunk) {
                response.addChunk(std::move(chunk));
            });
            writer->write(response.getResponse());
        }
    }

    if constexpr (send_exec_summary_at_last)
    {
        TrackedSelectResp response;
        response.setEncodeType(dag_context.encode_type);
        summary_collector.addExecuteSummaries(response.getResponse(), /*delta_mode=*/true);
        if constexpr (is_mpp)
        {
            auto tracked_packet = std::make_shared<TrackedMppDataPacket>();
            tracked_packet->serializeByResponse(response.getResponse());
            writer->write(tracked_packet, 0);
        }
        else
        {
            writer->write(response.getResponse());
        }
    }
}

template class StreamingDAGResponseWriter<StreamWriterPtr, false>;
template class StreamingDAGResponseWriter<MPPTunnelSetPtr, true>;
} // namespace DB
