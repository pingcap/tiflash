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

#include <Common/TiFlashException.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Mpp/BroadcastOrPassThroughWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>

namespace DB
{
template <class StreamWriterPtr>
BroadcastOrPassThroughWriter<StreamWriterPtr>::BroadcastOrPassThroughWriter(
    StreamWriterPtr writer_,
    Int64 batch_send_min_limit_,
    bool should_send_exec_summary_at_last_,
    DAGContext & dag_context_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
    , writer(writer_)
{
    rows_in_blocks = 0;
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
    chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(dag_context.result_field_types);
}

template <class StreamWriterPtr>
void BroadcastOrPassThroughWriter<StreamWriterPtr>::finishWrite()
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

template <class StreamWriterPtr>
void BroadcastOrPassThroughWriter<StreamWriterPtr>::flush()
{
    if (rows_in_blocks > 0)
        encodeThenWriteBlocks<false>();
}

template <class StreamWriterPtr>
void BroadcastOrPassThroughWriter<StreamWriterPtr>::write(const Block & block)
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

template <class StreamWriterPtr>
template <bool send_exec_summary_at_last>
void BroadcastOrPassThroughWriter<StreamWriterPtr>::encodeThenWriteBlocks()
{
    TrackedMppDataPacket tracked_packet(current_memory_tracker);
    if constexpr (send_exec_summary_at_last)
    {
        TrackedSelectResp response;
        summary_collector.addExecuteSummaries(response.getResponse(), /*delta_mode=*/false);
        tracked_packet.serializeByResponse(response.getResponse());
    }
    if (blocks.empty())
    {
        if constexpr (send_exec_summary_at_last)
        {
            writer->write(tracked_packet.getPacket());
        }
        return;
    }
    while (!blocks.empty())
    {
        const auto & block = blocks.back();
        chunk_codec_stream->encode(block, 0, block.rows());
        blocks.pop_back();
        tracked_packet.addChunk(chunk_codec_stream->getString());
        chunk_codec_stream->clear();
    }
    assert(blocks.empty());
    rows_in_blocks = 0;
    writer->write(tracked_packet.getPacket());
}

template class BroadcastOrPassThroughWriter<MPPTunnelSetPtr>;

} // namespace DB
