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
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Mpp/BroadcastOrPassThroughWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/MppVersion.h>

namespace DB
{
template <class ExchangeWriterPtr>
BroadcastOrPassThroughWriter<ExchangeWriterPtr>::BroadcastOrPassThroughWriter(
    ExchangeWriterPtr writer_,
    Int64 batch_send_min_limit_,
    DAGContext & dag_context_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , writer(writer_)
{
    rows_in_blocks = 0;
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
    chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(dag_context.result_field_types);
}

template <class ExchangeWriterPtr>
void BroadcastOrPassThroughWriter<ExchangeWriterPtr>::flush()
{
    if (rows_in_blocks > 0)
        encodeThenWriteBlocks();
}

template <class ExchangeWriterPtr>
void BroadcastOrPassThroughWriter<ExchangeWriterPtr>::write(const Block & block)
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

template <class ExchangeWriterPtr>
void BroadcastOrPassThroughWriter<ExchangeWriterPtr>::encodeThenWriteBlocks()
{
    if (unlikely(blocks.empty()))
        return;

    auto tracked_packet = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
    while (!blocks.empty())
    {
        const auto & block = blocks.back();
        chunk_codec_stream->encode(block, 0, block.rows());
        blocks.pop_back();
        tracked_packet->addChunk(chunk_codec_stream->getString());
        chunk_codec_stream->clear();
    }
    assert(blocks.empty());
    rows_in_blocks = 0;
    auto packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    writer->broadcastOrPassThroughWrite(std::move(tracked_packet));

    {
        auto tunnel_cnt = writer->getPartitionNum();
        size_t local_tunnel_cnt = 0;
        for (size_t i = 0; i < tunnel_cnt; ++i)
        {
            local_tunnel_cnt += writer->isLocal(i);
        }
        auto original = packet_bytes * tunnel_cnt;
        auto local = packet_bytes * local_tunnel_cnt;
        GET_METRIC(tiflash_exchange_data_bytes, type_broadcast_passthrough_original).Increment(original);
        GET_METRIC(tiflash_exchange_data_bytes, type_broadcast_passthrough_none_compression_local).Increment(local);
        GET_METRIC(tiflash_exchange_data_bytes, type_broadcast_passthrough_none_compression_remote).Increment(original - local);
    }
}

template class BroadcastOrPassThroughWriter<MPPTunnelSetPtr>;

} // namespace DB
