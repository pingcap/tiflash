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
#include <Flash/Coprocessor/CompressedCHBlockChunkCodec.h>
#include <Flash/Mpp/BroadcastOrPassThroughWriter.h>
#include <Flash/Mpp/MPPTunnelSet.h>

namespace DB
{
template <class ExchangeWriterPtr>
BroadcastOrPassThroughWriter<ExchangeWriterPtr>::BroadcastOrPassThroughWriter(
    ExchangeWriterPtr writer_,
    Int64 batch_send_min_limit_,
    bool should_send_exec_summary_at_last_,
    DAGContext & dag_context_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , should_send_exec_summary_at_last(should_send_exec_summary_at_last_)
    , writer(writer_)
    , compress_method(dag_context.getExchangeSenderMeta().compress())
{
    rows_in_blocks = 0;
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
    chunk_codec_stream = std::make_unique<CHBlockChunkCodec>()->newCodecStream(dag_context.result_field_types);
    if (auto method = ToInternalCompressionMethod(compress_method); method != CompressionMethod::NONE)
    {
        compress_chunk_codec_stream = CompressedCHBlockChunkCodec::newCodecStream(dag_context.result_field_types, method);
    }
}

template <class ExchangeWriterPtr>
void BroadcastOrPassThroughWriter<ExchangeWriterPtr>::finishWrite()
{
    assert(0 == rows_in_blocks);
    if (should_send_exec_summary_at_last)
        sendExecutionSummary();
}

template <class ExchangeWriterPtr>
void BroadcastOrPassThroughWriter<ExchangeWriterPtr>::sendExecutionSummary()
{
    tipb::SelectResponse response;
    summary_collector.addExecuteSummaries(response);
    writer->sendExecutionSummary(response);
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
    rows_in_blocks += rows;
    if (rows > 0)
    {
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

    auto tracked_packet = std::make_shared<TrackedMppDataPacket>();
    decltype(tracked_packet) compressed_tracked_packet = {};
    bool need_compress = compress_method != mpp::CompressMethod::NONE;
    if (need_compress)
    {
        auto all_is_local = std::all_of(writer->getTunnels().begin(), writer->getTunnels().end(), [](const auto & tunnel) {
            return tunnel->isLocal();
        });
        if (all_is_local)
            need_compress = false;
    }
    if (need_compress)
    {
        compressed_tracked_packet = std::make_shared<TrackedMppDataPacket>();
    }

    while (!blocks.empty())
    {
        const auto & block = blocks.back();

        if (need_compress)
        {
            assert(compressed_tracked_packet);
            compress_chunk_codec_stream->encode(block, 0, block.rows());
            compressed_tracked_packet->addChunk(compress_chunk_codec_stream->getString());
            compress_chunk_codec_stream->clear();
        }

        {
            assert(tracked_packet);
            chunk_codec_stream->encode(block, 0, block.rows());
            tracked_packet->addChunk(chunk_codec_stream->getString());
            chunk_codec_stream->clear();
        }

        blocks.pop_back();
    }
    assert(blocks.empty());
    rows_in_blocks = 0;

    if (!need_compress)
    {
        writer->broadcastOrPassThroughWrite(tracked_packet);
    }
    else
    {
        writer->broadcastOrPassThroughWrite(tracked_packet, compressed_tracked_packet);
    }
}

template class BroadcastOrPassThroughWriter<MPPTunnelSetPtr>;

} // namespace DB
