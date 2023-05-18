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
#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/BroadcastOrPassThroughWriter.h>
#include <Flash/Mpp/MPPTunnelSetWriter.h>

namespace DB
{
template <class ExchangeWriterPtr>
BroadcastOrPassThroughWriter<ExchangeWriterPtr>::BroadcastOrPassThroughWriter(
    ExchangeWriterPtr writer_,
    Int64 batch_send_min_limit_,
    DAGContext & dag_context_,
    MPPDataPacketVersion data_codec_version_,
    tipb::CompressionMode compression_mode_,
    tipb::ExchangeType exchange_type_)
    : DAGResponseWriter(/*records_per_chunk=*/-1, dag_context_)
    , batch_send_min_limit(batch_send_min_limit_)
    , writer(writer_)
    , exchange_type(exchange_type_)
    , data_codec_version(data_codec_version_)
    , compression_method(ToInternalCompressionMethod(compression_mode_))
{
    rows_in_blocks = 0;
    RUNTIME_CHECK(dag_context.encode_type == tipb::EncodeType::TypeCHBlock);
    RUNTIME_CHECK(exchange_type == tipb::ExchangeType::Broadcast || exchange_type == tipb::ExchangeType::PassThrough);

    switch (data_codec_version)
    {
    case MPPDataPacketV0:
        break;
    case MPPDataPacketV1:
    default:
    {
        // make `batch_send_min_limit` always GT 0
        if (batch_send_min_limit <= 0)
        {
            // set upper limit if not specified
            batch_send_min_limit = 8 * 1024 /* 8K */;
        }
        for (const auto & field_type : dag_context.result_field_types)
        {
            expected_types.emplace_back(getDataTypeByFieldTypeForComputingLayer(field_type));
        }
        break;
    }
    }
}

template <class ExchangeWriterPtr>
void BroadcastOrPassThroughWriter<ExchangeWriterPtr>::flush()
{
    if (rows_in_blocks > 0)
        writeBlocks();
}

template <class ExchangeWriterPtr>
bool BroadcastOrPassThroughWriter<ExchangeWriterPtr>::isWritable() const
{
    return writer->isWritable();
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
        writeBlocks();
}

template <class ExchangeWriterPtr>
void BroadcastOrPassThroughWriter<ExchangeWriterPtr>::writeBlocks()
{
    if unlikely (blocks.empty())
        return;

    // check schema
    if (!expected_types.empty())
    {
        for (auto && block : blocks)
            assertBlockSchema(expected_types, block, "BroadcastOrPassThroughWriter");
    }

    if (exchange_type == tipb::ExchangeType::Broadcast)
        writer->broadcastWrite(blocks, data_codec_version, compression_method);
    else
        writer->passThroughWrite(blocks, data_codec_version, compression_method);
    blocks.clear();
    rows_in_blocks = 0;
}

template class BroadcastOrPassThroughWriter<SyncMPPTunnelSetWriterPtr>;
template class BroadcastOrPassThroughWriter<AsyncMPPTunnelSetWriterPtr>;
} // namespace DB
