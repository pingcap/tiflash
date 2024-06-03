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

#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/BroadcastOrPassThroughWriter.h>
#include <Flash/Mpp/FineGrainedShuffleWriter.h>
#include <Flash/Mpp/HashPartitionWriter.h>
#include <Flash/Mpp/MPPTunnelSetWriter.h>
#include <Flash/Mpp/MppVersion.h>

namespace DB
{
namespace
{
template <typename ExchangeWriterPtr, bool selective_block>
std::unique_ptr<DAGResponseWriter> buildMPPExchangeWriter(
    const ExchangeWriterPtr & writer,
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & partition_col_collators,
    const tipb::ExchangeType & exchange_type,
    Int64 records_per_chunk,
    Int64 batch_send_min_limit,
    DAGContext & dag_context,
    bool enable_fine_grained_shuffle,
    UInt64 fine_grained_shuffle_stream_count,
    UInt64 fine_grained_shuffle_batch_size,
    tipb::CompressionMode compression_mode,
    Int64 batch_send_min_limit_compression)
{
    if (dag_context.isRootMPPTask())
    {
        // No need to use use data compression
        RUNTIME_CHECK(compression_mode == tipb::CompressionMode::NONE);

        RUNTIME_CHECK(!enable_fine_grained_shuffle);
        RUNTIME_CHECK(exchange_type == tipb::ExchangeType::PassThrough);
        // selective_block is only used for 1st HashAgg, so only for hash partition or fine grained shuffle.
        RUNTIME_CHECK(!selective_block);
        return std::make_unique<StreamingDAGResponseWriter<ExchangeWriterPtr>>(
            writer,
            records_per_chunk,
            batch_send_min_limit,
            dag_context);
    }
    else
    {
        auto mpp_version = dag_context.getMPPTaskMeta().mpp_version();
        auto data_codec_version = mpp_version == MppVersionV0 ? MPPDataPacketV0 : MPPDataPacketV1;
        auto chosen_batch_send_min_limit
            = mpp_version == MppVersionV0 ? batch_send_min_limit : batch_send_min_limit_compression;

        if (exchange_type == tipb::ExchangeType::Hash)
        {
            if (enable_fine_grained_shuffle)
            {
                return std::make_unique<FineGrainedShuffleWriter<ExchangeWriterPtr, selective_block>>(
                    writer,
                    partition_col_ids,
                    partition_col_collators,
                    dag_context,
                    fine_grained_shuffle_stream_count,
                    fine_grained_shuffle_batch_size,
                    data_codec_version,
                    compression_mode);
            }
            else
            {
                return std::make_unique<HashPartitionWriter<ExchangeWriterPtr, selective_block>>(
                    writer,
                    partition_col_ids,
                    partition_col_collators,
                    chosen_batch_send_min_limit,
                    dag_context,
                    data_codec_version,
                    compression_mode);
            }
        }
        else
        {
            RUNTIME_CHECK(!enable_fine_grained_shuffle);
            // selective_block is only used for 1st HashAgg, so only for hash partition or fine grained shuffle.
            RUNTIME_CHECK(!selective_block);
            return std::make_unique<BroadcastOrPassThroughWriter<ExchangeWriterPtr>>(
                writer,
                chosen_batch_send_min_limit,
                dag_context,
                data_codec_version,
                compression_mode,
                exchange_type);
        }
    }
}
} // namespace

std::unique_ptr<DAGResponseWriter> newMPPExchangeWriter(
    const std::vector<Int64> & partition_col_ids,
    const TiDB::TiDBCollators & partition_col_collators,
    const tipb::ExchangeType & exchange_type,
    Int64 records_per_chunk,
    Int64 batch_send_min_limit,
    DAGContext & dag_context,
    bool enable_fine_grained_shuffle,
    UInt64 fine_grained_shuffle_stream_count,
    UInt64 fine_grained_shuffle_batch_size,
    tipb::CompressionMode compression_mode,
    Int64 batch_send_min_limit_compression,
    const String & req_id,
    bool is_async,
    bool selective_block)
{
    RUNTIME_CHECK_MSG(dag_context.isMPPTask() && dag_context.tunnel_set != nullptr, "exchange writer only run in MPP");
    if (is_async)
    {
        auto writer
            = std::make_shared<AsyncMPPTunnelSetWriter>(dag_context.tunnel_set, dag_context.result_field_types, req_id);
        if (selective_block)
            return buildMPPExchangeWriter<decltype(writer), true>(
                writer,
                partition_col_ids,
                partition_col_collators,
                exchange_type,
                records_per_chunk,
                batch_send_min_limit,
                dag_context,
                enable_fine_grained_shuffle,
                fine_grained_shuffle_stream_count,
                fine_grained_shuffle_batch_size,
                compression_mode,
                batch_send_min_limit_compression);
        else
            return buildMPPExchangeWriter<decltype(writer), false>(
                writer,
                partition_col_ids,
                partition_col_collators,
                exchange_type,
                records_per_chunk,
                batch_send_min_limit,
                dag_context,
                enable_fine_grained_shuffle,
                fine_grained_shuffle_stream_count,
                fine_grained_shuffle_batch_size,
                compression_mode,
                batch_send_min_limit_compression);
    }
    else
    {
        auto writer
            = std::make_shared<SyncMPPTunnelSetWriter>(dag_context.tunnel_set, dag_context.result_field_types, req_id);
        if (selective_block)
            return buildMPPExchangeWriter<decltype(writer), true>(
                writer,
                partition_col_ids,
                partition_col_collators,
                exchange_type,
                records_per_chunk,
                batch_send_min_limit,
                dag_context,
                enable_fine_grained_shuffle,
                fine_grained_shuffle_stream_count,
                fine_grained_shuffle_batch_size,
                compression_mode,
                batch_send_min_limit_compression);
        else
            return buildMPPExchangeWriter<decltype(writer), false>(
                writer,
                partition_col_ids,
                partition_col_collators,
                exchange_type,
                records_per_chunk,
                batch_send_min_limit,
                dag_context,
                enable_fine_grained_shuffle,
                fine_grained_shuffle_stream_count,
                fine_grained_shuffle_batch_size,
                compression_mode,
                batch_send_min_limit_compression);
    }
}
} // namespace DB
