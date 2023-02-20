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

#include <Flash/Coprocessor/StreamingDAGResponseWriter.h>
#include <Flash/Mpp/BroadcastOrPassThroughWriter.h>
#include <Flash/Mpp/FineGrainedShuffleWriter.h>
#include <Flash/Mpp/HashPartitionWriter.h>
#include <Flash/Mpp/MPPTunnelSetWriter.h>
#include <Flash/Mpp/MppVersion.h>
#include <Flash/Coprocessor/DAGContext.h>

namespace DB
{
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
    const String & req_id)
{
    RUNTIME_CHECK_MSG(dag_context.isMPPTask() && dag_context.tunnel_set != nullptr, "exchange writer only run in MPP");
    auto writer = std::make_shared<SyncMPPTunnelSetWriter>(dag_context.tunnel_set, dag_context.result_field_types, req_id);
    if (dag_context.isRootMPPTask())
    {
        // No need to use use data compression
        RUNTIME_CHECK(compression_mode == tipb::CompressionMode::NONE);

        RUNTIME_CHECK(!enable_fine_grained_shuffle);
        RUNTIME_CHECK(exchange_type == tipb::ExchangeType::PassThrough);
        return std::make_unique<StreamingDAGResponseWriter<SyncMPPTunnelSetWriterPtr>>(
            writer,
            records_per_chunk,
            batch_send_min_limit,
            dag_context);
    }
    else
    {
        if (exchange_type == tipb::ExchangeType::Hash)
        {
            auto mpp_version = dag_context.getMPPTaskMeta().mpp_version();
            auto data_codec_version = mpp_version == MppVersionV0
                ? MPPDataPacketV0
                : MPPDataPacketV1;

            if (enable_fine_grained_shuffle)
            {
                return std::make_unique<FineGrainedShuffleWriter<SyncMPPTunnelSetWriterPtr>>(
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
                auto chosen_batch_send_min_limit = mpp_version == MppVersionV0
                    ? batch_send_min_limit
                    : batch_send_min_limit_compression;

                return std::make_unique<HashPartitionWriter<SyncMPPTunnelSetWriterPtr>>(
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
            // TODO: support data compression if necessary
            RUNTIME_CHECK(compression_mode == tipb::CompressionMode::NONE);

            RUNTIME_CHECK(!enable_fine_grained_shuffle);
            return std::make_unique<BroadcastOrPassThroughWriter<SyncMPPTunnelSetWriterPtr>>(
                writer,
                batch_send_min_limit,
                dag_context);
        }
    }
}

} // namespace DB
