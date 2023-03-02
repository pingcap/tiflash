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

#include <Common/Exception.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>
#include <Flash/Mpp/MPPTunnelSetHelper.h>
#include <Flash/Mpp/MPPTunnelSetWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Flash/Mpp/Utils.h>
#include <fmt/core.h>

namespace DB
{
namespace
{
void checkPacketSize(size_t size)
{
    static constexpr size_t max_packet_size = 1u << 31;
    RUNTIME_CHECK_MSG(size < max_packet_size, "Packet is too large to send, size : {}", size);
}

void updatePartitionWriterMetrics(size_t packet_bytes, bool is_local)
{
    // statistic
    GET_METRIC(tiflash_exchange_data_bytes, type_hash_original).Increment(packet_bytes);
    // compression method is always NONE
    if (is_local)
        GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_compression_local).Increment(packet_bytes);
    else
        GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_compression_remote).Increment(packet_bytes);
}

void updatePartitionWriterMetrics(CompressionMethod method, size_t original_size, size_t sz, bool is_local)
{
    // statistic
    GET_METRIC(tiflash_exchange_data_bytes, type_hash_original).Increment(original_size);

    switch (method)
    {
    case CompressionMethod::NONE:
    {
        if (is_local)
        {
            GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_compression_local).Increment(sz);
        }
        else
        {
            GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_compression_remote).Increment(sz);
        }
        break;
    }
    case CompressionMethod::LZ4:
    {
        GET_METRIC(tiflash_exchange_data_bytes, type_hash_lz4_compression).Increment(sz);
        break;
    }
    case CompressionMethod::ZSTD:
    {
        GET_METRIC(tiflash_exchange_data_bytes, type_hash_zstd_compression).Increment(sz);
        break;
    }
    default:
        break;
    }
}
} // namespace

MPPTunnelSetWriterBase::MPPTunnelSetWriterBase(
    const MPPTunnelSetPtr & mpp_tunnel_set_,
    const std::vector<tipb::FieldType> & result_field_types_,
    const String & req_id)
    : mpp_tunnel_set(mpp_tunnel_set_)
    , result_field_types(result_field_types_)
    , log(Logger::get(req_id))
{
    RUNTIME_CHECK(mpp_tunnel_set->getPartitionNum() > 0);
}

void MPPTunnelSetWriterBase::write(tipb::SelectResponse & response)
{
    checkPacketSize(response.ByteSizeLong());
    // for root mpp task, only one tunnel will connect to tidb/tispark.
    writeToTunnel(response, 0);
}

void MPPTunnelSetWriterBase::broadcastOrPassThroughWrite(Blocks & blocks)
{
    auto && tracked_packet = MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types);
    if (!tracked_packet)
        return;

    auto packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    checkPacketSize(packet_bytes);
    // TODO avoid copy packet for broadcast.
    for (size_t i = 1; i < getPartitionNum(); ++i)
        writeToTunnel(tracked_packet->copy(), i);
    writeToTunnel(std::move(tracked_packet), 0);
    {
        // statistic
        size_t data_bytes = 0;
        size_t local_data_bytes = 0;
        {
            auto tunnel_cnt = getPartitionNum();
            size_t local_tunnel_cnt = 0;
            for (size_t i = 0; i < tunnel_cnt; ++i)
            {
                local_tunnel_cnt += mpp_tunnel_set->isLocal(i);
            }
            data_bytes = packet_bytes * tunnel_cnt;
            local_data_bytes = packet_bytes * local_tunnel_cnt;
        }
        GET_METRIC(tiflash_exchange_data_bytes, type_broadcast_passthrough_original).Increment(data_bytes);
        GET_METRIC(tiflash_exchange_data_bytes, type_broadcast_passthrough_none_compression_local).Increment(local_data_bytes);
        GET_METRIC(tiflash_exchange_data_bytes, type_broadcast_passthrough_none_compression_remote).Increment(data_bytes - local_data_bytes);
    }
}

void MPPTunnelSetWriterBase::partitionWrite(Blocks & blocks, int16_t partition_id)
{
    auto && tracked_packet = MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types);
    if (!tracked_packet)
        return;
    auto packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    checkPacketSize(packet_bytes);
    writeToTunnel(std::move(tracked_packet), partition_id);
    updatePartitionWriterMetrics(packet_bytes, mpp_tunnel_set->isLocal(partition_id));
}

void MPPTunnelSetWriterBase::partitionWrite(
    const Block & header,
    std::vector<MutableColumns> && part_columns,
    int16_t partition_id,
    MPPDataPacketVersion version,
    CompressionMethod compression_method)
{
    assert(version > MPPDataPacketV0);

    bool is_local = mpp_tunnel_set->isLocal(partition_id);
    compression_method = is_local ? CompressionMethod::NONE : compression_method;

    size_t original_size = 0;
    auto tracked_packet = MPPTunnelSetHelper::ToPacket(header, std::move(part_columns), version, compression_method, original_size);
    if (!tracked_packet)
        return;

    auto packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    checkPacketSize(packet_bytes);
    writeToTunnel(std::move(tracked_packet), partition_id);
    updatePartitionWriterMetrics(compression_method, original_size, packet_bytes, is_local);
}

void MPPTunnelSetWriterBase::fineGrainedShuffleWrite(
    const Block & header,
    std::vector<IColumn::ScatterColumns> & scattered,
    size_t bucket_idx,
    UInt64 fine_grained_shuffle_stream_count,
    size_t num_columns,
    int16_t partition_id,
    MPPDataPacketVersion version,
    CompressionMethod compression_method)
{
    if (version == MPPDataPacketV0)
        return fineGrainedShuffleWrite(header, scattered, bucket_idx, fine_grained_shuffle_stream_count, num_columns, partition_id);

    bool is_local = mpp_tunnel_set->isLocal(partition_id);
    compression_method = is_local ? CompressionMethod::NONE : compression_method;

    size_t original_size = 0;
    auto tracked_packet = MPPTunnelSetHelper::ToFineGrainedPacket(
        header,
        scattered,
        bucket_idx,
        fine_grained_shuffle_stream_count,
        num_columns,
        version,
        compression_method,
        original_size);

    if unlikely (tracked_packet->getPacket().chunks_size() <= 0)
        return;

    auto packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    checkPacketSize(packet_bytes);
    writeToTunnel(std::move(tracked_packet), partition_id);
    updatePartitionWriterMetrics(compression_method, original_size, packet_bytes, is_local);
}

void MPPTunnelSetWriterBase::fineGrainedShuffleWrite(
    const Block & header,
    std::vector<IColumn::ScatterColumns> & scattered,
    size_t bucket_idx,
    UInt64 fine_grained_shuffle_stream_count,
    size_t num_columns,
    int16_t partition_id)
{
    auto tracked_packet = MPPTunnelSetHelper::ToFineGrainedPacketV0(
        header,
        scattered,
        bucket_idx,
        fine_grained_shuffle_stream_count,
        num_columns,
        result_field_types);

    if unlikely (tracked_packet->getPacket().chunks_size() <= 0)
        return;

    auto packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    checkPacketSize(packet_bytes);
    writeToTunnel(std::move(tracked_packet), partition_id);
    updatePartitionWriterMetrics(packet_bytes, mpp_tunnel_set->isLocal(partition_id));
}

void SyncMPPTunnelSetWriter::writeToTunnel(TrackedMppDataPacketPtr && data, size_t index)
{
    mpp_tunnel_set->write(std::move(data), index);
}

void SyncMPPTunnelSetWriter::writeToTunnel(tipb::SelectResponse & response, size_t index)
{
    mpp_tunnel_set->write(response, index);
}

void AsyncMPPTunnelSetWriter::writeToTunnel(TrackedMppDataPacketPtr && data, size_t index)
{
    mpp_tunnel_set->nonBlockingWrite(std::move(data), index);
}

void AsyncMPPTunnelSetWriter::writeToTunnel(tipb::SelectResponse & response, size_t index)
{
    mpp_tunnel_set->nonBlockingWrite(response, index);
}
} // namespace DB
