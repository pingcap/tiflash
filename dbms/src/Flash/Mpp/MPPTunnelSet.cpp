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
#include <Common/FailPoint.h>
#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/MPPTunnelSetHelper.h>
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
    RUNTIME_CHECK(size < max_packet_size, fmt::format("Packet is too large to send, size : {}", size));
}

TrackedMppDataPacketPtr serializePacket(const tipb::SelectResponse & response)
{
    auto tracked_packet = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
    tracked_packet->serializeByResponse(response);
    checkPacketSize(tracked_packet->getPacket().ByteSizeLong());
    return tracked_packet;
}
} // namespace

template <typename Tunnel>
MPPTunnelSetBase<Tunnel>::MPPTunnelSetBase(DAGContext & dag_context, const String & req_id)
    : log(Logger::get(req_id))
    , result_field_types(dag_context.result_field_types)
{}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::sendExecutionSummary(const tipb::SelectResponse & response)
{
    RUNTIME_CHECK(!tunnels.empty());
    // for execution summary, only need to send to one tunnel.
    tunnels[0]->write(serializePacket(response));
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::write(tipb::SelectResponse & response)
{
    // for root mpp task, only one tunnel will connect to tidb/tispark.
    RUNTIME_CHECK(1 == tunnels.size());
    tunnels.back()->write(serializePacket(response));
}

static inline void updatePartitionWriterMetrics(size_t packet_bytes, bool is_local)
{
    // statistic
    GET_METRIC(tiflash_exchange_data_bytes, type_hash_original).Increment(packet_bytes);
    // compression method is always NONE
    if (is_local)
        GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_compression_local).Increment(packet_bytes);
    else
        GET_METRIC(tiflash_exchange_data_bytes, type_hash_none_compression_remote).Increment(packet_bytes);
}

static inline void updatePartitionWriterMetrics(CompressionMethod method, size_t original_size, size_t sz, bool is_local)
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

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::broadcastOrPassThroughWrite(Blocks & blocks)
{
    RUNTIME_CHECK(!tunnels.empty());
    auto && tracked_packet = MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types);
    if (!tracked_packet)
        return;

    auto packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    checkPacketSize(packet_bytes);
    // TODO avoid copy packet for broadcast.
    for (size_t i = 1; i < tunnels.size(); ++i)
        tunnels[i]->write(tracked_packet->copy());
    tunnels[0]->write(std::move(tracked_packet));
    {
        // statistic
        size_t data_bytes = 0;
        size_t local_data_bytes = 0;
        {
            auto tunnel_cnt = getPartitionNum();
            size_t local_tunnel_cnt = 0;
            for (size_t i = 0; i < tunnel_cnt; ++i)
            {
                local_tunnel_cnt += isLocal(i);
            }
            data_bytes = packet_bytes * tunnel_cnt;
            local_data_bytes = packet_bytes * local_tunnel_cnt;
        }
        GET_METRIC(tiflash_exchange_data_bytes, type_broadcast_passthrough_original).Increment(data_bytes);
        GET_METRIC(tiflash_exchange_data_bytes, type_broadcast_passthrough_none_compression_local).Increment(local_data_bytes);
        GET_METRIC(tiflash_exchange_data_bytes, type_broadcast_passthrough_none_compression_remote).Increment(data_bytes - local_data_bytes);
    }
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::partitionWrite(Blocks & blocks, int16_t partition_id)
{
    auto && tracked_packet = MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types);
    if (!tracked_packet)
        return;
    auto packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    checkPacketSize(packet_bytes);
    tunnels[partition_id]->write(std::move(tracked_packet));
    updatePartitionWriterMetrics(packet_bytes, isLocal(partition_id));
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::partitionWrite(
    const Block & header,
    std::vector<MutableColumns> && part_columns,
    int16_t partition_id,
    MPPDataPacketVersion version,
    CompressionMethod compression_method)
{
    assert(version > MPPDataPacketV0);

    bool is_local = isLocal(partition_id);
    compression_method = is_local ? CompressionMethod::NONE : compression_method;

    size_t original_size = 0;
    auto tracked_packet = MPPTunnelSetHelper::ToPacket(header, std::move(part_columns), version, compression_method, original_size);
    if (!tracked_packet)
        return;

    auto packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    checkPacketSize(packet_bytes);
    tunnels[partition_id]->write(std::move(tracked_packet));
    updatePartitionWriterMetrics(compression_method, original_size, packet_bytes, is_local);
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::fineGrainedShuffleWrite(
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

    bool is_local = isLocal(partition_id);
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
    tunnels[partition_id]->write(std::move(tracked_packet));
    updatePartitionWriterMetrics(compression_method, original_size, packet_bytes, is_local);
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::fineGrainedShuffleWrite(
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
    tunnels[partition_id]->write(std::move(tracked_packet));
    updatePartitionWriterMetrics(packet_bytes, isLocal(partition_id));
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::registerTunnel(const MPPTaskId & receiver_task_id, const TunnelPtr & tunnel)
{
    if (receiver_task_id_to_index_map.find(receiver_task_id) != receiver_task_id_to_index_map.end())
        throw Exception(fmt::format("the tunnel {} has been registered", tunnel->id()));

    receiver_task_id_to_index_map[receiver_task_id] = tunnels.size();
    tunnels.push_back(tunnel);
    if (!tunnel->isLocal() && !tunnel->isAsync())
    {
        ++external_thread_cnt;
    }
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::close(const String & reason, bool wait_sender_finish)
{
    for (auto & tunnel : tunnels)
        tunnel->close(reason, wait_sender_finish);
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::finishWrite()
{
    for (auto & tunnel : tunnels)
    {
        tunnel->writeDone();
    }
}

template <typename Tunnel>
typename MPPTunnelSetBase<Tunnel>::TunnelPtr MPPTunnelSetBase<Tunnel>::getTunnelByReceiverTaskId(const MPPTaskId & id)
{
    auto it = receiver_task_id_to_index_map.find(id);
    if (it == receiver_task_id_to_index_map.end())
    {
        return nullptr;
    }
    return tunnels[it->second];
}

template <typename Tunnel>
bool MPPTunnelSetBase<Tunnel>::isLocal(size_t index) const
{
    assert(getPartitionNum() > index);
    return getTunnels()[index]->isLocal();
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class MPPTunnelSetBase<MPPTunnel>;

} // namespace DB
