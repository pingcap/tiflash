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


#define UPDATE_EXCHANGE_MATRIC_IMPL(type, compress, value, ...)                                                        \
    do                                                                                                                 \
    {                                                                                                                  \
        GET_METRIC(tiflash_exchange_data_bytes, type_##type##_##compress##_compression##__VA_ARGS__).Increment(value); \
    } while (false)
#define UPDATE_EXCHANGE_MATRIC_NONE_COMPRESS_LOCAL(type, value) UPDATE_EXCHANGE_MATRIC_IMPL(type, none, value, _local)
#define UPDATE_EXCHANGE_MATRIC_NONE_COMPRESS_REMOTE(type, value) UPDATE_EXCHANGE_MATRIC_IMPL(type, none, value, _remote)
#define UPDATE_EXCHANGE_MATRIC_NONE_COMPRESS(type, is_local, value)     \
    do                                                                  \
    {                                                                   \
        if (is_local)                                                   \
        {                                                               \
            UPDATE_EXCHANGE_MATRIC_NONE_COMPRESS_LOCAL(type, (value));  \
        }                                                               \
        else                                                            \
        {                                                               \
            UPDATE_EXCHANGE_MATRIC_NONE_COMPRESS_REMOTE(type, (value)); \
        }                                                               \
    } while (false)
#define UPDATE_EXCHANGE_MATRIC_LZ4_COMPRESS(type, value) UPDATE_EXCHANGE_MATRIC_IMPL(type, lz4, value)
#define UPDATE_EXCHANGE_MATRIC_ZSTD_COMPRESS(type, value) UPDATE_EXCHANGE_MATRIC_IMPL(type, zstd, value)
#define UPDATE_EXCHANGE_MATRIC_ORIGINAL(type, value)                                      \
    do                                                                                    \
    {                                                                                     \
        GET_METRIC(tiflash_exchange_data_bytes, type_##type##_original).Increment(value); \
    } while (false)
#define UPDATE_EXCHANGE_MATRIC(type, compress_method, original_size, compressed_size, is_local) \
    do                                                                                          \
    {                                                                                           \
        UPDATE_EXCHANGE_MATRIC_ORIGINAL(type, original_size);                                   \
        switch (compress_method)                                                                \
        {                                                                                       \
        case CompressionMethod::NONE:                                                           \
        {                                                                                       \
            UPDATE_EXCHANGE_MATRIC_NONE_COMPRESS(type, is_local, compressed_size);              \
            break;                                                                              \
        }                                                                                       \
        case CompressionMethod::LZ4:                                                            \
        {                                                                                       \
            UPDATE_EXCHANGE_MATRIC_LZ4_COMPRESS(type, compressed_size);                         \
            break;                                                                              \
        }                                                                                       \
        case CompressionMethod::ZSTD:                                                           \
        {                                                                                       \
            UPDATE_EXCHANGE_MATRIC_ZSTD_COMPRESS(type, compressed_size);                        \
            break;                                                                              \
        }                                                                                       \
        default:                                                                                \
            break;                                                                              \
        }                                                                                       \
    } while (false)

static inline void updatePartitionWriterMetrics(CompressionMethod method, size_t original_size, size_t compressed_size, bool is_local)
{
    UPDATE_EXCHANGE_MATRIC(hash, method, original_size, compressed_size, is_local);
}

template <typename Tunnel>
static inline bool IsLocalTunnel(const std::shared_ptr<Tunnel> & tunnel)
{
    return tunnel->isLocal();
}

template <typename Tunnel>
void BroadcastOrPassThroughWriteImpl(
    std::vector<std::shared_ptr<Tunnel>> & tunnels,
    size_t local_tunnel_cnt, // can be 0 for PassThrough writer
    size_t original_data_packet_size,
    bool is_broadcast,
    TrackedMppDataPacketPtr && tracked_packet,
    TrackedMppDataPacketPtr && compressed_tracked_packet,
    CompressionMethod compression_method)
{
    const size_t remote_tunnel_cnt = tunnels.size() - local_tunnel_cnt;
    auto tracked_packet_bytes = tracked_packet ? tracked_packet->getPacket().ByteSizeLong() : 0;
    auto compressed_tracked_packet_bytes = compressed_tracked_packet ? compressed_tracked_packet->getPacket().ByteSizeLong() : 0;
    checkPacketSize(tracked_packet_bytes);
    checkPacketSize(compressed_tracked_packet_bytes);

    // TODO avoid copy packet for broadcast.

    if (!compressed_tracked_packet)
    {
        original_data_packet_size = tracked_packet_bytes;
        compressed_tracked_packet_bytes = tracked_packet_bytes;

        for (size_t i = 1; i < tunnels.size(); ++i)
            tunnels[i]->write(tracked_packet->copy());
        tunnels[0]->write(std::move(tracked_packet));
    }
    else
    {
        for (size_t i = 0, local_cnt = 0, remote_cnt = 0; i < tunnels.size(); ++i)
        {
            if (IsLocalTunnel(tunnels[i]))
            {
                local_cnt++;
                if (local_cnt == local_tunnel_cnt)
                    tunnels[i]->write(std::move(tracked_packet));
                else
                    tunnels[i]->write(tracked_packet->copy());
            }
            else
            {
                remote_cnt++;
                if (remote_cnt == remote_tunnel_cnt)
                    tunnels[i]->write(std::move(compressed_tracked_packet));
                else
                    tunnels[i]->write(compressed_tracked_packet->copy());
            }
        }
    }

    if (is_broadcast)
    {
        UPDATE_EXCHANGE_MATRIC(broadcast, CompressionMethod::NONE, local_tunnel_cnt * original_data_packet_size, local_tunnel_cnt * original_data_packet_size, true);
        UPDATE_EXCHANGE_MATRIC(broadcast, compression_method, remote_tunnel_cnt * original_data_packet_size, remote_tunnel_cnt * compressed_tracked_packet_bytes, false);
    }
    else
    {
        UPDATE_EXCHANGE_MATRIC(passthrough, CompressionMethod::NONE, local_tunnel_cnt * original_data_packet_size, local_tunnel_cnt * original_data_packet_size, true);
        UPDATE_EXCHANGE_MATRIC(passthrough, compression_method, remote_tunnel_cnt * original_data_packet_size, remote_tunnel_cnt * compressed_tracked_packet_bytes, false);
    }
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::broadcastOrPassThroughWrite(Blocks & blocks, bool is_broadcast)
{
    RUNTIME_CHECK(!tunnels.empty());
    auto && tracked_packet = MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types);
    if (!tracked_packet)
        return;
    const size_t local_tunnel_cnt = std::accumulate(tunnels.begin(), tunnels.end(), 0, [](auto res, auto && tunnel) {
        return res + tunnel->isLocal();
    });

    BroadcastOrPassThroughWriteImpl(
        tunnels,
        local_tunnel_cnt,
        0,
        is_broadcast,
        std::move(tracked_packet),
        nullptr,
        CompressionMethod::NONE);
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::broadcastOrPassThroughWrite(Blocks & blocks, MPPDataPacketVersion version, CompressionMethod compression_method, bool is_broadcast)
{
    if (MPPDataPacketV0 == version)
        return broadcastOrPassThroughWrite(blocks, is_broadcast);

    RUNTIME_CHECK(!tunnels.empty());
    const size_t local_tunnel_cnt = std::accumulate(tunnels.begin(), tunnels.end(), 0, [](auto res, auto && tunnel) {
        return res + tunnel->isLocal();
    });

    size_t original_size = 0;
    // encode by method NONE
    auto && local_tunnel_tracked_packet = MPPTunnelSetHelper::ToPacket(std::move(blocks), version, CompressionMethod::NONE, original_size);
    if (!local_tunnel_tracked_packet)
        return;
    // Only if all tunnels are local mode or compression method is NONE
    if (local_tunnel_cnt == getPartitionNum() || compression_method == CompressionMethod::NONE)
    {
        return BroadcastOrPassThroughWriteImpl(
            tunnels,
            local_tunnel_cnt,
            0,
            is_broadcast,
            std::move(local_tunnel_tracked_packet),
            nullptr,
            CompressionMethod::NONE);
    }
    assert(local_tunnel_tracked_packet->getPacket().chunks_size() == 1);

    const auto & chunk = local_tunnel_tracked_packet->getPacket().chunks(0);
    assert(static_cast<CompressionMethodByte>(chunk[0]) == CompressionMethodByte::NONE);

    // re-encode by specified compression method
    auto && remote_tunnel_tracked_packet = std::make_shared<TrackedMppDataPacket>(version);
    {
        auto && compressed_buffer = CHBlockChunkCodecV1::encode({&chunk[1], chunk.size() - 1}, compression_method);
        assert(!compressed_buffer.empty());

        remote_tunnel_tracked_packet->addChunk(std::move(compressed_buffer));
    }

    return BroadcastOrPassThroughWriteImpl(
        tunnels,
        local_tunnel_cnt,
        original_size,
        is_broadcast,
        std::move(local_tunnel_tracked_packet),
        std::move(remote_tunnel_tracked_packet),
        compression_method);
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
    updatePartitionWriterMetrics(CompressionMethod::NONE, packet_bytes, packet_bytes, isLocal(partition_id));
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
    updatePartitionWriterMetrics(CompressionMethod::NONE, packet_bytes, packet_bytes, isLocal(partition_id));
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
    return IsLocalTunnel(getTunnels()[index]);
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class MPPTunnelSetBase<MPPTunnel>;

} // namespace DB
