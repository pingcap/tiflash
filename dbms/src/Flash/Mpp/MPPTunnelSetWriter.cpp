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
} // namespace


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

template <bool is_broadcast, typename Tunnel>
static void broadcastOrPassThroughWriteImpl(
    const std::vector<std::shared_ptr<Tunnel>> & tunnels,
    size_t local_tunnel_cnt, // can be 0 for PassThrough writer
    TrackedMppDataPacketPtr && ori_tracked_packet, // can be NULL if there is no local tunnel
    size_t ori_tracked_packet_bytes,
    TrackedMppDataPacketPtr && remote_tracked_packet,
    CompressionMethod compression_method)
{
    const size_t remote_tunnel_cnt = tunnels.size() - local_tunnel_cnt;
    auto remote_tracked_packet_bytes = remote_tracked_packet ? remote_tracked_packet->getPacket().ByteSizeLong() : 0;
    checkPacketSize(ori_tracked_packet_bytes);
    checkPacketSize(remote_tracked_packet_bytes);

    // TODO avoid copy packet for broadcast.

    if (!remote_tracked_packet)
    {
        assert(ori_tracked_packet);

        remote_tracked_packet_bytes = ori_tracked_packet_bytes;

        for (size_t i = 1; i < tunnels.size(); ++i)
            tunnels[i]->write(ori_tracked_packet->copy());
        tunnels[0]->write(std::move(ori_tracked_packet));
    }
    else
    {
        for (size_t i = 0, local_cnt = 0, remote_cnt = 0; i < tunnels.size(); ++i)
        {
            if (isLocalTunnel(tunnels[i]))
            {
                local_cnt++;
                if (local_cnt == local_tunnel_cnt)
                    tunnels[i]->write(std::move(ori_tracked_packet));
                else
                    tunnels[i]->write(ori_tracked_packet->copy()); // NOLINT
            }
            else
            {
                remote_cnt++;
                if (remote_cnt == remote_tunnel_cnt)
                    tunnels[i]->write(std::move(remote_tracked_packet));
                else
                    tunnels[i]->write(remote_tracked_packet->copy()); // NOLINT
            }
        }
    }

    if constexpr (is_broadcast)
    {
        UPDATE_EXCHANGE_MATRIC(broadcast, CompressionMethod::NONE, local_tunnel_cnt * ori_tracked_packet_bytes, local_tunnel_cnt * ori_tracked_packet_bytes, true);
        UPDATE_EXCHANGE_MATRIC(broadcast, compression_method, remote_tunnel_cnt * ori_tracked_packet_bytes, remote_tunnel_cnt * remote_tracked_packet_bytes, false);
    }
    else
    {
        UPDATE_EXCHANGE_MATRIC(passthrough, CompressionMethod::NONE, local_tunnel_cnt * ori_tracked_packet_bytes, local_tunnel_cnt * ori_tracked_packet_bytes, true);
        UPDATE_EXCHANGE_MATRIC(passthrough, compression_method, remote_tunnel_cnt * ori_tracked_packet_bytes, remote_tunnel_cnt * remote_tracked_packet_bytes, false);
    }
}

template <bool is_broadcast, typename Tunnel>
static void broadcastOrPassThroughWriteV0(
    const std::vector<std::shared_ptr<Tunnel>> & tunnels,
    size_t local_tunnel_cnt,
    Blocks & blocks,
    const std::vector<tipb::FieldType> & result_field_types)
{
    RUNTIME_CHECK(!tunnels.empty());
    auto && tracked_packet = MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types);
    if (!tracked_packet)
        return;
    size_t tracked_packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    broadcastOrPassThroughWriteImpl<is_broadcast>(
        tunnels,
        local_tunnel_cnt,
        std::move(tracked_packet),
        tracked_packet_bytes,
        nullptr,
        CompressionMethod::NONE);
}

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

void MPPTunnelSetWriterBase::broadcastWrite(Blocks & blocks)
{
    return broadcastOrPassThroughWriteV0<true>(mpp_tunnel_set->getTunnels(), mpp_tunnel_set->getLocalTunnelCnt(), blocks, result_field_types);
}

void MPPTunnelSetWriterBase::passThroughWrite(Blocks & blocks)
{
    return broadcastOrPassThroughWriteV0<false>(mpp_tunnel_set->getTunnels(), mpp_tunnel_set->getLocalTunnelCnt(), blocks, result_field_types);
}

template <bool is_broadcast, typename Tunnel>
static void broadcastOrPassThroughWrite(
    const std::vector<std::shared_ptr<Tunnel>> & tunnels,
    size_t local_tunnel_cnt,
    Blocks & blocks,
    MPPDataPacketVersion version,
    CompressionMethod compression_method,
    const std::vector<tipb::FieldType> & result_field_types)
{
    if (MPPDataPacketV0 == version)
        return broadcastOrPassThroughWriteV0<is_broadcast>(tunnels, local_tunnel_cnt, blocks, result_field_types);

    RUNTIME_CHECK(!tunnels.empty());

    size_t original_size = 0;
    // encode by method NONE
    auto && ori_tracked_packet = MPPTunnelSetHelper::ToPacket(std::move(blocks), version, CompressionMethod::NONE, original_size);
    if (!ori_tracked_packet)
        return;
    size_t tracked_packet_bytes = ori_tracked_packet->getPacket().ByteSizeLong();

    // Only if all tunnels are local mode or compression method is NONE
    if (local_tunnel_cnt == tunnels.size() || compression_method == CompressionMethod::NONE)
    {
        return broadcastOrPassThroughWriteImpl<is_broadcast>(
            tunnels,
            local_tunnel_cnt,
            std::move(ori_tracked_packet),
            tracked_packet_bytes,
            nullptr,
            CompressionMethod::NONE);
    }

    auto remote_tunnel_tracked_packet = MPPTunnelSetHelper::ToCompressedPacket(ori_tracked_packet, version, compression_method);

    if (0 == local_tunnel_cnt)
    {
        // if no need local tunnel, just release early to reduce memory usage
        ori_tracked_packet = nullptr;
    }

    return broadcastOrPassThroughWriteImpl<is_broadcast>(
        tunnels,
        local_tunnel_cnt,
        std::move(ori_tracked_packet),
        tracked_packet_bytes,
        std::move(remote_tunnel_tracked_packet),
        compression_method);
}

void MPPTunnelSetWriterBase::broadcastWrite(Blocks & blocks, MPPDataPacketVersion version, CompressionMethod compression_method)
{
    return broadcastOrPassThroughWrite<true>(mpp_tunnel_set->getTunnels(), mpp_tunnel_set->getLocalTunnelCnt(), blocks, version, compression_method, result_field_types);
}

void MPPTunnelSetWriterBase::passThroughWrite(Blocks & blocks, MPPDataPacketVersion version, CompressionMethod compression_method)
{
    return broadcastOrPassThroughWrite<false>(mpp_tunnel_set->getTunnels(), mpp_tunnel_set->getLocalTunnelCnt(), blocks, version, compression_method, result_field_types);
}

void MPPTunnelSetWriterBase::partitionWrite(Blocks & blocks, int16_t partition_id)
{
    auto && tracked_packet = MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types);
    if (!tracked_packet)
        return;
    auto packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    checkPacketSize(packet_bytes);
    writeToTunnel(std::move(tracked_packet), partition_id);
    updatePartitionWriterMetrics(CompressionMethod::NONE, packet_bytes, packet_bytes, isLocalTunnel(mpp_tunnel_set->getTunnels()[partition_id]));
}

void MPPTunnelSetWriterBase::partitionWrite(
    const Block & header,
    std::vector<MutableColumns> && part_columns,
    int16_t partition_id,
    MPPDataPacketVersion version,
    CompressionMethod compression_method)
{
    assert(version > MPPDataPacketV0);

    bool is_local = isLocalTunnel(mpp_tunnel_set->getTunnels()[partition_id]);
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

    bool is_local = isLocalTunnel(mpp_tunnel_set->getTunnels()[partition_id]);
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
    updatePartitionWriterMetrics(CompressionMethod::NONE, packet_bytes, packet_bytes, isLocalTunnel(mpp_tunnel_set->getTunnels()[partition_id]));
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
