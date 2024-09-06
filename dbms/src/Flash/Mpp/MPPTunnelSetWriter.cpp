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
#define UPDATE_EXCHANGE_MATRIC(type, compress_method, original_size, actual_size, is_local) \
    do                                                                                      \
    {                                                                                       \
        UPDATE_EXCHANGE_MATRIC_ORIGINAL(type, original_size);                               \
        switch (compress_method)                                                            \
        {                                                                                   \
        case CompressionMethod::NONE:                                                       \
        {                                                                                   \
            UPDATE_EXCHANGE_MATRIC_NONE_COMPRESS(type, is_local, actual_size);              \
            break;                                                                          \
        }                                                                                   \
        case CompressionMethod::LZ4:                                                        \
        {                                                                                   \
            UPDATE_EXCHANGE_MATRIC_LZ4_COMPRESS(type, actual_size);                         \
            break;                                                                          \
        }                                                                                   \
        case CompressionMethod::ZSTD:                                                       \
        {                                                                                   \
            UPDATE_EXCHANGE_MATRIC_ZSTD_COMPRESS(type, actual_size);                        \
            break;                                                                          \
        }                                                                                   \
        default:                                                                            \
            break;                                                                          \
        }                                                                                   \
    } while (false)

static inline void updatePartitionWriterMetrics(
    CompressionMethod method,
    size_t original_size,
    size_t actual_size,
    bool is_local)
{
    UPDATE_EXCHANGE_MATRIC(hash, method, original_size, actual_size, is_local);
}

template <bool is_broadcast, typename FuncIsLocalTunnel, typename FuncWriteToTunnel>
static void broadcastOrPassThroughWriteImpl(
    const size_t tunnel_cnt,
    const size_t local_tunnel_cnt, // can be 0 for PassThrough writer
    const size_t ori_packet_bytes, // original data packet size
    TrackedMppDataPacketPtr && local_tracked_packet, // can be NULL if there is no local tunnel
    TrackedMppDataPacketPtr && remote_tracked_packet, // can be NULL if all tunnels are local mode
    const CompressionMethod compression_method,
    FuncIsLocalTunnel && isLocalTunnel,
    FuncWriteToTunnel && writeToTunnel)
{
    assert(tunnel_cnt > 0);
    assert(ori_packet_bytes > 0);

    const size_t remote_tunnel_cnt = tunnel_cnt - local_tunnel_cnt;
    auto remote_tracked_packet_bytes = remote_tracked_packet ? remote_tracked_packet->getPacket().ByteSizeLong() : 0;

    if (!local_tracked_packet)
    {
        assert(local_tunnel_cnt == 0);
        assert(remote_tracked_packet);
    }
    else
    {
        checkPacketSize(ori_packet_bytes);
    }

    if (!remote_tracked_packet)
    {
        assert(local_tracked_packet);
        assert(local_tunnel_cnt == tunnel_cnt);
    }
    else
    {
        checkPacketSize(remote_tracked_packet_bytes);
    }

    // TODO avoid copy packet for broadcast.

    if (local_tracked_packet == remote_tracked_packet)
    {
        // `TrackedMppDataPacket` in `TrackedMppDataPacketPtr` is mutable.
        // If `local_tracked_packet` and `remote_tracked_packet` share same object, there is also another optional way to copy `remote_tracked_packet` early.
        // Use this fast path to reduce maximum memory usage
        auto tracked_packet = std::move(local_tracked_packet);
        remote_tracked_packet = nullptr;

        for (size_t i = 1; i < tunnel_cnt; ++i)
        {
            writeToTunnel(tracked_packet->copy(), i);
        }
        writeToTunnel(std::move(tracked_packet), 0);
    }
    else
    {
        for (size_t i = 0, local_cnt = 0, remote_cnt = 0; i < tunnel_cnt; ++i)
        {
            if (isLocalTunnel(i))
            {
                local_cnt++;
                if (local_cnt == local_tunnel_cnt)
                    writeToTunnel(std::move(local_tracked_packet), i);
                else
                    writeToTunnel(local_tracked_packet->copy(), i); // NOLINT
            }
            else
            {
                remote_cnt++;
                if (remote_cnt == remote_tunnel_cnt)
                    writeToTunnel(std::move(remote_tracked_packet), i);
                else
                    writeToTunnel(remote_tracked_packet->copy(), i); // NOLINT
            }
        }
    }

    if constexpr (is_broadcast)
    {
        UPDATE_EXCHANGE_MATRIC(
            broadcast,
            CompressionMethod::NONE,
            local_tunnel_cnt * ori_packet_bytes,
            local_tunnel_cnt * ori_packet_bytes,
            true);
        UPDATE_EXCHANGE_MATRIC(
            broadcast,
            compression_method,
            remote_tunnel_cnt * ori_packet_bytes,
            remote_tunnel_cnt * remote_tracked_packet_bytes,
            false);
    }
    else
    {
        UPDATE_EXCHANGE_MATRIC(
            passthrough,
            CompressionMethod::NONE,
            local_tunnel_cnt * ori_packet_bytes,
            local_tunnel_cnt * ori_packet_bytes,
            true);
        UPDATE_EXCHANGE_MATRIC(
            passthrough,
            compression_method,
            remote_tunnel_cnt * ori_packet_bytes,
            remote_tunnel_cnt * remote_tracked_packet_bytes,
            false);
    }
}

template <bool is_broadcast, typename FuncIsLocalTunnel, typename FuncWriteToTunnel>
static void broadcastOrPassThroughWriteV0(
    const size_t tunnel_cnt,
    const size_t local_tunnel_cnt,
    Blocks & blocks,
    const std::vector<tipb::FieldType> & result_field_types,
    FuncIsLocalTunnel && isLocalTunnel,
    FuncWriteToTunnel && writeToTunnel)
{
    auto && ori_tracked_packet = MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types);
    if (!ori_tracked_packet)
        return;
    size_t tracked_packet_bytes = ori_tracked_packet->getPacket().ByteSizeLong();
    TrackedMppDataPacketPtr remote_tracked_packet = nullptr;
    if (local_tunnel_cnt != tunnel_cnt)
    {
        remote_tracked_packet = ori_tracked_packet;
    }
    if (0 == local_tunnel_cnt)
    {
        ori_tracked_packet = nullptr;
    }
    broadcastOrPassThroughWriteImpl<is_broadcast>(
        tunnel_cnt,
        local_tunnel_cnt,
        tracked_packet_bytes,
        std::move(ori_tracked_packet),
        std::move(remote_tracked_packet),
        CompressionMethod::NONE,
        std::forward<FuncIsLocalTunnel>(isLocalTunnel),
        std::forward<FuncWriteToTunnel>(writeToTunnel));
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
    return broadcastOrPassThroughWriteV0<true>(
        mpp_tunnel_set->getTunnels().size(),
        mpp_tunnel_set->getLocalTunnelCnt(),
        blocks,
        result_field_types,
        [&](size_t i) { return mpp_tunnel_set->isLocal(i); },
        [&](TrackedMppDataPacketPtr && data, size_t index) { return writeToTunnel(std::move(data), index); });
}

void MPPTunnelSetWriterBase::passThroughWrite(Blocks & blocks)
{
    return broadcastOrPassThroughWriteV0<false>(
        mpp_tunnel_set->getTunnels().size(),
        mpp_tunnel_set->getLocalTunnelCnt(),
        blocks,
        result_field_types,
        [&](size_t i) { return mpp_tunnel_set->isLocal(i); },
        [&](TrackedMppDataPacketPtr && data, size_t index) { return writeToTunnel(std::move(data), index); });
}

template <bool is_broadcast, typename FuncIsLocalTunnel, typename FuncWriteToTunnel>
static void broadcastOrPassThroughWrite(
    const size_t tunnel_cnt,
    const size_t local_tunnel_cnt,
    Blocks & blocks,
    MPPDataPacketVersion version,
    CompressionMethod compression_method,
    FuncIsLocalTunnel && isLocalTunnel,
    FuncWriteToTunnel && writeToTunnel)
{
    assert(version > MPPDataPacketV0);

    size_t original_size = 0;
    // encode by method NONE
    auto && ori_tracked_packet
        = MPPTunnelSetHelper::ToPacket(std::move(blocks), version, CompressionMethod::NONE, original_size);
    if (!ori_tracked_packet)
        return;

    size_t tracked_packet_bytes = ori_tracked_packet->getPacket().ByteSizeLong();

    TrackedMppDataPacketPtr remote_tunnel_tracked_packet = nullptr;

    if (local_tunnel_cnt != tunnel_cnt)
    {
        if (compression_method != CompressionMethod::NONE)
            remote_tunnel_tracked_packet
                = MPPTunnelSetHelper::ToCompressedPacket(ori_tracked_packet, version, compression_method);
        else
            remote_tunnel_tracked_packet = ori_tracked_packet;
    }
    else
    {
        // remote packet will be NULL if local_tunnel_cnt == tunnel_cnt
    }

    if (0 == local_tunnel_cnt)
    {
        // if no need local tunnel, just release early to reduce memory usage
        ori_tracked_packet = nullptr;
    }

    return broadcastOrPassThroughWriteImpl<is_broadcast>(
        tunnel_cnt,
        local_tunnel_cnt,
        tracked_packet_bytes,
        std::move(ori_tracked_packet),
        std::move(remote_tunnel_tracked_packet),
        compression_method,
        std::forward<FuncIsLocalTunnel>(isLocalTunnel),
        std::forward<FuncWriteToTunnel>(writeToTunnel));
}

void MPPTunnelSetWriterBase::broadcastWrite(
    Blocks & blocks,
    MPPDataPacketVersion version,
    CompressionMethod compression_method)
{
    if (MPPDataPacketV0 == version)
        return broadcastWrite(blocks);
    return broadcastOrPassThroughWrite<true>(
        mpp_tunnel_set->getTunnels().size(),
        mpp_tunnel_set->getLocalTunnelCnt(),
        blocks,
        version,
        compression_method,
        [&](size_t i) { return mpp_tunnel_set->isLocal(i); },
        [&](TrackedMppDataPacketPtr && data, size_t index) { return writeToTunnel(std::move(data), index); });
}

void MPPTunnelSetWriterBase::passThroughWrite(
    Blocks & blocks,
    MPPDataPacketVersion version,
    CompressionMethod compression_method)
{
    if (MPPDataPacketV0 == version)
        return passThroughWrite(blocks);
    return broadcastOrPassThroughWrite<false>(
        mpp_tunnel_set->getTunnels().size(),
        mpp_tunnel_set->getLocalTunnelCnt(),
        blocks,
        version,
        compression_method,
        [&](size_t i) { return mpp_tunnel_set->isLocal(i); },
        [&](TrackedMppDataPacketPtr && data, size_t index) { return writeToTunnel(std::move(data), index); });
}

void MPPTunnelSetWriterBase::partitionWrite(Blocks & blocks, int16_t partition_id)
{
    auto && tracked_packet = MPPTunnelSetHelper::ToPacketV0(blocks, result_field_types);
    assert(!tracked_packet);
    auto packet_bytes = tracked_packet->getPacket().ByteSizeLong();
    checkPacketSize(packet_bytes);
    writeToTunnel(std::move(tracked_packet), partition_id);
    updatePartitionWriterMetrics(
        CompressionMethod::NONE,
        packet_bytes,
        packet_bytes,
        mpp_tunnel_set->isLocal(partition_id));
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
    auto tracked_packet
        = MPPTunnelSetHelper::ToPacket(header, std::move(part_columns), version, compression_method, original_size);
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
        return fineGrainedShuffleWrite(
            header,
            scattered,
            bucket_idx,
            fine_grained_shuffle_stream_count,
            num_columns,
            partition_id);

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
    updatePartitionWriterMetrics(
        CompressionMethod::NONE,
        packet_bytes,
        packet_bytes,
        mpp_tunnel_set->isLocal(partition_id));
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
    mpp_tunnel_set->forceWrite(std::move(data), index);
}

void AsyncMPPTunnelSetWriter::writeToTunnel(tipb::SelectResponse & response, size_t index)
{
    mpp_tunnel_set->forceWrite(response, index);
}
} // namespace DB
