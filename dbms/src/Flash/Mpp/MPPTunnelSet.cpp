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
    auto tracked_packet = std::make_shared<TrackedMppDataPacket>();
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

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::broadcastOrPassThroughWrite(Blocks & blocks)
{
    RUNTIME_CHECK(!tunnels.empty());
    auto tracked_packet = MPPTunnelSetHelper::toPacket(blocks, result_field_types);
    checkPacketSize(tracked_packet->getPacket().ByteSizeLong());

    // TODO avoid copy packet for broadcast.
    for (size_t i = 1; i < tunnels.size(); ++i)
        tunnels[i]->write(tracked_packet->copy());
    tunnels[0]->write(std::move(tracked_packet));
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::partitionWrite(Blocks & blocks, int16_t partition_id)
{
    auto tracked_packet = MPPTunnelSetHelper::toPacket(blocks, result_field_types);
    if (likely(tracked_packet->getPacket().chunks_size() > 0))
    {
        checkPacketSize(tracked_packet->getPacket().ByteSizeLong());
        tunnels[partition_id]->write(std::move(tracked_packet));
    }
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
    auto tracked_packet = MPPTunnelSetHelper::toFineGrainedPacket(
        header,
        scattered,
        bucket_idx,
        fine_grained_shuffle_stream_count,
        num_columns,
        result_field_types);
    if (likely(tracked_packet->getPacket().chunks_size() > 0))
    {
        checkPacketSize(tracked_packet->getPacket().ByteSizeLong());
        tunnels[partition_id]->write(std::move(tracked_packet));
    }
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

/// Explicit template instantiations - to avoid code bloat in headers.
template class MPPTunnelSetBase<MPPTunnel>;

} // namespace DB
