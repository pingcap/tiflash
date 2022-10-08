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
#include <Flash/Mpp/MPPTunnelSet.h>
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
    if (size >= max_packet_size)
        throw Exception(fmt::format("Packet is too large to send, size : {}", size));
}

} // namespace

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::clearExecutionSummaries(tipb::SelectResponse & response)
{
    /// can not use response.clear_execution_summaries() because
    /// TiDB assume all the executor should return execution summary
    for (int i = 0; i < response.execution_summaries_size(); i++)
    {
        auto * mutable_execution_summary = response.mutable_execution_summaries(i);
        mutable_execution_summary->set_num_produced_rows(0);
        mutable_execution_summary->set_num_iterations(0);
        mutable_execution_summary->set_concurrency(0);
    }
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::write(tipb::SelectResponse & response)
{
    TrackedMppDataPacket tracked_packet;
    tracked_packet.serializeByResponse(response);
    tunnels[0]->write(tracked_packet.getPacket());

    if (tunnels.size() > 1)
    {
        /// only the last response has execution_summaries
        if (response.execution_summaries_size() > 0)
        {
            clearExecutionSummaries(response);
            tracked_packet = TrackedMppDataPacket();
            tracked_packet.serializeByResponse(response);
        }
        for (size_t i = 1; i < tunnels.size(); ++i)
            tunnels[i]->write(tracked_packet.getPacket());
    }
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::write(mpp::MPPDataPacket & packet)
{
    checkPacketSize(packet.ByteSizeLong());
    tunnels[0]->write(packet);
    auto tunnels_size = tunnels.size();
    if (tunnels_size > 1)
    {
        if (!packet.data().empty())
        {
            packet.mutable_data()->clear();
        }
        for (size_t i = 1; i < tunnels_size; ++i)
        {
            tunnels[i]->write(packet);
        }
    }
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::write(tipb::SelectResponse & response, int16_t partition_id)
{
    TrackedMppDataPacket tracked_packet;
    if (partition_id != 0 && response.execution_summaries_size() > 0)
        clearExecutionSummaries(response);
    tracked_packet.serializeByResponse(response);
    tunnels[partition_id]->write(tracked_packet.getPacket());
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::write(mpp::MPPDataPacket & packet, int16_t partition_id)
{
    checkPacketSize(packet.ByteSizeLong());
    if (partition_id != 0 && !packet.data().empty())
        packet.mutable_data()->clear();

    tunnels[partition_id]->write(packet);
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::registerTunnel(const MPPTaskId & receiver_task_id, const TunnelPtr & tunnel)
{
    if (receiver_task_id_to_index_map.find(receiver_task_id) != receiver_task_id_to_index_map.end())
        throw Exception(fmt::format("the tunnel {} has been registered", tunnel->id()));

    receiver_task_id_to_index_map[receiver_task_id] = tunnels.size();
    tunnels.push_back(tunnel);
    if (!tunnel->isLocal())
    {
        remote_tunnel_cnt++;
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
