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
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Mpp/MPPTunnelSet.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <Flash/Mpp/Utils.h>
#include <fmt/core.h>

namespace DB
{
namespace
{
TrackedMppDataPacketPtr serializePacket(const tipb::SelectResponse & response)
{
    auto tracked_packet = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
    tracked_packet->serializeByResponse(response);
    return tracked_packet;
}
} // namespace

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::sendExecutionSummary(const tipb::SelectResponse & response)
{
    RUNTIME_CHECK(!tunnels.empty());
    // for execution summary, only need to send to one tunnel.
    tunnels[0]->write(serializePacket(response));
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::write(TrackedMppDataPacketPtr && data, size_t index)
{
    assert(index < tunnels.size());
    tunnels[index]->write(std::move(data));
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::forceWrite(TrackedMppDataPacketPtr && data, size_t index)
{
    assert(index < tunnels.size());
    tunnels[index]->forceWrite(std::move(data));
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::write(tipb::SelectResponse & response, size_t index)
{
    assert(index < tunnels.size());
    tunnels[index]->write(serializePacket(response));
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::forceWrite(tipb::SelectResponse & response, size_t index)
{
    assert(index < tunnels.size());
    tunnels[index]->forceWrite(serializePacket(response));
}

template <typename Tunnel>
WaitResult MPPTunnelSetBase<Tunnel>::waitForWritable() const
{
    for (const auto & tunnel : tunnels)
    {
        if (auto res = tunnel->waitForWritable(); res != WaitResult::Ready)
            return res;
    }
    return WaitResult::Ready;
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::notifyNextPipelineWriter() const
{
    for (const auto & tunnel : tunnels)
    {
        tunnel->notifyNextPipelineWriter();
    }
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::registerTunnel(const MPPTaskId & receiver_task_id, const TunnelPtr & tunnel)
{
    RUNTIME_CHECK_MSG(
        receiver_task_id_to_index_map.find(receiver_task_id) == receiver_task_id_to_index_map.end(),
        "the tunnel {} has been registered",
        tunnel->id());

    receiver_task_id_to_index_map[receiver_task_id] = tunnels.size();
    tunnels.push_back(tunnel);
    if (!tunnel->isLocal() && !tunnel->isAsync())
    {
        ++external_thread_cnt;
    }
    if (tunnel->isLocal())
    {
        ++local_tunnel_cnt;
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
