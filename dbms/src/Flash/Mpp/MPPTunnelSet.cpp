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
#include <Flash/Mpp/MPPTunnelSet.h>
#include <fmt/core.h>

namespace DB
{
namespace
{
inline mpp::MPPDataPacket serializeToPacket(const tipb::SelectResponse & response)
{
    mpp::MPPDataPacket packet;
    if (!response.SerializeToString(packet.mutable_data()))
        throw Exception(fmt::format("Fail to serialize response, response size: {}", response.ByteSizeLong()));
    return packet;
}

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
    auto packet = serializeToPacket(response);
    tunnels[0]->write(packet);

    if (tunnels.size() > 1)
    {
        /// only the last response has execution_summaries
        if (response.execution_summaries_size() > 0)
        {
            clearExecutionSummaries(response);
            packet = serializeToPacket(response);
        }
        for (size_t i = 1; i < tunnels.size(); ++i)
            tunnels[i]->write(packet);
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
    if (partition_id != 0 && response.execution_summaries_size() > 0)
        clearExecutionSummaries(response);

    tunnels[partition_id]->write(serializeToPacket(response));
}

template <typename Tunnel>
void MPPTunnelSetBase<Tunnel>::write(mpp::MPPDataPacket & packet, int16_t partition_id)
{
    checkPacketSize(packet.ByteSizeLong());
    if (partition_id != 0 && !packet.data().empty())
        packet.mutable_data()->clear();

    tunnels[partition_id]->write(packet);
}

/// Explicit template instantiations - to avoid code bloat in headers.
template class MPPTunnelSetBase<MPPTunnel>;

} // namespace DB
