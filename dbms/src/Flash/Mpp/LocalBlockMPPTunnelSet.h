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

#pragma once

#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <tipb/select.pb.h>

#include <boost/noncopyable.hpp>

namespace DB
{
template <typename Tunnel>
class LocalBlockMPPTunnelSetBase : private boost::noncopyable
{
public:
    using TunnelPtr = std::shared_ptr<Tunnel>;
    explicit LocalBlockMPPTunnelSetBase(const String & req_id)
        : log(Logger::get(req_id))
    {}

    /// for both broadcast writing and partition writing, only
    /// return meaningful execution summary for the first tunnel,
    /// because in TiDB, it does not know enough information
    /// about the execution details for the mpp query, it just
    /// add up all the execution summaries for the same executor,
    /// so if return execution summary for all the tunnels, the
    /// information in TiDB will be amplified, which may make
    /// user confused.
    // this is a root mpp writing.
    void write(tipb::SelectResponse & response);
    // this is a broadcast or pass through writing.
    void broadcastOrPassThroughWrite(const TrackedMppDataPacketPtr & packet);
    // this is a partition writing.
    void partitionWrite(Block && block, int16_t partition_id)
    {
    }
    // this is a execution summary writing.
    void sendExecutionSummary(tipb::SelectResponse & response);

    void close(const String & reason, bool wait_sender_finish);
    void finishWrite();
    void registerTunnel(const MPPTaskId & receiver_task_id, const TunnelPtr & tunnel)
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

    TunnelPtr getTunnelByReceiverTaskId(const MPPTaskId & id);

    uint16_t getPartitionNum() const { return tunnels.size(); }

    int getRemoteTunnelCnt()
    {
        return remote_tunnel_cnt;
    }

    const std::vector<TunnelPtr> & getTunnels() const { return tunnels; }

private:
    std::vector<TunnelPtr> tunnels;
    std::unordered_map<MPPTaskId, size_t> receiver_task_id_to_index_map;
    const LoggerPtr log;

    int remote_tunnel_cnt = 0;
};

class LocalBlockMPPTunnelSet : public LocalBlockMPPTunnelSetBase<MPPTunnel>
{
public:
    using Base = LocalBlockMPPTunnelSetBase<MPPTunnel>;
    using Base::Base;
};

using LocalBlockMPPTunnelSetPtr = std::shared_ptr<LocalBlockMPPTunnelSet>;

} // namespace DB
