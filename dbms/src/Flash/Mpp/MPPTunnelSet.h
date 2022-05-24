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

#include <Flash/Mpp/MPPTunnel.h>
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
#endif
#include <tipb/select.pb.h>
#ifdef __clang__
#pragma clang diagnostic pop
#endif
#include <boost/noncopyable.hpp>

namespace DB
{
template <typename Tunnel>
class MPPTunnelSetBase : private boost::noncopyable
{
public:
    using TunnelPtr = std::shared_ptr<Tunnel>;

    void clearExecutionSummaries(tipb::SelectResponse & response);

    /// for both broadcast writing and partition writing, only
    /// return meaningful execution summary for the first tunnel,
    /// because in TiDB, it does not know enough information
    /// about the execution details for the mpp query, it just
    /// add up all the execution summaries for the same executor,
    /// so if return execution summary for all the tunnels, the
    /// information in TiDB will be amplified, which may make
    /// user confused.
    // this is a broadcast writing.
    void write(tipb::SelectResponse & response);
    void write(mpp::MPPDataPacket & packet);

    // this is a partition writing.
    void write(tipb::SelectResponse & response, int16_t partition_id);
    void write(mpp::MPPDataPacket & packet, int16_t partition_id);

    uint16_t getPartitionNum() const { return tunnels.size(); }

    void addTunnel(const TunnelPtr & tunnel)
    {
        tunnels.push_back(tunnel);
        if (!tunnel->isLocal())
        {
            remote_tunnel_cnt++;
        }
    }

    int getRemoteTunnelCnt()
    {
        return remote_tunnel_cnt;
    }

    const std::vector<TunnelPtr> & getTunnels() const { return tunnels; }

private:
    std::vector<TunnelPtr> tunnels;

    int remote_tunnel_cnt = 0;
};

class MPPTunnelSet : public MPPTunnelSetBase<MPPTunnel>
{
public:
    using Base = MPPTunnelSetBase<MPPTunnel>;
    using Base::Base;
};

using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;

} // namespace DB
