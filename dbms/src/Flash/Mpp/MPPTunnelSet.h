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

#pragma once

#include <Flash/Mpp/MPPTaskId.h>
#include <Flash/Mpp/MPPTunnel.h>
#include <Flash/Mpp/MppVersion.h>

namespace DB
{
class DAGContext;

template <typename Tunnel>
class MPPTunnelSetBase : private boost::noncopyable
{
public:
    using TunnelPtr = std::shared_ptr<Tunnel>;
    explicit MPPTunnelSetBase(const String & req_id)
        : log(Logger::get(req_id))
    {}

    void write(TrackedMppDataPacketPtr && data, size_t index);
    void forceWrite(TrackedMppDataPacketPtr && data, size_t index);

    void write(tipb::SelectResponse & response, size_t index);
    void forceWrite(tipb::SelectResponse & response, size_t index);

    /// this is a execution summary writing.
    /// only return meaningful execution summary for the first tunnel,
    /// because in TiDB, it does not know enough information
    /// about the execution details for the mpp query, it just
    /// add up all the execution summaries for the same executor,
    /// so if return execution summary for all the tunnels, the
    /// information in TiDB will be amplified, which may make
    /// user confused.
    void sendExecutionSummary(const tipb::SelectResponse & response);

    void close(const String & reason, bool wait_sender_finish);
    void finishWrite();
    void registerTunnel(const MPPTaskId & id, const TunnelPtr & tunnel);

    TunnelPtr getTunnelByReceiverTaskId(const MPPTaskId & id);

    uint16_t getPartitionNum() const { return tunnels.size(); }

    int getExternalThreadCnt() { return external_thread_cnt; }
    size_t getLocalTunnelCnt() { return local_tunnel_cnt; }

    const std::vector<TunnelPtr> & getTunnels() const { return tunnels; }

    bool isWritable() const;

    WaitResult waitForWritable() const;

    bool isLocal(size_t index) const;

private:
    std::vector<TunnelPtr> tunnels;
    std::unordered_map<MPPTaskId, size_t> receiver_task_id_to_index_map;
    const LoggerPtr log;

    int external_thread_cnt = 0;
    size_t local_tunnel_cnt = 0;
};

class MPPTunnelSet : public MPPTunnelSetBase<MPPTunnel>
{
public:
    using Base = MPPTunnelSetBase<MPPTunnel>;
    using Base::Base;
};

using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;

} // namespace DB
