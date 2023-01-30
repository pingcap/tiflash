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
#include <Flash/Mpp/MppVersion.h>

namespace DB
{
class DAGContext;

template <typename Tunnel>
class MPPTunnelSetBase : private boost::noncopyable
{
public:
    using TunnelPtr = std::shared_ptr<Tunnel>;
    MPPTunnelSetBase(DAGContext & dag_context, const String & req_id);

    // this is a root mpp writing.
    void write(tipb::SelectResponse & response);
    // this is a broadcast or pass through writing.
    // data codec version V0
    void broadcastOrPassThroughWrite(Blocks & blocks);
    // this is a partition writing.
    // data codec version V0
    void partitionWrite(Blocks & blocks, int16_t partition_id);
    // data codec version > V0
    void partitionWrite(const Block & header, std::vector<MutableColumns> && part_columns, int16_t partition_id, MPPDataPacketVersion version, CompressionMethod compression_method);
    // this is a fine grained shuffle writing.
    // data codec version V0
    void fineGrainedShuffleWrite(
        const Block & header,
        std::vector<IColumn::ScatterColumns> & scattered,
        size_t bucket_idx,
        UInt64 fine_grained_shuffle_stream_count,
        size_t num_columns,
        int16_t partition_id);
    void fineGrainedShuffleWrite(
        const Block & header,
        std::vector<IColumn::ScatterColumns> & scattered,
        size_t bucket_idx,
        UInt64 fine_grained_shuffle_stream_count,
        size_t num_columns,
        int16_t partition_id,
        MPPDataPacketVersion version,
        CompressionMethod compression_method);
    /// this is a execution summary writing.
    /// for both broadcast writing and partition/fine grained shuffle writing, only
    /// return meaningful execution summary for the first tunnel,
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

    int getExternalThreadCnt()
    {
        return external_thread_cnt;
    }

    const std::vector<TunnelPtr> & getTunnels() const { return tunnels; }

private:
    bool isLocal(size_t index) const;

private:
    std::vector<TunnelPtr> tunnels;
    std::unordered_map<MPPTaskId, size_t> receiver_task_id_to_index_map;
    const LoggerPtr log;

    std::vector<tipb::FieldType> result_field_types;

    int external_thread_cnt = 0;
};

class MPPTunnelSet : public MPPTunnelSetBase<MPPTunnel>
{
public:
    using Base = MPPTunnelSetBase<MPPTunnel>;
    using Base::Base;
};

using MPPTunnelSetPtr = std::shared_ptr<MPPTunnelSet>;

} // namespace DB
