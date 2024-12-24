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

#include <Flash/Mpp/MPPTunnelSet.h>
#include <IO/Compression/CompressionMethod.h>

#include "Flash/Coprocessor/WaitResult.h"

namespace DB
{
class MPPTunnelSetWriterBase : private boost::noncopyable
{
public:
    MPPTunnelSetWriterBase(
        const MPPTunnelSetPtr & mpp_tunnel_set_,
        const std::vector<tipb::FieldType> & result_field_types_,
        const String & req_id);

    virtual ~MPPTunnelSetWriterBase() = default;

    // this is a root mpp writing.
    void write(tipb::SelectResponse & response);
    // this is a broadcast or pass through writing.
    // data codec version V0
    void broadcastWrite(Blocks & blocks);
    void passThroughWrite(Blocks & blocks);
    // data codec version > V0
    void broadcastWrite(Blocks & blocks, MPPDataPacketVersion version, CompressionMethod compression_method);
    void passThroughWrite(Blocks & blocks, MPPDataPacketVersion version, CompressionMethod compression_method);
    // this is a partition writing.
    // data codec version V0
    void partitionWrite(Blocks & blocks, int16_t partition_id);
    // data codec version > V0
    void partitionWrite(
        const Block & header,
        std::vector<MutableColumns> && part_columns,
        int16_t partition_id,
        MPPDataPacketVersion version,
        CompressionMethod compression_method);
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

    uint16_t getPartitionNum() const { return mpp_tunnel_set->getPartitionNum(); }

    virtual WaitResult waitForWritable() const = 0;

protected:
    virtual void writeToTunnel(TrackedMppDataPacketPtr && data, size_t index) = 0;
    virtual void writeToTunnel(tipb::SelectResponse & response, size_t index) = 0;

protected:
    MPPTunnelSetPtr mpp_tunnel_set;
    std::vector<tipb::FieldType> result_field_types;
    const LoggerPtr log;
};

class SyncMPPTunnelSetWriter : public MPPTunnelSetWriterBase
{
public:
    SyncMPPTunnelSetWriter(
        const MPPTunnelSetPtr & mpp_tunnel_set_,
        const std::vector<tipb::FieldType> & result_field_types_,
        const String & req_id)
        : MPPTunnelSetWriterBase(mpp_tunnel_set_, result_field_types_, req_id)
    {}

    // For sync writer, `waitForWritable` will not be called, so an exception is thrown here.
    WaitResult waitForWritable() const override { return WaitResult::Ready; }

protected:
    void writeToTunnel(TrackedMppDataPacketPtr && data, size_t index) override;
    void writeToTunnel(tipb::SelectResponse & response, size_t index) override;
};
using SyncMPPTunnelSetWriterPtr = std::shared_ptr<SyncMPPTunnelSetWriter>;

class AsyncMPPTunnelSetWriter : public MPPTunnelSetWriterBase
{
public:
    AsyncMPPTunnelSetWriter(
        const MPPTunnelSetPtr & mpp_tunnel_set_,
        const std::vector<tipb::FieldType> & result_field_types_,
        const String & req_id)
        : MPPTunnelSetWriterBase(mpp_tunnel_set_, result_field_types_, req_id)
    {}

    WaitResult waitForWritable() const override { return mpp_tunnel_set->waitForWritable(); }

protected:
    void writeToTunnel(TrackedMppDataPacketPtr && data, size_t index) override;
    void writeToTunnel(tipb::SelectResponse & response, size_t index) override;
};
using AsyncMPPTunnelSetWriterPtr = std::shared_ptr<AsyncMPPTunnelSetWriter>;

} // namespace DB
