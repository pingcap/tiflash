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

#include <Core/Block.h>
#include <Flash/Mpp/MppVersion.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>

namespace DB
{
enum class CompressionMethod;
}

namespace DB::MPPTunnelSetHelper
{
TrackedMppDataPacketPtr ToPacketV0(
    Blocks & blocks,
    const std::vector<tipb::FieldType> & field_types,
    MppVersion mpp_version);

TrackedMppDataPacketPtr ToCompressedPacket(
    const TrackedMppDataPacketPtr & uncompressed_source,
    MPPDataPacketVersion version,
    CompressionMethod method);

TrackedMppDataPacketPtr ToPacket(
    Blocks && blocks,
    MPPDataPacketVersion version,
    CompressionMethod method,
    size_t & original_size,
    MppVersion mpp_version);

TrackedMppDataPacketPtr ToPacket(
    const Block & header,
    std::vector<MutableColumns> && part_columns,
    MPPDataPacketVersion version,
    CompressionMethod compression_method,
    size_t & original_size,
    MppVersion mpp_version);

TrackedMppDataPacketPtr ToFineGrainedPacketV0(
    const Block & header,
    std::vector<IColumn::ScatterColumns> & scattered,
    size_t bucket_idx,
    UInt64 fine_grained_shuffle_stream_count,
    size_t num_columns,
    const std::vector<tipb::FieldType> & field_types,
    MppVersion mpp_version);

TrackedMppDataPacketPtr ToFineGrainedPacket(
    const Block & header,
    std::vector<IColumn::ScatterColumns> & scattered,
    size_t bucket_idx,
    UInt64 fine_grained_shuffle_stream_count,
    size_t num_columns,
    MPPDataPacketVersion version,
    CompressionMethod compression_method,
    size_t & original_size,
    MppVersion mpp_version);

} // namespace DB::MPPTunnelSetHelper
