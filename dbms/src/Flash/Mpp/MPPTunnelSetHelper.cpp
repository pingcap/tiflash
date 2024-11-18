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

#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>
#include <Flash/Mpp/MPPTunnelSetHelper.h>
#include <IO/Compression/CompressionInfo.h>

namespace DB::MPPTunnelSetHelper
{

TrackedMppDataPacketPtr ToPacket(
    const Block & header,
    std::vector<MutableColumns> && part_columns,
    MPPDataPacketVersion version,
    CompressionMethod method,
    size_t & original_size)
{
    assert(version > MPPDataPacketV0);

    auto && codec = CHBlockChunkCodecV1{
        header,
    };

    auto && res = codec.encode(std::move(part_columns), method);

    auto tracked_packet = std::make_shared<TrackedMppDataPacket>(version);
    if likely (!res.empty())
    {
        tracked_packet->addChunk(std::move(res));
        original_size += codec.original_size;
    }
    return tracked_packet;
}

TrackedMppDataPacketPtr ToPacket(
    Blocks && blocks,
    MPPDataPacketVersion version,
    CompressionMethod method,
    size_t & original_size)
{
    assert(version > MPPDataPacketV0);

    if (blocks.empty())
        return nullptr;
    const Block & header = blocks.front().cloneEmpty();
    auto && codec = CHBlockChunkCodecV1{header};
    auto && res = codec.encode(std::move(blocks), method, false);
    if unlikely (res.empty())
        return nullptr;

    auto tracked_packet = std::make_shared<TrackedMppDataPacket>(version);
    tracked_packet->addChunk(std::move(res));
    original_size += codec.original_size;
    return tracked_packet;
}

TrackedMppDataPacketPtr ToPacketV0(Blocks & blocks, const std::vector<tipb::FieldType> & field_types)
{
    CHBlockChunkCodec codec;
    auto codec_stream = codec.newCodecStream(field_types);
    auto tracked_packet = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
    for (auto & block : blocks)
    {
        codec_stream->encode(block, 0, block.rows());
        block.clear();
        tracked_packet->addChunk(codec_stream->getString());
        codec_stream->clear();
    }
    return tracked_packet;
}

TrackedMppDataPacketPtr ToFineGrainedPacket(
    const Block & header,
    std::vector<IColumn::ScatterColumns> & scattered,
    size_t bucket_idx,
    UInt64 fine_grained_shuffle_stream_count,
    size_t num_columns,
    MPPDataPacketVersion version,
    CompressionMethod method,
    size_t & original_size)
{
    assert(version > MPPDataPacketV0);

    auto && codec = CHBlockChunkCodecV1{
        header,
    };
    auto tracked_packet = std::make_shared<TrackedMppDataPacket>(version);

    for (uint64_t stream_idx = 0; stream_idx < fine_grained_shuffle_stream_count; ++stream_idx)
    {
        // assemble scatter columns into a block
        MutableColumns columns;
        columns.reserve(num_columns);
        for (size_t col_id = 0; col_id < num_columns; ++col_id)
            columns.emplace_back(std::move(scattered[col_id][bucket_idx + stream_idx]));

        auto && res = codec.encode(columns, method);
        if (!res.empty())
        {
            tracked_packet->addChunk(std::move(res));
            tracked_packet->getPacket().add_stream_ids(stream_idx);
        }

        for (size_t col_id = 0; col_id < num_columns; ++col_id)
        {
            columns[col_id]->popBack(columns[col_id]->size()); // clear column
            scattered[col_id][bucket_idx + stream_idx] = std::move(columns[col_id]);
        }
    }

    original_size += codec.original_size;
    return tracked_packet;
}

TrackedMppDataPacketPtr ToFineGrainedPacketV0(
    const Block & header,
    std::vector<IColumn::ScatterColumns> & scattered,
    size_t bucket_idx,
    UInt64 fine_grained_shuffle_stream_count,
    size_t num_columns,
    const std::vector<tipb::FieldType> & field_types)
{
    CHBlockChunkCodec codec;
    auto codec_stream = codec.newCodecStream(field_types);
    auto tracked_packet = std::make_shared<TrackedMppDataPacket>(MPPDataPacketV0);
    for (uint64_t stream_idx = 0; stream_idx < fine_grained_shuffle_stream_count; ++stream_idx)
    {
        // assemble scatter columns into a block
        MutableColumns columns;
        columns.reserve(num_columns);
        for (size_t col_id = 0; col_id < num_columns; ++col_id)
            columns.emplace_back(std::move(scattered[col_id][bucket_idx + stream_idx]));
        auto block = header.cloneWithColumns(std::move(columns));
        if (block.rows())
        {
            // encode into packet
            codec_stream->encode(block, 0, block.rows());
            tracked_packet->addChunk(codec_stream->getString());
            tracked_packet->getPacket().add_stream_ids(stream_idx);
            codec_stream->clear();
        }

        // disassemble the block back to scatter columns
        columns = block.mutateColumns();
        for (size_t col_id = 0; col_id < num_columns; ++col_id)
        {
            columns[col_id]->popBack(columns[col_id]->size()); // clear column
            scattered[col_id][bucket_idx + stream_idx] = std::move(columns[col_id]);
        }
    }
    return tracked_packet;
}

TrackedMppDataPacketPtr ToCompressedPacket(
    const TrackedMppDataPacketPtr & uncompressed_source,
    MPPDataPacketVersion version,
    CompressionMethod method)
{
    assert(uncompressed_source);
    for ([[maybe_unused]] const auto & chunk : uncompressed_source->getPacket().chunks())
    {
        assert(!chunk.empty());
        assert(static_cast<CompressionMethodByte>(chunk[0]) == CompressionMethodByte::NONE);
    }

    // re-encode by specified compression method
    auto compressed_tracked_packet = std::make_shared<TrackedMppDataPacket>(version);
    for (const auto & chunk : uncompressed_source->getPacket().chunks())
    {
        auto && compressed_buffer = CHBlockChunkCodecV1::encode({&chunk[1], chunk.size() - 1}, method);
        assert(!compressed_buffer.empty());

        compressed_tracked_packet->addChunk(std::move(compressed_buffer));
    }
    return compressed_tracked_packet;
}


} // namespace DB::MPPTunnelSetHelper
