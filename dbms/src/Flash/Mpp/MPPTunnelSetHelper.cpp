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

#include <Flash/Mpp/MPPTunnelSetHelper.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>

namespace DB::MPPTunnelSetHelper
{
TrackedMppDataPacketPtr toPacket(Blocks & blocks, const std::vector<tipb::FieldType> & field_types)
{
    CHBlockChunkCodec codec;
    auto codec_stream = codec.newCodecStream(field_types);
    auto tracked_packet = std::make_shared<TrackedMppDataPacket>();
    while (!blocks.empty())
    {
        const auto & block = blocks.back();
        codec_stream->encode(block, 0, block.rows());
        blocks.pop_back();
        tracked_packet->addChunk(codec_stream->getString());
        codec_stream->clear();
    }
    return tracked_packet;
}

TrackedMppDataPacketPtr toFineGrainedPacket(
    const Block & header,
    std::vector<IColumn::ScatterColumns> & scattered,
    size_t bucket_idx,
    UInt64 fine_grained_shuffle_stream_count,
    size_t num_columns,
    const std::vector<tipb::FieldType> & field_types)
{
    CHBlockChunkCodec codec;
    auto codec_stream = codec.newCodecStream(field_types);
    auto tracked_packet = std::make_shared<TrackedMppDataPacket>();
    for (uint64_t stream_idx = 0; stream_idx < fine_grained_shuffle_stream_count; ++stream_idx)
    {
        // assemble scatter columns into a block
        MutableColumns columns;
        columns.reserve(num_columns);
        for (size_t col_id = 0; col_id < num_columns; ++col_id)
            columns.emplace_back(std::move(scattered[col_id][bucket_idx + stream_idx]));
        auto block = header.cloneWithColumns(std::move(columns));

        // encode into packet
        codec_stream->encode(block, 0, block.rows());
        tracked_packet->addChunk(codec_stream->getString());
        tracked_packet->getPacket().add_stream_ids(stream_idx);
        codec_stream->clear();

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
} // namespace DB::HashBaseWriterHelper
