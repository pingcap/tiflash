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

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <common/types.h>

namespace DB
{
template <class StreamWriterPtr>
class HashPartitionWriter : public DAGResponseWriter
{
public:
    HashPartitionWriter(
        StreamWriterPtr writer_,
        std::vector<Int64> partition_col_ids_,
        TiDB::TiDBCollators collators_,
        Int64 batch_send_min_limit_,
        bool should_send_exec_summary_at_last,
        DAGContext & dag_context_);
    void write(const Block & block) override;
    void finishWrite() override;

private:
    template <bool send_exec_summary_at_last>
    void partitionAndEncodeThenWriteBlocks();

    template <bool send_exec_summary_at_last>
    void writePackets(std::vector<TrackedMppDataPacket> & packets);

    Int64 batch_send_min_limit;
    bool should_send_exec_summary_at_last;
    StreamWriterPtr writer;
    std::vector<Block> blocks;
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators collators;
    size_t rows_in_blocks;
    uint16_t partition_num;
    std::unique_ptr<ChunkCodecStream> chunk_codec_stream;
};

} // namespace DB
