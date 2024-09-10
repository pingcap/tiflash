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

#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <common/types.h>

namespace DB
{
class DAGContext;
enum class CompressionMethod;
enum MPPDataPacketVersion : int64_t;

template <class ExchangeWriterPtr>
class HashPartitionWriter : public DAGResponseWriter
{
public:
    HashPartitionWriter(
        ExchangeWriterPtr writer_,
        std::vector<Int64> partition_col_ids_,
        TiDB::TiDBCollators collators_,
        Int64 batch_send_min_limit_,
        DAGContext & dag_context_,
        MPPDataPacketVersion data_codec_version_,
        tipb::CompressionMode compression_mode_);
    bool doWrite(const Block & block) override;
    WaitResult waitForWritable() const override;
    bool doFlush() override;
    void notifyNextPipelineWriter() override;

private:
    bool writeImpl(const Block & block);
    bool writeImplV1(const Block & block);
    void partitionAndWriteBlocks();
    void partitionAndWriteBlocksV1();

    void writePartitionBlocks(std::vector<Blocks> & partition_blocks);

private:
    Int64 batch_send_min_limit;
    ExchangeWriterPtr writer;
    std::vector<Block> blocks;
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators collators;
    size_t rows_in_blocks;
    uint16_t partition_num;
    // support data compression
    int64_t mem_size_in_blocks{};
    DataTypes expected_types;
    MPPDataPacketVersion data_codec_version;
    CompressionMethod compression_method{};
};

} // namespace DB
