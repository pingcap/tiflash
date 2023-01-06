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
#include <Flash/Coprocessor/CompressedCHBlockChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <common/types.h>


namespace DB
{

struct CompressCHBlockChunkCodecStream;

enum HashPartitionWriterVersion : int64_t
{
    HashPartitionWriterV0 = 0,
    HashPartitionWriterV1,
    HashPartitionWriterVersionMax,
};

template <class ExchangeWriterPtr>
class HashPartitionWriterImplV1 : public DAGResponseWriter
{
public:
    HashPartitionWriterImplV1(
        ExchangeWriterPtr writer_,
        std::vector<Int64> partition_col_ids_,
        TiDB::TiDBCollators collators_,
        Int64 partition_batch_limit_,
        DAGContext & dag_context_,
        mpp::CompressionMode compression_mode_);
    void write(const Block & block) override;
    void flush() override;

private:
    void partitionAndEncodeThenWriteBlocks();
    void partitionAndEncodeThenWriteBlocksTest();

    void writePackets(TrackedMppDataPacketPtrs && packets);

private:
    uint16_t partition_num;
    Int64 partition_batch_limit;
    ExchangeWriterPtr writer;
    std::vector<Block> blocks;
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators collators;
    size_t rows_in_blocks;
    DataTypes expected_types;
    mpp::CompressionMode compression_mode{};
};

CompressionMethod ToInternalCompressionMethod(mpp::CompressionMode compression_mode);

} // namespace DB
