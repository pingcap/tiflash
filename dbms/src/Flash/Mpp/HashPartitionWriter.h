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
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>
#include <common/types.h>

namespace DB::HashBaseWriterHelper
{
struct HashPartitionWriterHelperV1;
}
namespace DB
{
class DAGContext;
enum class CompressionMethod;
enum MPPDataPacketVersion : int64_t;

template <class ExchangeWriterPtr>
class HashPartitionWriter : public DAGResponseWriter
{
public:
    // If `batch_send_min_limit_` is LT 0, `batch_send_min_limit` will be set to `8192 * partition_num`
    HashPartitionWriter(
        ExchangeWriterPtr writer_,
        std::vector<Int64> partition_col_ids_,
        TiDB::TiDBCollators collators_,
        Int64 batch_send_min_limit_,
        DAGContext & dag_context_,
        MPPDataPacketVersion data_codec_version,
        tipb::CompressionMode compression_mode_);
    void write(const Block & block) override;
    void flush() override;
    ~HashPartitionWriter() override;

private:
    void partitionAndEncodeThenWriteBlocks();
    void partitionAndEncodeThenWriteBlocksImpl();
    void partitionAndEncodeThenWriteBlocksImplV1();

    void writePackets(TrackedMppDataPacketPtrs & packets);

private:
    Int64 batch_send_min_limit;
    ExchangeWriterPtr writer;
    std::vector<Block> blocks;
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators collators;
    size_t rows_in_blocks;
    uint16_t partition_num;
    std::unique_ptr<ChunkCodecStream> chunk_codec_stream;
    // support data compression
    MPPDataPacketVersion data_codec_version;
    CompressionMethod compression_method{};
    std::unique_ptr<HashBaseWriterHelper::HashPartitionWriterHelperV1> codec_helper_v1{};
};

} // namespace DB
