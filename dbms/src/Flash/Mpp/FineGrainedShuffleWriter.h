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
class FineGrainedShuffleWriter : public DAGResponseWriter
{
public:
    FineGrainedShuffleWriter(
        ExchangeWriterPtr writer_,
        std::vector<Int64> partition_col_ids_,
        TiDB::TiDBCollators collators_,
        DAGContext & dag_context_,
        UInt64 fine_grained_shuffle_stream_count_,
        UInt64 fine_grained_shuffle_batch_size,
        MPPDataPacketVersion data_codec_version_,
        tipb::CompressionMode compression_mode_);
    void prepare(const Block & sample_block) override;
    void write(const Block & block) override;
    WaitResult waitForWritable() const override;
    void flush() override;

private:
    void batchWriteFineGrainedShuffle();
    void initScatterColumns();
    template <MPPDataPacketVersion version>
    void batchWriteFineGrainedShuffleImpl();

private:
    ExchangeWriterPtr writer;
    std::vector<Block> blocks;
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators collators;
    size_t rows_in_blocks = 0;
    uint16_t partition_num;
    UInt64 fine_grained_shuffle_stream_count;
    UInt64 fine_grained_shuffle_batch_size;

    Block header;
    bool prepared = false;
    size_t num_columns = 0, num_bucket = 0,
           batch_send_row_limit
        = 0; // Assign they initial values to pass clang-tidy check, they will be initialized in prepare method
    std::vector<String> partition_key_containers_for_reuse;
    WeakHash32 hash;
    IColumn::Selector selector;
    std::vector<IColumn::ScatterColumns> scattered; // size = num_columns
    // support data compression
    DataTypes expected_types;
    MPPDataPacketVersion data_codec_version;
    CompressionMethod compression_method{};
};

} // namespace DB
