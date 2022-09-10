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

#include <Flash/Mpp/MPPTunnelSet.h>
#include <Common/Logger.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <common/logger_useful.h>
#include <Flash/Mpp/TrackedMppDataPacket.h>

namespace DB
{
class AsyncMPPTunnelWriter
{
public:
    AsyncMPPTunnelWriter(
        MPPTunnelSetPtr writer_,
        std::vector<Int64> partition_col_ids_,
        TiDB::TiDBCollators collators_,
        tipb::ExchangeType exchange_type_,
        Int64 records_per_chunk_,
        Int64 batch_send_min_limit_,
        DAGContext & dag_context_);
    void write(Block && block);
    void finishWrite();
    bool isReady();

private:
    void batchWrite();

    void encodeThenWriteBlocks(const std::vector<Block> & input_blocks) const;
    void partitionAndEncodeThenWriteBlocks(std::vector<Block> & input_blocks) const;

    void writePackets(const std::vector<size_t> & responses_row_count, std::vector<TrackedMppDataPacket> & packets) const;

    Int64 records_per_chunk;
    DAGContext & dag_context;
    Int64 batch_send_min_limit;
    tipb::ExchangeType exchange_type;
    MPPTunnelSetPtr writer;
    std::vector<Block> blocks;
    std::vector<Int64> partition_col_ids;
    TiDB::TiDBCollators collators;
    size_t rows_in_blocks;
    uint16_t partition_num;
    std::unique_ptr<ChunkCodecStream> chunk_codec_stream;
};

} // namespace DB
