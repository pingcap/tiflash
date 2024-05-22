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
class BroadcastOrPassThroughWriter : public DAGResponseWriter
{
public:
    BroadcastOrPassThroughWriter(
        ExchangeWriterPtr writer_,
        Int64 batch_send_min_limit_,
        DAGContext & dag_context_,
        MPPDataPacketVersion data_codec_version_,
        tipb::CompressionMode compression_mode_,
        tipb::ExchangeType exchange_type_);
    void write(const Block & block) override;
    bool isWritable() const override;
    WaitResult waitForWritable() const override;
    void flush() override;

private:
    void writeBlocks();

private:
    Int64 batch_send_min_limit;
    ExchangeWriterPtr writer;
    std::vector<Block> blocks;
    size_t rows_in_blocks;
    const tipb::ExchangeType exchange_type;

    // support data compression
    DataTypes expected_types;
    MPPDataPacketVersion data_codec_version;
    CompressionMethod compression_method{};
};

} // namespace DB
