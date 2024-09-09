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

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop

namespace DB
{
/// Serializes the stream of blocks in TiDB DAG response format.
class UnaryDAGResponseWriter : public DAGResponseWriter
{
public:
    UnaryDAGResponseWriter(tipb::SelectResponse * response_, Int64 records_per_chunk_, DAGContext & dag_context_);

    bool doWrite(const Block & block) override;
    bool doFlush() override;
    void triggerPipelineWriterNotify() override {};
    void encodeChunkToDAGResponse();
    void appendWarningsToDAGResponse();

private:
    tipb::SelectResponse * dag_response;
    std::unique_ptr<ChunkCodecStream> chunk_codec_stream;
    Int64 current_records_num;
};

} // namespace DB
