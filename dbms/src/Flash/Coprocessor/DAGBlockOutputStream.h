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

#include <Core/Types.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGQuerySource.h>
#include <common/logger_useful.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Flash/Coprocessor/DAGResponseWriter.h>
#include <tipb/select.pb.h>

#pragma GCC diagnostic pop

namespace DB
{
/// Serializes the stream of blocks in TiDB DAG response format.
class DAGBlockOutputStream : public IBlockOutputStream
{
public:
    DAGBlockOutputStream(Block && header_, std::unique_ptr<DAGResponseWriter> response_writer_);

    Block getHeader() const { return header; }
    void write(const Block & block);
    void writePrefix();
    void writeSuffix();

private:
    Block header;
    std::unique_ptr<DAGResponseWriter> response_writer;
};

} // namespace DB
