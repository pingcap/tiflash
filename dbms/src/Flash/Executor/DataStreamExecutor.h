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

#include <DataStreams/BlockIO.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Executor/QueryExecutor.h>

namespace DB
{
class DataStreamExecutor : public QueryExecutor
{
public:
    explicit DataStreamExecutor(const BlockIO & block_io)
        : QueryExecutor(block_io.process_list_entry)
        , data_stream(block_io.in)
    {
        assert(data_stream);
    }

    String dump() const override;

    void cancel(bool is_kill) override;

    int estimateNewThreadCount() override;

protected:
    ExecutionResult execute(ResultHandler result_handler) override;

protected:
    BlockInputStreamPtr data_stream;
};
} // namespace DB
