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

#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/ShardInfo.h>
#include <Operators/Operator.h>
#include <Storages/Tantivy/TantivyInputStream.h>
#include <Storages/Tantivy/TiCIReadTaskPool.h>
#include <common/types.h>
#include <pingcap/coprocessor/Client.h>

namespace DB
{

class TantivyReaderSourceOp : public SourceOp
{
public:
    TantivyReaderSourceOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        TS::TiCIReadTaskPoolPtr task_pool_,
        const NamesAndTypes & return_columns);

    String getName() const override;

protected:
    void operatePrefixImpl() override;
    void operateSuffixImpl() override;

    OperatorStatus readImpl(Block & block) override;

    IOProfileInfoPtr getIOProfileInfo() const override { return io_profile_info; }

private:
    Block popFromBlockQueue();

private:
    std::queue<Block> block_queue;

    UInt64 total_rows{};
    IOProfileInfoPtr io_profile_info;
    TS::TiCIReadTaskPoolPtr task_pool;
    bool done = false;

    BlockInputStreamPtr cur_stream;
    Stopwatch watcher;
};

} // namespace DB
