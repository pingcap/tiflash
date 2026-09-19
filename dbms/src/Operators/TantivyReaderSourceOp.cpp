// Copyright 2025 PingCAP, Inc.
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

#include <Core/NamesAndTypes.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/ShardInfo.h>
#include <Operators/Operator.h>
#include <Operators/TantivyReaderSourceOp.h>
#include <Storages/Tantivy/TantivyInputStream.h>
#include <common/types.h>

namespace DB
{
TantivyReaderSourceOp::TantivyReaderSourceOp(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    const TS::TiCIReadTaskPoolPtr task_pool_,
    const NamesAndTypes & return_columns)
    : SourceOp(exec_context_, req_id)
    , task_pool(task_pool_)
{
    setHeader(Block(return_columns));
}

String TantivyReaderSourceOp::getName() const
{
    return "TantivyReaderSourceOp";
}

void TantivyReaderSourceOp::operatePrefixImpl()
{
    watcher.start();
}

void TantivyReaderSourceOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "TantivyReaderSourceOp read {} rows, took {} ms", total_rows, watcher.elapsedMilliseconds());
}

Block TantivyReaderSourceOp::popFromBlockQueue()
{
    assert(!block_queue.empty());
    Block block = std::move(block_queue.front());
    block_queue.pop();
    return block;
}

OperatorStatus TantivyReaderSourceOp::readImpl(Block & block)
{
    if (unlikely(done))
    {
        return OperatorStatus::HAS_OUTPUT;
    }

    while (true)
    {
        while (!cur_stream)
        {
            auto task = task_pool->getNextTask();
            if (!task)
            {
                done = true;
                return OperatorStatus::HAS_OUTPUT;
            }
            cur_stream = task_pool->buildInputStream(task);
        }
        Block res = cur_stream->read();
        if (res)
        {
            total_rows += res.rows();
            block.swap(res);
            return OperatorStatus::HAS_OUTPUT;
        }
        else
        {
            cur_stream = {};
            continue;
        }
    }
}

} // namespace DB
