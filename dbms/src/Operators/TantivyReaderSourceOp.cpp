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
    const UInt32 & keyspace_id,
    const Int64 & table_id,
    const Int64 & index_id,
    const ShardInfoList & query_shard_infos,
    const NamesAndTypes & return_columns,
    const UInt64 & limit,
    const UInt64 & read_ts,
    const google::protobuf::RepeatedPtrField<tipb::Expr> & expr,
    bool is_count)
    : SourceOp(exec_context_, req_id)
{
    setHeader(Block(return_columns));

    input = std::make_shared<TS::TantivyInputStream>(
        log,
        keyspace_id,
        table_id,
        index_id,
        query_shard_infos,
        return_columns,
        limit,
        read_ts,
        expr,
        is_count);
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
    if (!block_queue.empty())
    {
        block = popFromBlockQueue();
        return OperatorStatus::HAS_OUTPUT;
    }

    assert(block_queue.empty());
    for (;;)
    {
        auto tmp = input->readImpl();
        block_queue.push(tmp);
        total_rows += tmp.rows();
        if (!tmp)
            break;
    }
    block = popFromBlockQueue();
    return OperatorStatus::HAS_OUTPUT;
}

} // namespace DB
