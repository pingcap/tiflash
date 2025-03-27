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

#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Operators/TantivyReaderSourceOp.h>

#include "Core/NamesAndTypes.h"
#include "Operators/Operator.h"
#include "Storages/Tantivy/TantivyInputStream.h"
#include "common/types.h"

namespace DB
{
TantivyReaderSourceOp::TantivyReaderSourceOp(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    const Int64 & table_id,
    const Int64 & index_id,
    const NamesAndTypes & query_columns,
    const NamesAndTypes & return_columns,
    const String & query_json_str,
    const UInt64 & limit)
    : SourceOp(exec_context_, req_id)
{
    setHeader(Block(getColumnWithTypeAndName(return_columns)));
    input = std::make_shared<TS::TantivyInputStream>(
        log,
        table_id,
        index_id,
        query_columns,
        return_columns,
        query_json_str,
        limit);
}

String TantivyReaderSourceOp::getName() const
{
    return "TantivyReaderSourceOp";
}

void TantivyReaderSourceOp::operatePrefixImpl()
{
    LOG_DEBUG(log, "start reading from TantivyReaderSourceOp", total_rows);
}

void TantivyReaderSourceOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish read {} rows from TantivyReaderSourceOp", total_rows);
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
    block = input->readImpl();
    return OperatorStatus::HAS_OUTPUT;
}
OperatorStatus TantivyReaderSourceOp::awaitImpl()
{
    if (!block_queue.empty())
        return OperatorStatus::HAS_OUTPUT;
    return OperatorStatus::FINISHED;
}
} // namespace DB
