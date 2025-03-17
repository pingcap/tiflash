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

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Operators/TantivyReaderSourceOp.h>

#include "DataTypes/DataTypeFactory.h"
#include "DataTypes/DataTypeNullable.h"
#include "Operators/Operator.h"
#include "Storages/Tantivy/TantivyInputStream.h"

namespace DB
{
TantivyReaderSourceOp::TantivyReaderSourceOp(PipelineExecutorContext & exec_context_, const String & req_id)
    : SourceOp(exec_context_, req_id)
{
    LOG_INFO(log, "6666666666666666666666666666666666");
    NamesAndTypes names_and_types;
    auto tp = DataTypeFactory::instance().get("StringV2");
    names_and_types.emplace_back("test", tp);
    names_and_types.emplace_back("test2", tp);
    setHeader(Block(getColumnWithTypeAndName(names_and_types)));
    input = std::make_shared<TS::TantivyInputStream>(log, "", "parquet");
}

String TantivyReaderSourceOp::getName() const
{
    return "TantivyReaderSourceOp";
}

void TantivyReaderSourceOp::operatePrefixImpl()
{
    LOG_DEBUG(log, "start reading from remote coprocessor", total_rows);
}

void TantivyReaderSourceOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "finish read {} rows from remote coprocessor", total_rows);
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
    LOG_INFO(log, "666666666666666666666");
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
