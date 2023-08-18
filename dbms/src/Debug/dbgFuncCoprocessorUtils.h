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
#include <Core/BlockUtils.h>
#include <DataStreams/BlocksListBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Debug/DAGProperties.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <Interpreters/Context.h>
#include <Interpreters/sortBlock.h>
#include <Poco/StringTokenizer.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

std::unique_ptr<ChunkCodec> getCodec(tipb::EncodeType encode_type);
DAGSchema getSelectSchema(Context & context);
SortDescription generateSDFromSchema(const DAGSchema & schema);
DAGProperties getDAGProperties(const String & prop_string);
void chunksToBlocks(const DAGSchema & schema, const tipb::SelectResponse & dag_response, BlocksList & blocks);
BlockInputStreamPtr outputDAGResponse(Context &, const DAGSchema & schema, const tipb::SelectResponse & dag_response);
// Just for test usage, dag_response should not contain result more than 128M
Block getMergedBigBlockFromDagRsp(Context & context, const DAGSchema & schema, const tipb::SelectResponse & dag_response);
bool dagRspEqual(Context & context, const tipb::SelectResponse & expected, const tipb::SelectResponse & actual, String & unequal_msg);

} // namespace DB
