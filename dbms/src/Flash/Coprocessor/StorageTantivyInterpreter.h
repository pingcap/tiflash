// Copyright 2025 PingCAP, Ltd.
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

#include <Core/Names.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Flash/Coprocessor/ShardInfo.h>
#include <Flash/Coprocessor/TiCIScan.h>
#include <Interpreters/Context.h>
#include <Operators/CoprocessorReaderSourceOp.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/TMTStorages.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTantivy.h>

namespace DB
{

class StorageTantivyIterpreter
{
public:
    StorageTantivyIterpreter(Context & context_, const TiCIScan & tici_scan_, size_t max_streams_)
        : context(context_)
        , storage(std::make_unique<StorageTantivy>(context_, tici_scan_))
        , max_streams(max_streams_)
        , tmt(context.getTMTContext())
        , log(Logger::get(context.getDAGContext()->log ? context.getDAGContext()->log->identifier() : ""))
        , tici_scan(tici_scan_)
    {}

    void execute(PipelineExecutorContext & exec_context, PipelineExecGroupBuilder & group_builder);

    std::vector<RemoteRequest> buildRemoteRequests(ShardInfoList & remote_shard_infos);
    void buildRemoteExec(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        const std::vector<RemoteRequest> & remote_requests);
    CoprocessorReaderPtr buildCoprocessorReader(const std::vector<RemoteRequest> & remote_requests);
    std::vector<pingcap::coprocessor::CopTask> buildCopTasks(const std::vector<RemoteRequest> & remote_requests);

    // Members will be transferred to DAGQueryBlockInterpreter after execute
    std::unique_ptr<DAGExpressionAnalyzer> analyzer;

private:
    Context & context;
    std::unique_ptr<StorageTantivy> storage;
    size_t max_streams;

    TMTContext & tmt;
    LoggerPtr log;
    const TiCIScan tici_scan;
};
} // namespace DB
