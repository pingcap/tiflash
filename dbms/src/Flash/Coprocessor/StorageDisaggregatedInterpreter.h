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

#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/StorageDisaggregated.h>

namespace DB
{

// For TableScan in disaggregated tiflash mode,
// we convert it to ExchangeReceiver(executed in tiflash_compute node),
// and ExchangeSender + TableScan(executed in tiflash_storage node).
class StorageDisaggregatedInterpreter
{
public:
    StorageDisaggregatedInterpreter(
        Context & context_,
        const TiDBTableScan & table_scan_,
        const FilterConditions & filter_conditions_,
        size_t max_streams_)
        : context(context_)
        , storage(std::make_unique<StorageDisaggregated>(context_, table_scan_, filter_conditions_))
        , max_streams(max_streams_)
    {}

    void execute(DAGPipeline & pipeline)
    {
        auto stage = QueryProcessingStage::Enum::FetchColumns;
        pipeline.streams = storage->read(Names(), SelectQueryInfo(), context, stage, 0, max_streams);
    }

    void execute(PipelineExecutorContext & exec_context, PipelineExecGroupBuilder & group_builder)
    {
        storage->read(exec_context, group_builder, Names(), SelectQueryInfo(), context, 0, max_streams);
    }

private:
    Context & context;
    std::unique_ptr<StorageDisaggregated> storage;
    size_t max_streams;
};
} // namespace DB
