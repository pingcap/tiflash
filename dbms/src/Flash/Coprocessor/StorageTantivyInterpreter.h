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

#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Interpreters/Context.h>
#include <Storages/StorageTantivy.h>

#include <algorithm>

#include "Core/Names.h"
#include "Core/NamesAndTypes.h"
#include "Flash/Coprocessor/DAGContext.h"
#include "Flash/Coprocessor/GenSchemaAndColumn.h"
#include "Flash/Coprocessor/TiCIScan.h"
#include "Storages/SelectQueryInfo.h"

namespace DB
{

class StorageTantivyIterpreter
{
public:
    StorageTantivyIterpreter(
        Context & context_,
        const TiCIScan & tici_scan_,
        const FilterConditions & filter_conditions_,
        size_t max_streams_)
        : context(context_)
        , storage(std::make_unique<StorageTantivy>(context_, tici_scan_))
        , max_streams(max_streams_)
        , log(Logger::get(context.getDAGContext()->log ? context.getDAGContext()->log->identifier() : ""))
        , tici_scan(tici_scan_)
        , filter_conditions(filter_conditions_)
    {}

    void execute(DAGPipeline &) {}

    void execute(PipelineExecutorContext & exec_context, PipelineExecGroupBuilder & group_builder)
    {
        storage->read(exec_context, group_builder, Names(), query_info, context, 0, 0);
    }

    // Members will be transferred to DAGQueryBlockInterpreter after execute
    std::unique_ptr<DAGExpressionAnalyzer> analyzer;

private:
    void generateSelectQueryInfos();
    Context & context;
    std::unique_ptr<StorageTantivy> storage;
    [[maybe_unused]] size_t max_streams;

    LoggerPtr log;
    const TiCIScan tici_scan;
    [[maybe_unused]] const FilterConditions & filter_conditions;
    SelectQueryInfo query_info;
};
} // namespace DB
