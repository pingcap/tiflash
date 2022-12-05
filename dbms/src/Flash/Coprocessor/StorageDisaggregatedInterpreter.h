// Copyright 2022 PingCAP, Ltd.
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

#include <Common/Logger.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/PushDownFilter.h>
#include <Interpreters/Context.h>
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
            const PushDownFilter & push_down_filter_,
            size_t max_streams_)
        : context(context_)
        , table_scan(table_scan_)
        , push_down_filter(push_down_filter_)
        , log(Logger::get(context_.getDAGContext()->log ? context_.getDAGContext()->log->identifier() : ""))
        , max_streams(max_streams_) {}

    void execute(DAGPipeline & pipeline);
    std::vector<RemoteRequest> buildRemoteRequests();
    // Members will be transferred to DAGQueryBlockInterpreter after execute
    std::unique_ptr<DAGExpressionAnalyzer> analyzer;
private:
    void pushDownFilter(DAGPipeline & pipeline, std::shared_ptr<ExchangeReceiver> exchange_receiver);

    Context & context;
    const TiDBTableScan & table_scan;
    const PushDownFilter & push_down_filter;
    LoggerPtr log;
    size_t max_streams;
};
} // namespace DB
