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
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <tipb/executor.pb.h>
#pragma GCC diagnostic pop

namespace DB
{

// Naive implementation of StorageDisaggregated, all region data will be transferred by GRPC,
// rewrite this when local cache is supported.
// Naive StorageDisaggregated will convert TableScan to ExchangeReceiver(executed in tiflash_compute node),
// and ExchangeSender + TableScan(executed in tiflash_storage node).
class StorageDisaggregated : public IStorage
{
public:
    StorageDisaggregated(
        Context & context_,
        const TiDBTableScan & table_scan_,
        const PushDownFilter & push_down_filter_)
        : IStorage()
        , context(context_)
        , table_scan(table_scan_)
        , log(Logger::get(context_.getDAGContext()->log ? context_.getDAGContext()->log->identifier() : ""))
        , sender_target_task_start_ts(context_.getDAGContext()->getMPPTaskMeta().start_ts())
        , sender_target_task_task_id(context_.getDAGContext()->getMPPTaskMeta().task_id())
        , push_down_filter(push_down_filter_)
    {}

    std::string getName() const override
    {
        return "StorageDisaggregated";
    }

    std::string getTableName() const override
    {
        return "StorageDisaggregated_" + table_scan.getTableScanExecutorID();
    }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    using RequestAndRegionIDs = std::tuple<std::shared_ptr<::mpp::DispatchTaskRequest>, std::vector<::pingcap::kv::RegionVerID>, uint64_t>;
    RequestAndRegionIDs buildDispatchMPPTaskRequest(const pingcap::coprocessor::BatchCopTask & batch_cop_task);

    // To help find exec summary of ExchangeSender in tiflash_storage and merge it into TableScan's exec summary.
    static const String ExecIDPrefixForTiFlashStorageSender;
    // Members will be transferred to DAGQueryBlockInterpreter after execute
    std::unique_ptr<DAGExpressionAnalyzer> analyzer;

private:
    using RemoteTableRange = std::pair<Int64, pingcap::coprocessor::KeyRanges>;
    std::vector<RemoteTableRange> buildRemoteTableRanges();
    std::vector<pingcap::coprocessor::BatchCopTask> buildBatchCopTasks(const std::vector<RemoteTableRange> & remote_table_ranges);
    void buildReceiverStreams(const std::vector<RequestAndRegionIDs> & dispatch_reqs, unsigned num_streams, DAGPipeline & pipeline);
    void pushDownFilter(DAGPipeline & pipeline);
    tipb::Executor buildTableScanTiPB();

    Context & context;
    const TiDBTableScan & table_scan;
    LoggerPtr log;
    uint64_t sender_target_task_start_ts;
    int64_t sender_target_task_task_id;
    const PushDownFilter & push_down_filter;

    std::shared_ptr<ExchangeReceiver> exchange_receiver;
};
} // namespace DB
