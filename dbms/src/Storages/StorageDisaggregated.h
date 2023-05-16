// Copyright 2023 PingCAP, Ltd.
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
#include <Flash/Mpp/MPPTaskId.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask_fwd.h>
#include <Storages/IStorage.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/mpp.pb.h>
#include <pingcap/kv/RegionCache.h>
#include <tipb/executor.pb.h>
#pragma GCC diagnostic pop

namespace DB
{
class DAGContext;
class ExchangeReceiver;
namespace DM
{
struct ColumnDefine;
using ColumnDefines = std::vector<ColumnDefine>;
using ColumnDefinesPtr = std::shared_ptr<ColumnDefines>;
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;
} // namespace DM

using RequestAndRegionIDs = std::tuple<std::shared_ptr<::mpp::DispatchTaskRequest>, std::vector<::pingcap::kv::RegionVerID>, uint64_t>;

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
        const FilterConditions & filter_conditions_);

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

    RequestAndRegionIDs buildDispatchMPPTaskRequest(const pingcap::coprocessor::BatchCopTask & batch_cop_task);

    // To help find exec summary of ExchangeSender in tiflash_storage and merge it into TableScan's exec summary.
    static const String ExecIDPrefixForTiFlashStorageSender;
    // Members will be transferred to DAGQueryBlockInterpreter after execute
    std::unique_ptr<DAGExpressionAnalyzer> analyzer;

private:
    // helper functions for building the task read from a shared remote storage system (e.g. S3)
    BlockInputStreams readThroughS3(
        const Context & db_context,
        const SelectQueryInfo & query_info,
        unsigned num_streams);
    /// helper functions for building the task fetch all data from write node through MPP exchange sender/receiver
    BlockInputStreams readThroughExchange(unsigned num_streams);
    DM::RNRemoteReadTaskPtr buildDisaggTasks(
        const Context & db_context,
        const DM::ScanContextPtr & scan_context,
        const std::vector<pingcap::coprocessor::BatchCopTask> & batch_cop_tasks);
    void buildDisaggTask(
        const Context & db_context,
        const DM::ScanContextPtr & scan_context,
        const pingcap::coprocessor::BatchCopTask & batch_cop_task,
        std::vector<DM::RNRemoteStoreReadTaskPtr> & store_read_tasks,
        std::mutex & store_read_tasks_lock);
    std::shared_ptr<disaggregated::EstablishDisaggTaskRequest>
    buildDisaggTaskForNode(
        const Context & db_context,
        const pingcap::coprocessor::BatchCopTask & batch_cop_task);
    DM::RSOperatorPtr buildRSOperator(
        const Context & db_context,
        const DM::ColumnDefinesPtr & columns_to_read);
    void buildRemoteSegmentInputStreams(
        const Context & db_context,
        const DM::RNRemoteReadTaskPtr & remote_read_tasks,
        const SelectQueryInfo & query_info,
        size_t num_streams,
        DAGPipeline & pipeline);

private:
    using RemoteTableRange = std::pair<Int64, pingcap::coprocessor::KeyRanges>;
    std::vector<RemoteTableRange> buildRemoteTableRanges();
    std::vector<pingcap::coprocessor::BatchCopTask> buildBatchCopTasks(
        const std::vector<RemoteTableRange> & remote_table_ranges,
        const pingcap::kv::LabelFilter & label_filter);
    void buildReceiverStreams(const std::vector<RequestAndRegionIDs> & dispatch_reqs, unsigned num_streams, DAGPipeline & pipeline);
    void filterConditions(DAGExpressionAnalyzer & analyzer, DAGPipeline & pipeline);
    void extraCast(DAGExpressionAnalyzer & analyzer, DAGPipeline & pipeline);
    tipb::Executor buildTableScanTiPB();

    Context & context;
    const TiDBTableScan & table_scan;
    LoggerPtr log;
    MPPTaskId sender_target_mpp_task_id;
    const FilterConditions & filter_conditions;

    std::shared_ptr<ExchangeReceiver> exchange_receiver;
};
} // namespace DB
