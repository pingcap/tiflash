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

#include <Common/Logger.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Flash/Mpp/MPPTaskId.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/DeltaMerge/ColumnDefine_fwd.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/RNWorkers_fwd.h>
#include <Storages/DeltaMerge/SegmentReadTask.h>
#include <Storages/DeltaMerge/SegmentReadTaskPool.h>
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

namespace DM
{
class RSOperator;
using RSOperatorPtr = std::shared_ptr<RSOperator>;
} // namespace DM

class StorageDisaggregated : public IStorage
{
public:
    StorageDisaggregated(
        Context & context_,
        const TiDBTableScan & table_scan_,
        const FilterConditions & filter_conditions_);

    std::string getName() const override { return "StorageDisaggregated"; }

    std::string getTableName() const override { return "StorageDisaggregated_" + table_scan.getTableScanExecutorID(); }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void read(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        const Names & /*column_names*/,
        const SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        size_t /*max_block_size*/,
        unsigned num_streams) override;

private:
    // helper functions for building the task read from a shared remote storage system (e.g. S3)
    BlockInputStreams readThroughS3(const Context & db_context, unsigned num_streams);
    void readThroughS3(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        const Context & db_context,
        unsigned num_streams);

    DM::SegmentReadTasks buildReadTaskWithBackoff(const Context & db_context);

    DM::SegmentReadTasks buildReadTask(const Context & db_context, const DM::ScanContextPtr & scan_context);

    void buildReadTaskForWriteNode(
        const Context & db_context,
        const DM::ScanContextPtr & scan_context,
        const pingcap::coprocessor::BatchCopTask & batch_cop_task,
        std::mutex & output_lock,
        DM::SegmentReadTasks & output_seg_tasks);

    void buildReadTaskForWriteNodeTable(
        const Context & db_context,
        const DM::ScanContextPtr & scan_context,
        const DM::DisaggTaskId & snapshot_id,
        StoreID store_id,
        const String & store_address,
        const String & serialized_physical_table,
        std::mutex & output_lock,
        DM::SegmentReadTasks & output_seg_tasks);

    std::shared_ptr<disaggregated::EstablishDisaggTaskRequest> buildEstablishDisaggTaskReq(
        const Context & db_context,
        const pingcap::coprocessor::BatchCopTask & batch_cop_task);
    DM::RSOperatorPtr buildRSOperator(const Context & db_context, const DM::ColumnDefinesPtr & columns_to_read);
    std::variant<DM::Remote::RNWorkersPtr, DM::SegmentReadTaskPoolPtr> packSegmentReadTasks(
        const Context & db_context,
        DM::SegmentReadTasks && read_tasks,
        const DM::ColumnDefinesPtr & column_defines,
        size_t num_streams,
        int extra_table_id_index);
    void buildRemoteSegmentInputStreams(
        const Context & db_context,
        DM::SegmentReadTasks && read_tasks,
        size_t num_streams,
        DAGPipeline & pipeline);
    void buildRemoteSegmentSourceOps(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        const Context & db_context,
        DM::SegmentReadTasks && read_tasks,
        size_t num_streams);

    using RemoteTableRange = std::pair<TableID, pingcap::coprocessor::KeyRanges>;
    std::vector<RemoteTableRange> buildRemoteTableRanges();
    std::vector<pingcap::coprocessor::BatchCopTask> buildBatchCopTasks(
        const std::vector<RemoteTableRange> & remote_table_ranges,
        const pingcap::kv::LabelFilter & label_filter);

    void filterConditions(DAGExpressionAnalyzer & analyzer, DAGPipeline & pipeline);
    void filterConditions(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        DAGExpressionAnalyzer & analyzer);
    ExpressionActionsPtr getExtraCastExpr(DAGExpressionAnalyzer & analyzer);
    void extraCast(DAGExpressionAnalyzer & analyzer, DAGPipeline & pipeline);
    void extraCast(
        PipelineExecutorContext & exec_context,
        PipelineExecGroupBuilder & group_builder,
        DAGExpressionAnalyzer & analyzer);
    tipb::Executor buildTableScanTiPB();

private:
    Context & context;
    const TiDBTableScan & table_scan;
    LoggerPtr log;
    MPPTaskId sender_target_mpp_task_id;
    const FilterConditions & filter_conditions;

    std::unique_ptr<DAGExpressionAnalyzer> analyzer;
};
} // namespace DB
