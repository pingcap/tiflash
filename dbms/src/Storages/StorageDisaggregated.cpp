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

#include <DataStreams/ExpressionBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Interpreters/Context.h>
#include <Operators/ExpressionTransformOp.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/S3/S3Common.h>
#include <Storages/StorageDisaggregated.h>
#include <kvproto/kvrpcpb.pb.h>


namespace DB
{
StorageDisaggregated::StorageDisaggregated(
    Context & context_,
    const TiDBTableScan & table_scan_,
    const FilterConditions & filter_conditions_)
    : IStorage()
    , context(context_)
    , table_scan(table_scan_)
    , log(Logger::get(context_.getDAGContext()->log ? context_.getDAGContext()->log->identifier() : ""))
    , sender_target_mpp_task_id(context_.getDAGContext()->getMPPTaskMeta())
    , filter_conditions(filter_conditions_)
{}

BlockInputStreams StorageDisaggregated::read(
    const Names &,
    const SelectQueryInfo & /*query_info*/,
    const Context & db_context,
    QueryProcessingStage::Enum &,
    size_t,
    unsigned num_streams)
{
    RUNTIME_CHECK_MSG(S3::ClientFactory::instance().isEnabled(), "storage disaggregated mode must with S3.");
    return readThroughS3(db_context, num_streams);
}

void StorageDisaggregated::read(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    const Context & db_context,
    size_t /*max_block_size*/,
    unsigned num_streams)
{
    RUNTIME_CHECK_MSG(S3::ClientFactory::instance().isEnabled(), "storage disaggregated mode must with S3.");
    return readThroughS3(exec_context, group_builder, db_context, num_streams);
}

std::tuple<std::vector<StorageDisaggregated::RemoteTableRange>, UInt64> StorageDisaggregated::buildRemoteTableRanges()
{
    std::unordered_map<TableID, RegionRetryList> all_remote_regions;
    UInt64 region_num = 0;
    for (auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & table_regions_info = context.getDAGContext()->getTableRegionsInfoByTableID(physical_table_id);

        RUNTIME_CHECK_MSG(
            table_regions_info.local_regions.empty(),
            "in disaggregated_compute_mode, local_regions should be empty");
        region_num += table_regions_info.remote_regions.size();
        for (const auto & reg : table_regions_info.remote_regions)
            all_remote_regions[physical_table_id].emplace_back(std::cref(reg));
    }

    std::vector<RemoteTableRange> remote_table_ranges;
    for (auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & remote_regions = all_remote_regions[physical_table_id];
        if (remote_regions.empty())
            continue;
        auto key_ranges = RemoteRequest::buildKeyRanges(remote_regions);
        remote_table_ranges.emplace_back(RemoteTableRange{physical_table_id, key_ranges});
    }
    return std::make_tuple(std::move(remote_table_ranges), region_num);
}

std::vector<pingcap::coprocessor::BatchCopTask> StorageDisaggregated::buildBatchCopTasks(
    const std::vector<RemoteTableRange> & remote_table_ranges,
    const pingcap::kv::LabelFilter & label_filter)
{
    std::vector<TableID> physical_table_ids;
    physical_table_ids.reserve(remote_table_ranges.size());
    std::vector<pingcap::coprocessor::KeyRanges> ranges_for_each_physical_table;
    ranges_for_each_physical_table.reserve(remote_table_ranges.size());
    for (const auto & remote_table_range : remote_table_ranges)
    {
        physical_table_ids.emplace_back(remote_table_range.first);
        ranges_for_each_physical_table.emplace_back(remote_table_range.second);
    }

    pingcap::kv::Cluster * cluster = context.getTMTContext().getKVCluster();
    pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
    pingcap::kv::StoreType store_type = pingcap::kv::StoreType::TiFlash;
    auto batch_cop_tasks = pingcap::coprocessor::buildBatchCopTasks(
        bo,
        cluster,
        /*is_mpp=*/true,
        table_scan.isPartitionTableScan(),
        physical_table_ids,
        ranges_for_each_physical_table,
        context.getStoreIdBlockList(),
        store_type,
        label_filter,
        &Poco::Logger::get("pingcap/coprocessor"));
    LOG_DEBUG(log, "batch cop tasks(nums: {}) build finish for tiflash_storage node", batch_cop_tasks.size());
    return batch_cop_tasks;
}

tipb::Executor StorageDisaggregated::buildTableScanTiPB()
{
    // TODO: For now, to avoid versions of tiflash_compute nodes and tiflash_storage being different,
    // disable filter push down to avoid unsupported expression in tiflash_storage.
    // Uncomment this when we are sure versions are same.
    // executor = filter_conditions.constructSelectionForRemoteRead(dag_req.mutable_root_executor());

    tipb::Executor ts_exec;
    ts_exec.set_tp(tipb::ExecType::TypeTableScan);
    ts_exec.set_executor_id(table_scan.getTableScanExecutorID());

    // In disaggregated mode, use DAGRequest sent from TiDB directly, so no need to rely on SchemaSyncer.
    if (table_scan.isPartitionTableScan())
    {
        ts_exec.set_tp(tipb::ExecType::TypePartitionTableScan);
        auto * mutable_partition_table_scan = ts_exec.mutable_partition_table_scan();
        *mutable_partition_table_scan = table_scan.getTableScanPB()->partition_table_scan();
    }
    else
    {
        ts_exec.set_tp(tipb::ExecType::TypeTableScan);
        auto * mutable_table_scan = ts_exec.mutable_tbl_scan();
        *mutable_table_scan = table_scan.getTableScanPB()->tbl_scan();
    }
    return ts_exec;
}

void StorageDisaggregated::filterConditions(DAGExpressionAnalyzer & analyzer, DAGPipeline & pipeline)
{
    if (filter_conditions.hasValue())
    {
        // No need to cast, because already done by tiflash_storage node.
        ::DB::executePushedDownFilter(filter_conditions, analyzer, log, pipeline);

        auto & profile_streams = context.getDAGContext()->getProfileStreamsMap()[filter_conditions.executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}

void StorageDisaggregated::filterConditions(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    DAGExpressionAnalyzer & analyzer)
{
    if (filter_conditions.hasValue())
    {
        ::DB::executePushedDownFilter(exec_context, group_builder, filter_conditions, analyzer, log);
        context.getDAGContext()->addOperatorProfileInfos(
            filter_conditions.executor_id,
            group_builder.getCurProfileInfos());
    }
}

ExpressionActionsPtr StorageDisaggregated::getExtraCastExpr(DAGExpressionAnalyzer & analyzer)
{
    // If the column is not in the columns of pushed down filter, append a cast to the column.
    std::vector<UInt8> may_need_add_cast_column;
    may_need_add_cast_column.reserve(table_scan.getColumnSize());
    std::unordered_set<ColumnID> filter_col_id_set;
    for (const auto & expr : table_scan.getPushedDownFilters())
    {
        getColumnIDsFromExpr(expr, table_scan.getColumns(), filter_col_id_set);
    }
    for (const auto & col : table_scan.getColumns())
        may_need_add_cast_column.push_back(
            !col.hasGeneratedColumnFlag() && !filter_col_id_set.contains(col.id) && col.id != -1);
    bool has_need_cast_column = std::find(may_need_add_cast_column.begin(), may_need_add_cast_column.end(), true)
        != may_need_add_cast_column.end();
    ExpressionActionsChain chain;
    if (has_need_cast_column && analyzer.appendExtraCastsAfterTS(chain, may_need_add_cast_column, table_scan))
    {
        ExpressionActionsPtr extra_cast = chain.getLastActions();
        chain.finalize();
        chain.clear();
        return extra_cast;
    }
    else
    {
        return nullptr;
    }
}

void StorageDisaggregated::extraCast(DAGExpressionAnalyzer & analyzer, DAGPipeline & pipeline)
{
    if (auto extra_cast = getExtraCastExpr(analyzer); extra_cast)
    {
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, log->identifier());
            stream->setExtraInfo("cast after local tableScan");
        });
    }
}

void StorageDisaggregated::extraCast(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    DAGExpressionAnalyzer & analyzer)
{
    if (auto extra_cast = getExtraCastExpr(analyzer); extra_cast)
    {
        group_builder.transform([&](auto & builder) {
            builder.appendTransformOp(
                std::make_unique<ExpressionTransformOp>(exec_context, log->identifier(), extra_cast));
        });
    }
}

} // namespace DB
