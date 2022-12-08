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

#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Storages/StorageDisaggregated.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{
const String StorageDisaggregated::ExecIDPrefixForTiFlashStorageSender = "exec_id_disaggregated_tiflash_storage_sender";

BlockInputStreams StorageDisaggregated::read(
    const Names &,
    const SelectQueryInfo &,
    const Context &,
    QueryProcessingStage::Enum &,
    size_t,
    unsigned num_streams)
{
    auto remote_table_ranges = buildRemoteTableRanges();

    auto batch_cop_tasks = buildBatchCopTasks(remote_table_ranges);
    RUNTIME_CHECK(!batch_cop_tasks.empty());

    std::vector<RequestAndRegionIDs> dispatch_reqs;
    dispatch_reqs.reserve(batch_cop_tasks.size());
    for (const auto & batch_cop_task : batch_cop_tasks)
        dispatch_reqs.emplace_back(buildDispatchMPPTaskRequest(batch_cop_task));

    DAGPipeline pipeline;
    buildReceiverStreams(dispatch_reqs, num_streams, pipeline);
    pushDownFilter(pipeline);

    return pipeline.streams;
}

std::vector<StorageDisaggregated::RemoteTableRange> StorageDisaggregated::buildRemoteTableRanges()
{
    std::unordered_map<Int64, RegionRetryList> all_remote_regions;
    for (auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & table_regions_info = context.getDAGContext()->getTableRegionsInfoByTableID(physical_table_id);

        RUNTIME_CHECK_MSG(table_regions_info.local_regions.empty(), "in disaggregated_compute_mode, local_regions should be empty");
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
    return remote_table_ranges;
}

std::vector<pingcap::coprocessor::BatchCopTask> StorageDisaggregated::buildBatchCopTasks(const std::vector<RemoteTableRange> & remote_table_ranges)
{
    std::vector<Int64> physical_table_ids;
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
    auto batch_cop_tasks = pingcap::coprocessor::buildBatchCopTasks(bo, cluster, table_scan.isPartitionTableScan(), physical_table_ids, ranges_for_each_physical_table, store_type, &Poco::Logger::get("pingcap/coprocessor"));
    LOG_DEBUG(log, "batch cop tasks(nums: {}) build finish for tiflash_storage node", batch_cop_tasks.size());
    return batch_cop_tasks;
}

StorageDisaggregated::RequestAndRegionIDs StorageDisaggregated::buildDispatchMPPTaskRequest(
    const pingcap::coprocessor::BatchCopTask & batch_cop_task)
{
    // For error handling, need to record region_ids and store_id to invalidate cache.
    std::vector<pingcap::kv::RegionVerID> region_ids;
    auto dispatch_req = std::make_shared<::mpp::DispatchTaskRequest>();
    ::mpp::TaskMeta * dispatch_req_meta = dispatch_req->mutable_meta();
    dispatch_req_meta->set_start_ts(sender_target_task_start_ts);
    dispatch_req_meta->set_task_id(sender_target_task_task_id);
    dispatch_req_meta->set_address(batch_cop_task.store_addr);
    const auto & settings = context.getSettings();
    dispatch_req->set_timeout(60);
    dispatch_req->set_schema_ver(settings.schema_version);
    RUNTIME_CHECK_MSG(batch_cop_task.region_infos.empty() != batch_cop_task.table_regions.empty(),
                      "region_infos and table_regions should not exist at the same time, single table region info: {}, partition table region info: {}",
                      batch_cop_task.region_infos.size(),
                      batch_cop_task.table_regions.size());
    if (!batch_cop_task.region_infos.empty())
    {
        // For non-partition table.
        for (const auto & region_info : batch_cop_task.region_infos)
        {
            region_ids.push_back(region_info.region_id);
            auto * region = dispatch_req->add_regions();
            region->set_region_id(region_info.region_id.id);
            region->mutable_region_epoch()->set_version(region_info.region_id.ver);
            region->mutable_region_epoch()->set_conf_ver(region_info.region_id.conf_ver);
            for (const auto & key_range : region_info.ranges)
            {
                key_range.setKeyRange(region->add_ranges());
            }
        }
    }
    else
    {
        // For partition table.
        for (const auto & table_region : batch_cop_task.table_regions)
        {
            auto * req_table_region = dispatch_req->add_table_regions();
            req_table_region->set_physical_table_id(table_region.physical_table_id);
            auto * region = req_table_region->add_regions();
            for (const auto & region_info : table_region.region_infos)
            {
                region_ids.push_back(region_info.region_id);
                region->set_region_id(region_info.region_id.id);
                region->mutable_region_epoch()->set_version(region_info.region_id.ver);
                region->mutable_region_epoch()->set_conf_ver(region_info.region_id.conf_ver);
                for (const auto & key_range : region_info.ranges)
                {
                    key_range.setKeyRange(region->add_ranges());
                }
            }
        }
    }

    const auto & sender_target_task_meta = context.getDAGContext()->getMPPTaskMeta();
    const auto * dag_req = context.getDAGContext()->dag_request;
    tipb::DAGRequest sender_dag_req;
    sender_dag_req.set_time_zone_name(dag_req->time_zone_name());
    sender_dag_req.set_time_zone_offset(dag_req->time_zone_offset());
    // TODO: We have exec summaries bug for now, remote exec summary will not be merged.
    sender_dag_req.set_collect_execution_summaries(false);
    sender_dag_req.set_flags(dag_req->flags());
    sender_dag_req.set_encode_type(tipb::EncodeType::TypeCHBlock);
    sender_dag_req.set_force_encode_type(true);
    const auto & column_infos = table_scan.getColumns();
    for (auto off = 0; off < column_infos.size(); ++off)
    {
        sender_dag_req.add_output_offsets(off);
    }

    tipb::Executor * executor = sender_dag_req.mutable_root_executor();
    executor->set_tp(tipb::ExecType::TypeExchangeSender);
    // Exec summary of ExchangeSender will be merged into TableScan.
    executor->set_executor_id(fmt::format("{}_{}_{}",
                                          ExecIDPrefixForTiFlashStorageSender,
                                          sender_target_task_start_ts,
                                          sender_target_task_task_id));

    tipb::ExchangeSender * sender = executor->mutable_exchange_sender();
    sender->set_tp(tipb::ExchangeType::PassThrough);
    sender->add_encoded_task_meta(sender_target_task_meta.SerializeAsString());
    auto * child = sender->mutable_child();
    child->CopyFrom(buildTableScanTiPB());
    for (const auto & column_info : column_infos)
    {
        auto * field_type = sender->add_all_field_types();
        auto tidb_column_info = TiDB::toTiDBColumnInfo(column_info);
        *field_type = columnInfoToFieldType(tidb_column_info);
    }
    // Ignore sender.PartitionKeys and sender.Types because it's a PassThrough sender.

    dispatch_req->set_encoded_plan(sender_dag_req.SerializeAsString());
    return StorageDisaggregated::RequestAndRegionIDs{dispatch_req, region_ids, batch_cop_task.store_id};
}

tipb::Executor StorageDisaggregated::buildTableScanTiPB()
{
    // TODO: For now, to avoid versions of tiflash_compute nodes and tiflash_storage being different,
    // disable filter push down to avoid unsupported expression in tiflash_storage.
    // Uncomment this when we are sure versions are same.
    // executor = push_down_filter.constructSelectionForRemoteRead(dag_req.mutable_root_executor());

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

void StorageDisaggregated::buildReceiverStreams(const std::vector<RequestAndRegionIDs> & dispatch_reqs, unsigned num_streams, DAGPipeline & pipeline)
{
    tipb::ExchangeReceiver receiver;
    for (const auto & dispatch_req : dispatch_reqs)
    {
        const ::mpp::TaskMeta & sender_task_meta = std::get<0>(dispatch_req)->meta();
        receiver.add_encoded_task_meta(sender_task_meta.SerializeAsString());
    }

    const auto & column_infos = table_scan.getColumns();
    for (const auto & column_info : column_infos)
    {
        auto * field_type = receiver.add_field_types();
        auto tidb_column_info = TiDB::toTiDBColumnInfo(column_info);
        *field_type = columnInfoToFieldType(tidb_column_info);
    }

    // ExchangeSender just use TableScan's executor_id, so exec summary will be merged to TableScan.
    const auto & sender_target_task_meta = context.getDAGContext()->getMPPTaskMeta();
    const String executor_id = table_scan.getTableScanExecutorID();

    exchange_receiver = std::make_shared<ExchangeReceiver>(
        std::make_shared<GRPCReceiverContext>(
            receiver,
            sender_target_task_meta,
            context.getTMTContext().getKVCluster(),
            context.getTMTContext().getMPPTaskManager(),
            context.getSettingsRef().enable_local_tunnel,
            context.getSettingsRef().enable_async_grpc_client),
        receiver.encoded_task_meta_size(),
        num_streams,
        log->identifier(),
        executor_id,
        /*fine_grained_shuffle_stream_count=*/0,
        dispatch_reqs);

    // MPPTask::receiver_set will record this ExchangeReceiver, so can cancel it in ReceiverSet::cancel().
    context.getDAGContext()->setDisaggregatedComputeExchangeReceiver(executor_id, exchange_receiver);

    // We can use PhysicalExchange::transform() to build InputStream after
    // DAGQueryBlockInterpreter is deprecated to avoid duplicated code here.
    const String extra_info = "disaggregated compute node exchange receiver";
    for (size_t i = 0; i < num_streams; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(exchange_receiver,
                                                                                   log->identifier(),
                                                                                   executor_id,
                                                                                   /*stream_id=*/0);
        stream->setExtraInfo(extra_info);
        pipeline.streams.push_back(stream);
    }

    auto & table_scan_io_input_streams = context.getDAGContext()->getInBoundIOInputStreamsMap()[table_scan.getTableScanExecutorID()];
    auto & profile_streams = context.getDAGContext()->getProfileStreamsMap()[table_scan.getTableScanExecutorID()];
    pipeline.transform([&](auto & stream) {
        table_scan_io_input_streams.push_back(stream);
        profile_streams.push_back(stream);
    });
}

void StorageDisaggregated::pushDownFilter(DAGPipeline & pipeline)
{
    NamesAndTypes source_columns = genNamesAndTypesForExchangeReceiver(table_scan);
    const auto & receiver_dag_schema = exchange_receiver->getOutputSchema();
    assert(receiver_dag_schema.size() == source_columns.size());

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    if (push_down_filter.hasValue())
    {
        // No need to cast, because already done by tiflash_storage node.
        ::DB::executePushedDownFilter(/*remote_read_streams_start_index=*/pipeline.streams.size(), push_down_filter, *analyzer, log, pipeline);

        auto & profile_streams = context.getDAGContext()->getProfileStreamsMap()[push_down_filter.executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}
} // namespace DB
