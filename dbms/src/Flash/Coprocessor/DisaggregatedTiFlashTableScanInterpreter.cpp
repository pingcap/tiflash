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

#include <Common/ThreadManager.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DisaggregatedTiFlashTableScanInterpreter.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{
const String DisaggregatedTiFlashTableScanInterpreter::ExecIDPrefixForTiFlashStorageSender = "exec_id_disaggregated_tiflash_storage_sender";

void DisaggregatedTiFlashTableScanInterpreter::execute(DAGPipeline & pipeline)
{
    buildRemoteRequests();

    auto dispatch_reqs = buildAndDispatchMPPTaskRequests();
    buildReceiverStreams(dispatch_reqs, pipeline);

    pushDownFilter(pipeline);
}

void DisaggregatedTiFlashTableScanInterpreter::buildRemoteRequests()
{
    std::unordered_map<Int64, RegionRetryList> all_remote_regions;
    for (auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & table_regions_info = context.getDAGContext()->getTableRegionsInfoByTableID(physical_table_id);

        RUNTIME_CHECK_MSG(table_regions_info.local_regions.empty(), "in disaggregated_compute_mode, local_regions should be empty");
        for (const auto & reg : table_regions_info.remote_regions)
            all_remote_regions[physical_table_id].emplace_back(std::cref(reg));
    }

    for (auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & remote_regions = all_remote_regions[physical_table_id];
        if (remote_regions.empty())
            continue;
        remote_requests.push_back(RemoteRequest::build(
                    remote_regions,
                    *context.getDAGContext(),
                    table_scan,
                    TiDB::TableInfo{},
                    push_down_filter,
                    log,
                    physical_table_id,
                    /*is_disaggregated_compute_mode=*/true));
    }
}

std::vector<pingcap::coprocessor::BatchCopTask> DisaggregatedTiFlashTableScanInterpreter::buildBatchCopTasks()
{
    std::vector<Int64> physical_table_ids;
    physical_table_ids.reserve(remote_requests.size());
    std::vector<pingcap::coprocessor::KeyRanges> ranges_for_each_physical_table;
    ranges_for_each_physical_table.reserve(remote_requests.size());
    for (const auto & remote_request : remote_requests)
    {
        physical_table_ids.emplace_back(remote_request.physical_table_id);
        ranges_for_each_physical_table.emplace_back(remote_request.key_ranges);
    }

    pingcap::kv::Cluster * cluster = context.getTMTContext().getKVCluster();
    pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
    pingcap::kv::StoreType store_type = pingcap::kv::StoreType::TiFlash;
    auto batch_cop_tasks = pingcap::coprocessor::buildBatchCopTasks(bo, cluster, table_scan.isPartitionTableScan(), physical_table_ids, ranges_for_each_physical_table, store_type, &Poco::Logger::get("pingcap/coprocessor"));
    LOG_DEBUG(log, "batch cop tasks(nums: {}) build finish for tiflash_storage node", batch_cop_tasks.size());
    return batch_cop_tasks;
}

std::shared_ptr<::mpp::DispatchTaskRequest> DisaggregatedTiFlashTableScanInterpreter::buildDispatchMPPTaskRequest(const pingcap::coprocessor::BatchCopTask & batch_cop_task)
{
    const auto & sender_target_task_meta = context.getDAGContext()->getMPPTaskMeta();
    const auto * dag_req = context.getDAGContext()->dag_request;

    auto dispatch_req = std::make_shared<::mpp::DispatchTaskRequest>();
    ::mpp::TaskMeta * dispatch_req_meta = dispatch_req->mutable_meta();
    dispatch_req_meta->set_start_ts(sender_target_task_start_ts);
    dispatch_req_meta->set_task_id(sender_target_task_task_id);
    dispatch_req_meta->set_address(batch_cop_task.store_addr);
    const auto & settings = context.getSettings();
    dispatch_req->set_timeout(settings.mpp_task_timeout);
    dispatch_req->set_schema_ver(settings.read_tso);
    RUNTIME_CHECK_MSG(batch_cop_task.region_infos.empty() != batch_cop_task.table_regions.empty(),
            "region_infos and table_regions should not exist at the same time, single table region info: {}, partition table region info: {}",
            batch_cop_task.region_infos.size(), batch_cop_task.table_regions.size());
    if (!batch_cop_task.region_infos.empty())
    {
        // For non-partition table.
        for (const auto & region_info : batch_cop_task.region_infos)
        {
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

    tipb::DAGRequest sender_dag_req;
    sender_dag_req.set_time_zone_name(dag_req->time_zone_name());
    sender_dag_req.set_time_zone_offset(dag_req->time_zone_offset());
    sender_dag_req.set_collect_execution_summaries(true);
    sender_dag_req.set_flags(dag_req->flags());
    sender_dag_req.set_encode_type(tipb::EncodeType::TypeCHBlock);
    const auto & column_infos = table_scan.getColumns();
    for (auto off = 0; off < column_infos.size(); ++off)
    {
        sender_dag_req.add_output_offsets(off);
    }

    tipb::Executor * executor = sender_dag_req.mutable_root_executor();
    executor->set_tp(tipb::ExecType::TypeExchangeSender);
    // Exec summary of ExchangeSender will be merged into TableScan.
    executor->set_executor_id(fmt::format("{}_{}_{}",
                ExecIDPrefixForTiFlashStorageSender, sender_target_task_start_ts, sender_target_task_task_id));

    tipb::ExchangeSender * sender = executor->mutable_exchange_sender();
    sender->set_tp(tipb::ExchangeType::PassThrough);
    sender->add_encoded_task_meta(sender_target_task_meta.SerializeAsString());
    auto * child = sender->mutable_child();
    RUNTIME_CHECK(!remote_requests.empty());
    child->CopyFrom(remote_requests[0].dag_request.root_executor());
    for (const auto & column_info : column_infos)
    {
        auto * field_type = sender->add_all_field_types();
        auto tidb_column_info = TiDB::toTiDBColumnInfo(column_info);
        *field_type = columnInfoToFieldType(tidb_column_info);
    }
    // Ignore sender.PartitionKeys and sender.Types because it's a PassThrough sender.

    dispatch_req->set_encoded_plan(sender_dag_req.SerializeAsString());
    return dispatch_req;
}

std::vector<std::shared_ptr<::mpp::DispatchTaskRequest>> DisaggregatedTiFlashTableScanInterpreter::buildAndDispatchMPPTaskRequests()
{
    auto batch_cop_tasks = buildBatchCopTasks();
    std::vector<std::shared_ptr<::mpp::DispatchTaskRequest>> dispatch_reqs;
    dispatch_reqs.reserve(batch_cop_tasks.size());
    for (const auto & batch_cop_task : batch_cop_tasks)
        dispatch_reqs.emplace_back(buildDispatchMPPTaskRequest(batch_cop_task));

    std::shared_ptr<ThreadManager> thread_manager = newThreadManager();
    for (const auto & dispatch_req : dispatch_reqs)
    {
        LOG_DEBUG(log, "tiflash_compute node start to send MPPTask({})", dispatch_req->DebugString());
        thread_manager->schedule(/*propagate_memory_tracker=*/true, "", [dispatch_req, this] {
                pingcap::kv::RpcCall<mpp::DispatchTaskRequest> rpc_call(dispatch_req);
                this->context.getTMTContext().getKVCluster()->rpc_client->sendRequest(dispatch_req->meta().address(), rpc_call, /*timeout=*/60);
        });
    }

    thread_manager->wait();
    return dispatch_reqs;
}

void DisaggregatedTiFlashTableScanInterpreter::buildReceiverStreams(
        const std::vector<std::shared_ptr<::mpp::DispatchTaskRequest>> & dispatch_reqs, DAGPipeline & pipeline)
{
    tipb::ExchangeReceiver receiver;
    const auto & sender_target_task_meta = context.getDAGContext()->getMPPTaskMeta();
    for (const auto & dispatch_req : dispatch_reqs)
    {
        const ::mpp::TaskMeta & sender_task_meta = dispatch_req->meta();
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
            context.getMaxStreams(),
            log->identifier(),
            executor_id,
            /*fine_grained_shuffle_stream_count=*/0,
            /*is_tiflash_storage_receiver=*/true);

    // MPPTask::receiver_set will record this ExchangeReceiver, so can cancel it in ReceiverSet::cancel().
    context.getDAGContext()->setDisaggregatedComputeExchangeReceiver(executor_id, exchange_receiver);

    // We can use PhysicalExchange::transform() to build InputStream after
    // DAGQueryBlockInterpreter is deprecated to avoid duplicated code here.
    const String extra_info = "disaggregated compute node exchange receiver";
    for (size_t i = 0; i < max_streams; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(exchange_receiver,
                                                                                   log->identifier(),
                                                                                   executor_id,
                                                                                   /*stream_id=*/0);
        stream->setExtraInfo(extra_info);
        pipeline.streams.push_back(stream);
    }

    auto & table_scan_io_input_streams = context.getDAGContext()->getInBoundIOInputStreamsMap()[table_scan.getTableScanExecutorID()];
    pipeline.transform([&](auto & stream) { table_scan_io_input_streams.push_back(stream); });
}

void DisaggregatedTiFlashTableScanInterpreter::pushDownFilter(DAGPipeline & pipeline)
{
    NamesAndTypes source_columns = genNamesAndTypes(table_scan, "exchange_receiver");
    const auto & receiver_dag_schema = exchange_receiver->getOutputSchema();
    assert(receiver_dag_schema.size() == source_columns.size());

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    if (push_down_filter.hasValue())
    {
        // No need to cast, because already done by tiflash_storage node.
        ::DB::executePushedDownFilter(/*remote_read_streams_start_index=*/0, push_down_filter, *analyzer, log, pipeline);

        auto & profile_streams = context.getDAGContext()->getProfileStreamsMap()[push_down_filter.executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}
} // namespace DB
