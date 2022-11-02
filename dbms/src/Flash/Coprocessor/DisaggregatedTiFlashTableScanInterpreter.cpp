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
#include <common/logger_useful.h>
#include <Flash/Coprocessor/DisaggregatedTiFlashTableScanInterpreter.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Storages/Transaction/TMTContext.h>

namespace DB
{

void DisaggregatedTiFlashTableScanInterpreter::execute(DAGPipeline & pipeline)
{
    auto dispatch_reqs = buildAndDispatchMPPTaskRequests();
    buildReceiverStreams(dispatch_reqs, pipeline);
}

std::vector<pingcap::coprocessor::BatchCopTask> DisaggregatedTiFlashTableScanInterpreter::buildBatchCopTasks(
        const std::vector<RemoteRequest> & remote_requests)
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
    LOG_DEBUG(log, "batch cop tasks({}) build finish for tiflash_storage node", batch_cop_tasks.size());
    return batch_cop_tasks;
}

::mpp::DispatchTaskRequest buildDispatchMPPTaskRequest(const pingcap::coprocessor::BatchCopTask & batch_cop_task)
{
    const auto & sender_target_task_meta = context.getDAGContext()->getMPPTaskMeta();
    const auto * dag_req = context.getDAGContext()->dag_request;

    ::mpp::DispatchTaskRequest dispatch_req;
    ::mpp::TaskMeta * dispatch_req_meta = dispatch_req.mutable_meta();
    dispatch_req_meta->set_task_id(sender_target_task_meta.task_id());
    dispatch_req_meta->set_start_ts(sender_target_task_meta.start_ts());
    dispatch_req_meta->set_address(batch_cop_task.store_addr);
    const auto & settings = context.getSettings();
    dispatch_req.set_timeout(settings.mpp_task_timeout);
    dispatch_req.set_schema_ver(settings.read_tso);
    RUNTIME_CHECK_MSG(batch_cop_task.region_infos.empty() != batch_cop_task.table_regions.empty(),
            "batch cop task invalid, single table region info: {}, partition table region info: {}",
            batch_cop_task.region_infos.size(), batch_cop_task.table_regions.size());
    if (!batch_cop_task.region_infos.empty())
    {
        // For non-partition table.
        for (const auto & region_info : batch_cop_task.region_infos)
        {
            auto * region = dispatch_req.add_regions();
            region->set_region_id(region_info.region_id.id);
            region->mutable_region_epoch()->set_version(region_info.region_id.ver);
            region->mutable_region_epoch()->set_conf_ver(region_info.region_id.conf_ver);
        }
    }
    else
    {
        // For partition table.
        for (const auto & table_region : batch_cop_task.table_regions)
        {
            auto * req_table_region = dispatch_req.add_table_regions();
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
    // Ignore sender_dag_req.output_offsets because tiflash_storage sender MPPTask is not root.

    tipb::Executor * executor = sender_dag_req.mutable_root_executor();
    executor->set_tp(tipb::ExecType::TypeExchangeSender);
    // gjt todo: also check exec summaries.
    executor->set_executor_id("disaggregated_storage_exchange_sender");

    tipb::ExchangeSender * sender = executor->mutable_exchange_sender();
    sender->set_tp(tipb::ExchangeType::PassThrough);
    sender->add_encoded_task_meta(sender_target_task_meta.SerializeAsString());
    auto * child = sender->mutable_child();
    child->CopyFrom(*(table_scan.getTableScanPB()));
    // gjt todo: ignore sender.all_filed_types
    // Ignore sender.PartitionKeys and sender.Types because it's a PassThrough sender.

    dispatch_req.set_encoded_plan(sender_dag_req.SerializeAsString());
}

std::vector<::mpp::DispatchTaskRequest> DisaggregatedTiFlashTableScanInterpreter::buildAndDispatchMPPTaskRequests()
{
    auto batch_cop_tasks = buildBatchCopTasks(remote_requests);
    std::vector<::mpp::DispatchTaskRequest> dispatch_reqs;
    dispatch_reqs.reserve(batch_cop_tasks.size());
    for (const auto & batch_cop_task : batch_cop_tasks)
        dispatch_reqs.emplace_back(buildDispatchMPPTaskRequest(batch_cop_task));

    std::shared_ptr<ThreadManager> thread_manager = newThreadManager();
    for (const auto & dispatch_req : dispatch_reqs)
    {
        thread_manager->schedule(/*mem_tracker=*/true, "", [&] {
                auto rpc_call = std::make_shared<pingcap::kv::RpcCall<mpp::DispatchTaskRequest>>(dispatch_reqs[i]);
                // gjt todo: retry dispatch
                // gjt todo: make timeout const
                context.getTMTContext().getKVCluster()->rpc_client->sendRequest(batch_cop_tasks[i].store_addr, rpc_call, /*timeout=*/60);
        });
    }

    thread_manager->wait();
    return dispatch_reqs;
}

void DisaggregatedTiFlashTableScanInterpreter::buildReceiverStreams(
        const std::vector<::mpp::DispatchTaskRequest> & dispatch_reqs, DAGPipeline & pipeline)
{
    tipb::ExchangeReceiver receiver;
    const auto & sender_target_task_meta = context.getDAGContext()->getMPPTaskMeta();
    // gjt todo: field types is used to construct schema of ExchangeReceiver, which is only meaningful for cop.
    for (const auto & dispatch_req : dispatch_reqs)
    {
        const ::mpp::TaskMeta & sender_task_meta = dispatch_req.meta();
        receiver.add_encoded_task_meta(sender_task_meta.SerializeAsString());
    }

    static const String executor_id = "disaggregated_compute_exchange_receiver";
    auto exchange_receiver = std::make_shared<ExchangeReceiver>(
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
            /*fine_grained_shuffle_stream_count=*/0);
    // No need to put this ExchangeReciever to DAGContext::receiver_set,
    // because ExchangeReceiverInputStream is generated immediately.
    // Planner no need to touch this ExchangeReciever.
    // gjt todo: put receiver_set!!

    // We can use PhysicalExchange::transform() to build InputStream after DAGQueryBlockInterpreter is deprecated.
    size_t max_streams = context.getMaxStreams();
    for (size_t i = 0; i < max_streams; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(receiver,
                                                                                   log->identifier(),
                                                                                   executor_id,
                                                                                   /*stream_id=*/0);
        // gjt todo:
        // exchange_receiver_io_input_streams.push_back(stream);
        stream = std::make_shared<SquashingBlockInputStream>(stream, 8192, 0, log->identifier());
        // stream->setExtraInfo(extra_info);
        pipeline.streams.push_back(stream);
    }
}
} // namespace DB
