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
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Interpreters/Context.h>
#include <Storages/S3/S3Common.h>
#include <Storages/StorageDisaggregated.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/Types.h>
#include <kvproto/kvrpcpb.pb.h>

namespace DB
{
const String StorageDisaggregated::ExecIDPrefixForTiFlashStorageSender = "exec_id_disaggregated_tiflash_storage_sender";

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
{
}

BlockInputStreams StorageDisaggregated::read(
    const Names &,
    const SelectQueryInfo & query_info,
    const Context & db_context,
    QueryProcessingStage::Enum &,
    size_t,
    unsigned num_streams)
{
    /// S3 config is enabled on the TiFlash compute node, let's read data from S3.
    bool remote_data_read = S3::ClientFactory::instance().isEnabled();
    if (remote_data_read)
        return readThroughS3(db_context, query_info, num_streams);

    /// Fetch all data from write node through MPP exchange sender/receiver
    return readThroughExchange(num_streams);
}

BlockInputStreams StorageDisaggregated::readThroughExchange(unsigned num_streams)
{
    auto remote_table_ranges = buildRemoteTableRanges();

    // only send to tiflash node with label {"engine": "tiflash"}
    auto batch_cop_tasks = buildBatchCopTasks(remote_table_ranges, pingcap::kv::labelFilterNoTiFlashWriteNode);
    RUNTIME_CHECK(!batch_cop_tasks.empty());

    std::vector<RequestAndRegionIDs> dispatch_reqs;
    dispatch_reqs.reserve(batch_cop_tasks.size());
    for (const auto & batch_cop_task : batch_cop_tasks)
        dispatch_reqs.emplace_back(buildDispatchMPPTaskRequest(batch_cop_task));

    DAGPipeline pipeline;
    buildReceiverStreams(dispatch_reqs, num_streams, pipeline);

    NamesAndTypes source_columns = genNamesAndTypesForExchangeReceiver(table_scan);
    assert(exchange_receiver->getOutputSchema().size() == source_columns.size());
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
    // TODO: push down filter conditions to write node
    filterConditions(*analyzer, pipeline);

    return pipeline.streams;
}

std::vector<StorageDisaggregated::RemoteTableRange> StorageDisaggregated::buildRemoteTableRanges()
{
    std::unordered_map<TableID, RegionRetryList> all_remote_regions;
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
        store_type,
        label_filter,
        &Poco::Logger::get("pingcap/coprocessor"));
    LOG_DEBUG(log, "batch cop tasks(nums: {}) build finish for tiflash_storage node", batch_cop_tasks.size());
    return batch_cop_tasks;
}

RequestAndRegionIDs StorageDisaggregated::buildDispatchMPPTaskRequest(
    const pingcap::coprocessor::BatchCopTask & batch_cop_task)
{
    auto dispatch_req = std::make_shared<::mpp::DispatchTaskRequest>();
    ::mpp::TaskMeta * dispatch_req_meta = dispatch_req->mutable_meta();
    auto keyspace_id = context.getDAGContext()->getKeyspaceID();
    dispatch_req_meta->set_keyspace_id(keyspace_id);
    dispatch_req_meta->set_api_version(keyspace_id == NullspaceID ? kvrpcpb::APIVersion::V1 : kvrpcpb::APIVersion::V2);
    dispatch_req_meta->set_start_ts(sender_target_mpp_task_id.query_id.start_ts);
    dispatch_req_meta->set_query_ts(sender_target_mpp_task_id.query_id.query_ts);
    dispatch_req_meta->set_local_query_id(sender_target_mpp_task_id.query_id.local_query_id);
    dispatch_req_meta->set_server_id(sender_target_mpp_task_id.query_id.server_id);
    dispatch_req_meta->set_task_id(sender_target_mpp_task_id.task_id);
    dispatch_req_meta->set_address(batch_cop_task.store_addr);

    // TODO: use different mpp version if necessary
    // dispatch_req_meta->set_mpp_version(?);

    const auto & settings = context.getSettings();
    dispatch_req->set_timeout(60);
    dispatch_req->set_schema_ver(settings.schema_version);

    // For error handling, need to record region_ids and store_id to invalidate cache.
    std::vector<pingcap::kv::RegionVerID> region_ids = RequestUtils::setUpRegionInfos(batch_cop_task, dispatch_req);

    const auto & sender_target_task_meta = context.getDAGContext()->getMPPTaskMeta();
    const auto * dag_req = context.getDAGContext()->dag_request();
    tipb::DAGRequest sender_dag_req;
    sender_dag_req.set_time_zone_name(dag_req->time_zone_name());
    sender_dag_req.set_time_zone_offset(dag_req->time_zone_offset());
    // TODO: We have exec summaries bug for now, remote exec summary will not be merged.
    sender_dag_req.set_collect_execution_summaries(false);
    sender_dag_req.set_flags(dag_req->flags());
    sender_dag_req.set_encode_type(tipb::EncodeType::TypeCHBlock);
    sender_dag_req.set_force_encode_type(true);
    const auto & column_infos = table_scan.getColumns();
    for (size_t off = 0; off < column_infos.size(); ++off)
    {
        sender_dag_req.add_output_offsets(off);
    }

    tipb::Executor * executor = sender_dag_req.mutable_root_executor();
    executor->set_tp(tipb::ExecType::TypeExchangeSender);
    // Exec summary of ExchangeSender will be merged into TableScan.
    executor->set_executor_id(fmt::format("{}_{}",
                                          ExecIDPrefixForTiFlashStorageSender,
                                          sender_target_mpp_task_id.toString()));

    tipb::ExchangeSender * sender = executor->mutable_exchange_sender();
    sender->set_tp(tipb::ExchangeType::PassThrough);

    // TODO: enable data compression if necessary
    // sender->set_compression(tipb::CompressionMode::FAST);

    sender->add_encoded_task_meta(sender_target_task_meta.SerializeAsString());
    auto * child = sender->mutable_child();
    child->CopyFrom(buildTableScanTiPB());
    for (const auto & column_info : column_infos)
    {
        auto * field_type = sender->add_all_field_types();
        *field_type = columnInfoToFieldType(column_info);
    }
    // Ignore sender.PartitionKeys and sender.Types because it's a PassThrough sender.

    dispatch_req->set_encoded_plan(sender_dag_req.SerializeAsString());
    return RequestAndRegionIDs{dispatch_req, region_ids, batch_cop_task.store_id};
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
        *field_type = columnInfoToFieldType(column_info);
    }

    // ExchangeSender just use TableScan's executor_id, so exec summary will be merged to TableScan.
    const auto & sender_target_task_meta = context.getDAGContext()->getMPPTaskMeta();
    const String & executor_id = table_scan.getTableScanExecutorID();

    exchange_receiver = std::make_shared<ExchangeReceiver>(
        std::make_shared<GRPCReceiverContext>(
            receiver,
            sender_target_task_meta,
            context.getTMTContext().getKVCluster(),
            context.getTMTContext().getMPPTaskManager(),
            context.getSettingsRef().enable_local_tunnel,
            context.getSettingsRef().enable_async_grpc_client),
        /*source_num=*/receiver.encoded_task_meta_size(),
        num_streams,
        log->identifier(),
        executor_id,
        /*fine_grained_shuffle_stream_count=*/0,
        context.getSettingsRef().local_tunnel_version,
        dispatch_reqs);

    // MPPTask::receiver_set will record this ExchangeReceiver, so can cancel it in ReceiverSet::cancel().
    context.getDAGContext()->setDisaggregatedComputeExchangeReceiver(executor_id, exchange_receiver);

    // We can use PhysicalExchange::transform() to build InputStream after
    // DAGQueryBlockInterpreter is deprecated to avoid duplicated code here.
    const String extra_info = "disaggregated compute node exchange receiver";
    for (size_t i = 0; i < num_streams; ++i)
    {
        BlockInputStreamPtr stream = std::make_shared<ExchangeReceiverInputStream>(
            exchange_receiver,
            log->identifier(),
            executor_id,
            /*stream_id=*/0);
        stream->setExtraInfo(extra_info);
        pipeline.streams.push_back(stream);
    }

    auto & table_scan_io_input_streams = context.getDAGContext()->getInBoundIOInputStreamsMap()[executor_id];
    auto & profile_streams = context.getDAGContext()->getProfileStreamsMap()[executor_id];
    pipeline.transform([&](auto & stream) {
        table_scan_io_input_streams.push_back(stream);
        profile_streams.push_back(stream);
    });
}

void StorageDisaggregated::filterConditions(DAGExpressionAnalyzer & analyzer, DAGPipeline & pipeline)
{
    if (filter_conditions.hasValue())
    {
        // No need to cast, because already done by tiflash_storage node.
        ::DB::executePushedDownFilter(/*remote_read_streams_start_index=*/pipeline.streams.size(), filter_conditions, analyzer, log, pipeline);

        auto & profile_streams = context.getDAGContext()->getProfileStreamsMap()[filter_conditions.executor_id];
        pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
    }
}

void StorageDisaggregated::extraCast(DAGExpressionAnalyzer & analyzer, DAGPipeline & pipeline)
{
    // If the column is not in the columns of pushed down filter, append a cast to the column.
    std::vector<ExtraCastAfterTSMode> need_cast_column;
    need_cast_column.reserve(table_scan.getColumnSize());
    std::unordered_set<ColumnID> col_id_set;
    for (const auto & expr : table_scan.getPushedDownFilters())
    {
        getColumnIDsFromExpr(expr, table_scan.getColumns(), col_id_set);
    }
    bool has_need_cast_column = false;
    for (const auto & col : table_scan.getColumns())
    {
        if (col_id_set.contains(col.id))
        {
            need_cast_column.push_back(ExtraCastAfterTSMode::None);
        }
        else
        {
            if (col.id != -1 && col.tp == TiDB::TypeTimestamp)
            {
                need_cast_column.push_back(ExtraCastAfterTSMode::AppendTimeZoneCast);
                has_need_cast_column = true;
            }
            else if (col.id != -1 && col.tp == TiDB::TypeTime)
            {
                need_cast_column.push_back(ExtraCastAfterTSMode::AppendDurationCast);
                has_need_cast_column = true;
            }
            else
            {
                need_cast_column.push_back(ExtraCastAfterTSMode::None);
            }
        }
    }
    ExpressionActionsChain chain;
    if (has_need_cast_column && analyzer.appendExtraCastsAfterTS(chain, need_cast_column, table_scan))
    {
        ExpressionActionsPtr extra_cast = chain.getLastActions();
        chain.finalize();
        chain.clear();
        for (auto & stream : pipeline.streams)
        {
            stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, log->identifier());
            stream->setExtraInfo("cast after local tableScan");
        }
    }
}
} // namespace DB
