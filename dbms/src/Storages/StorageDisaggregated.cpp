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

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadManager.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/PushDownFilter.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Flash/Disaggregated/GRPCPageReceiverContext.h>
#include <Flash/Disaggregated/PageDownloader.h>
#include <Flash/Disaggregated/PageReceiver.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/dtpb/column_file.pb.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/Remote/DisaggregatedTaskId.h>
#include <Storages/DeltaMerge/Remote/RemoteReadTask.h>
#include <Storages/DeltaMerge/Remote/RemoteSegmentThreadInputStream.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDisaggregated.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>
#include <kvproto/mpp.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Cluster.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <atomic>
#include <numeric>

namespace pingcap::kv
{
// The rpc trait
template <>
struct RpcTypeTraits<::mpp::EstablishDisaggregatedTaskRequest>
{
    using RequestType = ::mpp::EstablishDisaggregatedTaskRequest;
    using ResultType = ::mpp::EstablishDisaggregatedTaskResponse;
    static const char * err_msg() { return "EstablishDisaggregatedTask Failed"; } // NOLINT(readability-identifier-naming)
    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req,
        ResultType * res)
    {
        return client->stub->EstablishDisaggregatedTask(context, req, res);
    }
};
} // namespace pingcap::kv

namespace DB
{

namespace ErrorCodes
{
extern const int REGION_EPOCH_NOT_MATCH;
} // namespace ErrorCodes

const String StorageDisaggregated::ExecIDPrefixForTiFlashStorageSender = "exec_id_disaggregated_tiflash_storage_sender";

StorageDisaggregated::StorageDisaggregated(
    Context & context_,
    const TiDBTableScan & table_scan_,
    const PushDownFilter & push_down_filter_)
    : IStorage()
    , context(context_)
    , table_scan(table_scan_)
    , log(Logger::get(context_.getDAGContext()->log ? context_.getDAGContext()->log->identifier() : ""))
    , sender_target_mpp_task_id(context_.getDAGContext()->getMPPTaskMeta())
    , push_down_filter(push_down_filter_)
{
}

/**
 * Build the RPC request by region, key-ranges to
 * - build snapshots on write nodes
 * - fetch the related page ids to read node
 */
std::shared_ptr<::mpp::EstablishDisaggregatedTaskRequest>
StorageDisaggregated::buildDisaggregatedTaskForNode(
    const Context & db_context,
    const pingcap::coprocessor::BatchCopTask & batch_cop_task)
{
    const auto & settings = db_context.getSettingsRef();
    auto establish_req = std::make_shared<::mpp::EstablishDisaggregatedTaskRequest>();
    {
        auto * meta = establish_req->mutable_meta();
        meta->set_start_ts(sender_target_mpp_task_id.query_id.start_ts);
        meta->set_query_ts(sender_target_mpp_task_id.query_id.query_ts);
        meta->set_server_id(sender_target_mpp_task_id.query_id.server_id);
        meta->set_local_query_id(sender_target_mpp_task_id.query_id.local_query_id);
        auto * dag_context = db_context.getDAGContext();
        meta->set_task_id(dag_context->getMPPTaskId().task_id);
        meta->set_executor_id(table_scan.getTableScanExecutorID());
    }
    establish_req->set_timeout(10); // 10 secs
    establish_req->set_address(batch_cop_task.store_addr);
    establish_req->set_schema_ver(settings.schema_version);

    RequestUtils::setUpRegionInfos(batch_cop_task, establish_req);

    {
        // Setup the encoded plan
        const auto * dag_req = context.getDAGContext()->dag_request;
        tipb::DAGRequest table_scan_req;
        table_scan_req.set_time_zone_name(dag_req->time_zone_name());
        table_scan_req.set_time_zone_offset(dag_req->time_zone_offset());
        // TODO: disable exec summary for now
        table_scan_req.set_collect_execution_summaries(false);
        table_scan_req.set_flags(dag_req->flags());
        table_scan_req.set_encode_type(tipb::EncodeType::TypeCHBlock);
        table_scan_req.set_force_encode_type(true);
        const auto & column_infos = table_scan.getColumns();
        for (size_t off = 0; off < column_infos.size(); ++off)
        {
            table_scan_req.add_output_offsets(off);
        }

        tipb::Executor * executor = table_scan_req.mutable_root_executor();
        executor->CopyFrom(buildTableScanTiPB());

        establish_req->set_encoded_plan(table_scan_req.SerializeAsString());
    }
    return establish_req;
}

struct DisaggregatedExecutionSummary
{
    size_t establish_rpc_ms{0};
    size_t build_remote_task_ms{0};
};

DM::RemoteReadTaskPtr StorageDisaggregated::buildDisaggregatedTask(
    const Context & db_context,
    const std::vector<pingcap::coprocessor::BatchCopTask> & batch_cop_tasks)
{
    size_t tasks_n = batch_cop_tasks.size();

    // Collect the response from write nodes and build the remote tasks
    std::vector<DM::RemoteTableReadTaskPtr> remote_tasks(tasks_n, nullptr);
    // The execution summaries
    std::vector<DisaggregatedExecutionSummary> summaries(tasks_n);

    auto thread_manager = newThreadManager();
    auto * cluster = context.getTMTContext().getKVCluster();
    const auto & executor_id = table_scan.getTableScanExecutorID();
    const DM::DisaggregatedTaskId task_id(context.getDAGContext()->getMPPTaskId(), executor_id);

    for (size_t idx = 0; idx < tasks_n; ++idx)
    {
        thread_manager->schedule(
            true,
            "EstablishDisaggregated",
            [&, idx] {
                Stopwatch watch;

                const auto & cop_task = batch_cop_tasks[idx];
                auto req = buildDisaggregatedTaskForNode(db_context, cop_task);

                auto call = pingcap::kv::RpcCall<mpp::EstablishDisaggregatedTaskRequest>(req);
                cluster->rpc_client->sendRequest(req->address(), call, req->timeout());
                const auto resp = call.getResp();

                if (resp->has_error())
                {
                    LOG_DEBUG(
                        log,
                        "Received EstablishDisaggregated response with error, addr={} err={}",
                        cop_task.store_addr,
                        resp->error().msg());

                    if (resp->error().code() == ErrorCodes::REGION_EPOCH_NOT_MATCH)
                    {
                        for (const auto & retry_region : resp->retry_regions())
                        {
                            auto region_id = pingcap::kv::RegionVerID(
                                retry_region.id(),
                                retry_region.region_epoch().conf_ver(),
                                retry_region.region_epoch().version());
                            cluster->region_cache->dropRegion(region_id);
                        }
                        throw Exception(resp->error().msg(), ErrorCodes::REGION_EPOCH_NOT_MATCH);
                    }
                    else
                    {
                        // Meet other errors... May be not retryable?
                        throw Exception(fmt::format(
                            "EstablishDisaggregatedTask failed, addr={} error={} code={}",
                            cop_task.store_addr,
                            resp->error().msg(),
                            resp->error().code()));
                    }
                }

                LOG_DEBUG(
                    log,
                    "Received EstablishDisaggregated response, store={} addr={} resp.num_tables={}",
                    resp->store_id(),
                    cop_task.store_addr,
                    resp->tables_size());

                auto this_elapse_ms = watch.elapsedMillisecondsFromLastTime();
                auto & summary = summaries[idx];
                summary.establish_rpc_ms += this_elapse_ms;
                GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_establish).Observe(this_elapse_ms / 1000.0);

                // Parse the resp and gen tasks on read node
                // The number of tasks is equal to number of write nodes
                for (const auto & physical_table : resp->tables())
                {
                    dtpb::DisaggregatedPhysicalTable table;
                    auto parse_ok = table.ParseFromString(physical_table);
                    RUNTIME_CHECK(parse_ok); // TODO: handle error

                    Stopwatch watch_table;

                    remote_tasks[idx] = DM::RemoteTableReadTask::buildFrom(
                        db_context,
                        resp->store_id(),
                        cop_task.store_addr,
                        task_id,
                        table,
                        log);

                    LOG_DEBUG(
                        log,
                        "Build RemoteTableReadTask finished, elapsed={}s store={} addr={} segments={} task_id={}",
                        watch_table.elapsedSeconds(),
                        resp->store_id(),
                        cop_task.store_addr,
                        table.segments().size(),
                        task_id);
                }

                this_elapse_ms = watch.elapsedMillisecondsFromLastTime();
                summary.build_remote_task_ms += this_elapse_ms;
                GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_build_task).Observe(this_elapse_ms / 1000.0);
            });
    }
    thread_manager->wait();

    auto read_task = std::make_shared<DM::RemoteReadTask>(std::move(remote_tasks));

    const auto avg_establish_rpc_ms = std::accumulate(summaries.begin(), summaries.end(), 0.0, [](double lhs, const DisaggregatedExecutionSummary & rhs) -> double { return lhs + rhs.establish_rpc_ms; }) / summaries.size();
    const auto avg_build_remote_task_ms = std::accumulate(summaries.begin(), summaries.end(), 0.0, [](double lhs, const DisaggregatedExecutionSummary & rhs) -> double { return lhs + rhs.build_remote_task_ms; }) / summaries.size();
    LOG_INFO(log, "establish disaggregated task rpc cost {:.2f}ms, build remote tasks cost {:.2f}ms", avg_establish_rpc_ms, avg_build_remote_task_ms);

    return read_task;
}

DM::RSOperatorPtr StorageDisaggregated::buildRSOperator(
    const Context & db_context,
    const DM::ColumnDefinesPtr & columns_to_read)
{
    if (!push_down_filter.hasValue())
        return DM::EMPTY_FILTER;

    const bool enable_rs_filter = db_context.getSettingsRef().dt_enable_rough_set_filter;
    if (!enable_rs_filter)
    {
        LOG_DEBUG(log, "Rough set filter is disabled.");
        return DM::EMPTY_FILTER;
    }

    auto dag_query = std::make_unique<DAGQueryInfo>(
        push_down_filter.conditions,
        DAGPreparedSets{}, // Not care now
        NamesAndTypes{}, // Not care now
        db_context.getTimezoneInfo());
    auto create_attr_by_column_id = [defines = columns_to_read](ColumnID column_id) -> DM::Attr {
        auto iter = std::find_if(
            defines->begin(),
            defines->end(),
            [column_id](const DM::ColumnDefine & d) -> bool { return d.id == column_id; });
        if (iter != defines->end())
            return DM::Attr{.col_name = iter->name, .col_id = iter->id, .type = iter->type};
        return DM::Attr{.col_name = "", .col_id = column_id, .type = DataTypePtr{}};
    };
    auto rs_operator = DM::FilterParser::parseDAGQuery(*dag_query, *columns_to_read, std::move(create_attr_by_column_id), log);
    if (likely(rs_operator != DM::EMPTY_FILTER))
        LOG_DEBUG(log, "Rough set filter: {}", rs_operator->toDebugString());
    return rs_operator;
}

void StorageDisaggregated::buildRemoteSegmentInputStreams(
    const Context & db_context,
    const DM::RemoteReadTaskPtr & remote_read_tasks,
    size_t num_streams,
    DAGPipeline & pipeline)
{
    LOG_DEBUG(log, "build streams with {} segment tasks, num_streams={}", remote_read_tasks->numSegments(), num_streams);
    const auto & executor_id = table_scan.getTableScanExecutorID();
    // Build a PageReceiver to fetch the pages from all write nodes
    auto * kv_cluster = db_context.getTMTContext().getKVCluster();
    auto receiver_ctx = std::make_unique<GRPCPagesReceiverContext>(remote_read_tasks, kv_cluster, /*enable_async=*/false);
    auto page_receiver = std::make_shared<PageReceiver>(
        std::move(receiver_ctx),
        /*source_num_=*/remote_read_tasks->numSegments(),
        num_streams,
        log->identifier(),
        executor_id);

    bool do_prepare = db_context.getSettingsRef().dis_prepare;

    // Build the input streams to read blocks from remote segments
    auto [column_defines, extra_table_id_index] = genColumnDefinesForDisaggregatedRead(table_scan);
    auto page_downloader = std::make_shared<PageDownloader>(
        remote_read_tasks,
        page_receiver,
        column_defines,
        num_streams,
        log->identifier(),
        executor_id,
        do_prepare);

    const UInt64 read_tso = sender_target_mpp_task_id.query_id.start_ts;
    constexpr std::string_view extra_info = "disaggregated compute node remote segment reader";
    pipeline.streams.reserve(num_streams);

    auto rs_operator = buildRSOperator(db_context, column_defines);

    auto io_concurrency = std::max(50, num_streams * 10);
    auto sub_streams_size = io_concurrency / num_streams;

    for (size_t stream_idx = 0; stream_idx < num_streams; ++stream_idx)
    {
        // Build N UnionBlockInputStream, each one collects from M underlying RemoteInputStream.
        // As a result, we will have N * M IO concurrency (N = num_streams, M = sub_streams_size).

        auto sub_streams = DM::RemoteSegmentThreadInputStream::buildInputStreams(
            db_context,
            remote_read_tasks,
            page_downloader,
            column_defines,
            read_tso,
            sub_streams_size,
            extra_table_id_index,
            rs_operator,
            extra_info,
            /*tracing_id*/ log->identifier());
        RUNTIME_CHECK(!sub_streams.empty(), sub_streams.size(), sub_streams_size);

        auto union_stream = std::make_shared<UnionBlockInputStream<>>(sub_streams, BlockInputStreams{}, sub_streams_size, /*req_id=*/"");
        pipeline.streams.emplace_back(std::move(union_stream));
    }

    auto * dag_context = db_context.getDAGContext();
    auto & table_scan_io_input_streams = dag_context->getInBoundIOInputStreamsMap()[executor_id];
    auto & profile_streams = dag_context->getProfileStreamsMap()[executor_id];
    pipeline.transform([&](auto & stream) {
        table_scan_io_input_streams.push_back(stream);
        profile_streams.push_back(stream);
    });
}

BlockInputStreams StorageDisaggregated::readFromWriteNode(
    const Context & db_context,
    unsigned num_streams)
{
    DM::RemoteReadTaskPtr remote_read_tasks;

    pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
    while (true)
    {
        // TODO: We could only retry failed stores.

        try
        {
            auto remote_table_ranges = buildRemoteTableRanges();
            auto batch_cop_tasks = buildBatchCopTasks(remote_table_ranges);
            RUNTIME_CHECK(!batch_cop_tasks.empty());

            // Fetch the remote segment read tasks from write nodes
            remote_read_tasks = buildDisaggregatedTask(db_context, batch_cop_tasks);

            break;
        }
        catch (DB::Exception & e)
        {
            if (e.code() == ErrorCodes::REGION_EPOCH_NOT_MATCH)
                bo.backoff(pingcap::kv::boRegionMiss, pingcap::Exception(e.message()));
            else
                throw;
        }
    }

    // Build InputStream according to the remote segment read tasks
    DAGPipeline pipeline;
    buildRemoteSegmentInputStreams(db_context, remote_read_tasks, num_streams, pipeline);
    NamesAndTypes source_columns = genNamesAndTypesForExchangeReceiver(table_scan);
    pushDownFilter(std::move(source_columns), pipeline);
    return pipeline.streams;
}

BlockInputStreams StorageDisaggregated::read(
    const Names &,
    const SelectQueryInfo &,
    const Context & db_context,
    QueryProcessingStage::Enum &,
    size_t,
    unsigned num_streams)
{
    bool remote_data_read = !db_context.remoteDataServiceSource().empty();
    if (remote_data_read)
        return readFromWriteNode(db_context, num_streams);

    auto remote_table_ranges = buildRemoteTableRanges();

    auto batch_cop_tasks = buildBatchCopTasks(remote_table_ranges);
    RUNTIME_CHECK(!batch_cop_tasks.empty());

    // Fetch all data from write node through MPP exchange sender/receiver
    std::vector<RequestAndRegionIDs> dispatch_reqs;
    dispatch_reqs.reserve(batch_cop_tasks.size());
    for (const auto & batch_cop_task : batch_cop_tasks)
        dispatch_reqs.emplace_back(buildDispatchMPPTaskRequest(batch_cop_task));

    DAGPipeline pipeline;
    buildReceiverStreams(dispatch_reqs, num_streams, pipeline);

    NamesAndTypes source_columns = genNamesAndTypesForExchangeReceiver(table_scan);
    assert(exchange_receiver->getOutputSchema().size() == source_columns.size());
    pushDownFilter(std::move(source_columns), pipeline);

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
    auto batch_cop_tasks = pingcap::coprocessor::buildBatchCopTasks(
        bo,
        cluster,
        table_scan.isPartitionTableScan(),
        physical_table_ids,
        ranges_for_each_physical_table,
        store_type,
        &Poco::Logger::get("pingcap/coprocessor"));
    LOG_DEBUG(log, "batch cop tasks(nums: {}) build finish for tiflash_storage node", batch_cop_tasks.size());
    return batch_cop_tasks;
}

StorageDisaggregated::RequestAndRegionIDs StorageDisaggregated::buildDispatchMPPTaskRequest(
    const pingcap::coprocessor::BatchCopTask & batch_cop_task)
{
    auto dispatch_req = std::make_shared<::mpp::DispatchTaskRequest>();
    ::mpp::TaskMeta * dispatch_req_meta = dispatch_req->mutable_meta();
    dispatch_req_meta->set_start_ts(sender_target_mpp_task_id.query_id.start_ts);
    dispatch_req_meta->set_query_ts(sender_target_mpp_task_id.query_id.query_ts);
    dispatch_req_meta->set_local_query_id(sender_target_mpp_task_id.query_id.local_query_id);
    dispatch_req_meta->set_server_id(sender_target_mpp_task_id.query_id.server_id);
    dispatch_req_meta->set_task_id(sender_target_mpp_task_id.task_id);
    dispatch_req_meta->set_address(batch_cop_task.store_addr);
    const auto & settings = context.getSettings();
    dispatch_req->set_timeout(60);
    dispatch_req->set_schema_ver(settings.schema_version);

    // For error handling, need to record region_ids and store_id to invalidate cache.
    std::vector<pingcap::kv::RegionVerID> region_ids = RequestUtils::setUpRegionInfos(batch_cop_task, dispatch_req);

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

void StorageDisaggregated::pushDownFilter(NamesAndTypes && source_columns, DAGPipeline & pipeline)
{
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
