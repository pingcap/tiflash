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

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadManager.h>
#include <Common/TiFlashMetrics.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataTypes/IDataType.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Flash/Disaggregated/RNPagePreparer.h>
#include <Flash/Disaggregated/RNPageReceiver.h>
#include <Flash/Disaggregated/RNPageReceiverContext.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask.h>
#include <Storages/DeltaMerge/Remote/RNRemoteSegmentThreadInputStream.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDisaggregated.h>
#include <Storages/StorageDisaggregatedHelpers.h>
#include <Storages/Transaction/DecodingStorageSchemaSnapshot.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>
#include <kvproto/disaggregated.pb.h>
#include <kvproto/kvrpcpb.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionCache.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <atomic>
#include <numeric>
#include <unordered_set>

namespace pingcap::kv
{
// The rpc trait
template <>
struct RpcTypeTraits<disaggregated::EstablishDisaggTaskRequest>
{
    using RequestType = disaggregated::EstablishDisaggTaskRequest;
    using ResultType = disaggregated::EstablishDisaggTaskResponse;

    static const char * err_msg() { return "EstablishDisaggTask Failed"; } // NOLINT(readability-identifier-naming)

    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req,
        ResultType * res)
    {
        return client->stub->EstablishDisaggTask(context, req, res);
    }
};
} // namespace pingcap::kv

namespace DB
{

namespace ErrorCodes
{
extern const int DISAGG_ESTABLISH_RETRYABLE_ERROR;
} // namespace ErrorCodes

BlockInputStreams StorageDisaggregated::readThroughS3(
    const Context & db_context,
    const SelectQueryInfo & query_info,
    unsigned num_streams)
{
    using namespace pingcap;

    auto scan_context = std::make_shared<DM::ScanContext>();
    context.getDAGContext()->scan_context_map[table_scan.getTableScanExecutorID()] = scan_context;

    DM::RNRemoteReadTaskPtr remote_read_tasks;

    double total_backoff_seconds = 0.0;
    SCOPE_EXIT({
        GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_total_establish_backoff).Observe(total_backoff_seconds);
    });

    kv::Backoffer bo(kv::copNextMaxBackoff);
    while (true)
    {
        // TODO: We could only retry failed stores.

        try
        {
            auto remote_table_ranges = buildRemoteTableRanges();
            // only send to tiflash node with label [{"engine":"tiflash"}, {"engine-role":"write"}]
            auto label_filter = pingcap::kv::labelFilterOnlyTiFlashWriteNode;
            auto batch_cop_tasks = buildBatchCopTasks(remote_table_ranges, label_filter);
            RUNTIME_CHECK(!batch_cop_tasks.empty());

            // Fetch the remote segment read tasks from write nodes
            remote_read_tasks = buildDisaggTasks(
                db_context,
                scan_context,
                batch_cop_tasks);

            break;
        }
        catch (DB::Exception & e)
        {
            if (e.code() != ErrorCodes::DISAGG_ESTABLISH_RETRYABLE_ERROR)
                throw;

            Stopwatch w_backoff;
            SCOPE_EXIT({
                total_backoff_seconds += w_backoff.elapsedSeconds();
            });

            LOG_INFO(log, "Meets retryable error: {}, retry to build remote read tasks", e.message());
            bo.backoff(pingcap::kv::boRegionMiss, pingcap::Exception(e.message(), e.code()));
        }
    }

    // Build InputStream according to the remote segment read tasks
    DAGPipeline pipeline;
    buildRemoteSegmentInputStreams(db_context, remote_read_tasks, query_info, num_streams, pipeline);

    NamesAndTypes source_columns;
    source_columns.reserve(table_scan.getColumnSize());
    const auto & remote_segment_stream_header = pipeline.firstStream()->getHeader();
    for (const auto & col : remote_segment_stream_header)
    {
        source_columns.emplace_back(col.name, col.type);
    }
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    // Handle duration type column
    extraCast(*analyzer, pipeline);
    // Handle filter
    filterConditions(*analyzer, pipeline);
    return pipeline.streams;
}


DM::RNRemoteReadTaskPtr StorageDisaggregated::buildDisaggTasks(
    const Context & db_context,
    const DM::ScanContextPtr & scan_context,
    const std::vector<pingcap::coprocessor::BatchCopTask> & batch_cop_tasks)
{
    size_t tasks_n = batch_cop_tasks.size();

    std::mutex store_read_tasks_lock;
    std::vector<DM::RNRemoteStoreReadTaskPtr> store_read_tasks;
    store_read_tasks.reserve(tasks_n);

    auto thread_manager = newThreadManager();
    const auto & executor_id = table_scan.getTableScanExecutorID();
    const DM::DisaggTaskId task_id(context.getDAGContext()->getMPPTaskId(), executor_id);

    for (const auto & cop_task : batch_cop_tasks)
    {
        thread_manager->schedule(
            true,
            "BuildDisaggTask",
            [&] {
                buildDisaggTask(
                    db_context,
                    scan_context,
                    cop_task,
                    store_read_tasks,
                    store_read_tasks_lock);
            });
    }

    // The first exception will be thrown out.
    thread_manager->wait();

    return std::make_shared<DM::RNRemoteReadTask>(std::move(store_read_tasks));
}

/// Note: This function runs concurrently when there are multiple Write Nodes.
void StorageDisaggregated::buildDisaggTask(
    const Context & db_context,
    const DM::ScanContextPtr & scan_context,
    const pingcap::coprocessor::BatchCopTask & batch_cop_task,
    std::vector<DM::RNRemoteStoreReadTaskPtr> & store_read_tasks,
    std::mutex & store_read_tasks_lock)
{
    Stopwatch watch;

    auto req = buildDisaggTaskForNode(db_context, batch_cop_task);

    auto * cluster = context.getTMTContext().getKVCluster();
    auto call = pingcap::kv::RpcCall<disaggregated::EstablishDisaggTaskRequest>(req);
    cluster->rpc_client->sendRequest(req->address(), call, DEFAULT_DISAGG_TASK_BUILD_TIMEOUT_SEC);
    const auto resp = call.getResp();

    const DM::DisaggTaskId snapshot_id(resp->snapshot_id());
    LOG_DEBUG(
        log,
        "Received EstablishDisaggregated response, error={} store={} snap_id={} addr={} resp.num_tables={}",
        resp->has_error(),
        resp->store_id(),
        snapshot_id,
        batch_cop_task.store_addr,
        resp->tables_size());

    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_rpc_establish).Observe(watch.elapsedSeconds());
    watch.restart();

    if (resp->has_error())
    {
        // We meet error in the EstablishDisaggTask response.
        if (resp->error().has_error_region())
        {
            const auto & error = resp->error().error_region();
            // Refresh region cache and throw an exception for retrying.
            // Note: retry_region's region epoch is not set. We need to recover from the request.

            std::unordered_set<RegionID> retry_regions;
            for (const auto & region_id : error.region_ids())
                retry_regions.insert(region_id);

            LOG_INFO(
                log,
                "Received EstablishDisaggregated response with retryable error: {}, addr={} retry_regions={}",
                error.msg(),
                batch_cop_task.store_addr,
                retry_regions);

            dropRegionCache(cluster->region_cache, req, std::move(retry_regions));

            throw Exception(
                error.msg(),
                ErrorCodes::DISAGG_ESTABLISH_RETRYABLE_ERROR);
        }
        else if (resp->error().has_error_locked())
        {
            using namespace pingcap;

            const auto & error = resp->error().error_locked();

            LOG_INFO(
                log,
                "Received EstablishDisaggregated response with retryable error: {}, addr={}",
                error.msg(),
                batch_cop_task.store_addr);

            Stopwatch w_resolve_lock;

            // Try to resolve all locks.
            kv::Backoffer bo(kv::copNextMaxBackoff);
            std::vector<uint64_t> pushed;
            std::vector<kv::LockPtr> locks{};
            for (const auto & lock_info : error.locked())
                locks.emplace_back(std::make_shared<kv::Lock>(lock_info));
            auto before_expired = cluster->lock_resolver->resolveLocks(bo, sender_target_mpp_task_id.query_id.start_ts, locks, pushed);

            // TODO: Use `pushed` to bypass large txn.
            LOG_DEBUG(log, "Finished resolve locks, elapsed={}s n_locks={} pushed.size={} before_expired={}", w_resolve_lock.elapsedSeconds(), locks.size(), pushed.size(), before_expired);

            GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_resolve_lock).Observe(w_resolve_lock.elapsedSeconds());

            throw Exception(
                error.msg(),
                ErrorCodes::DISAGG_ESTABLISH_RETRYABLE_ERROR);
        }
        else
        {
            const auto & error = resp->error().error_other();

            LOG_WARNING(
                log,
                "Received EstablishDisaggregated response with error, addr={} err={}",
                batch_cop_task.store_addr,
                error.msg());

            // Meet other errors... May be not retryable?
            throw Exception(
                error.code(),
                "EstablishDisaggregatedTask failed: {}, addr={}",
                error.msg(),
                batch_cop_task.store_addr);
        }
    }

    // Parse the resp and gen tasks on read node
    std::vector<DM::RNRemotePhysicalTableReadTaskPtr> remote_seg_tasks;
    remote_seg_tasks.reserve(resp->tables_size());
    for (const auto & physical_table : resp->tables())
    {
        DB::DM::RemotePb::RemotePhysicalTable table;
        auto parse_ok = table.ParseFromString(physical_table);
        RUNTIME_CHECK_MSG(parse_ok, "Failed to deserialize RemotePhysicalTable from response");

        Stopwatch w_build_table_task;

        const auto task = DM::RNRemotePhysicalTableReadTask::buildFrom(
            db_context,
            scan_context,
            resp->store_id(),
            batch_cop_task.store_addr,
            snapshot_id,
            table,
            log);
        remote_seg_tasks.emplace_back(task);

        LOG_DEBUG(
            log,
            "Build RNRemotePhysicalTableReadTask finished, elapsed={:.3f}s store={} addr={} segments={}",
            w_build_table_task.elapsedSeconds(),
            resp->store_id(),
            batch_cop_task.store_addr,
            table.segments().size());
    }
    std::unique_lock lock(store_read_tasks_lock);
    store_read_tasks.emplace_back(std::make_shared<DM::RNRemoteStoreReadTask>(resp->store_id(), remote_seg_tasks));

    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_build_read_task).Observe(watch.elapsedSeconds());
}

/**
 * Build the RPC request by region, key-ranges to
 * - build snapshots on write nodes
 * - fetch the corresponding ColumnFiles' meta that read node
 *   need to read from the remote storage
 *
 * Similar to `StorageDisaggregated::buildDispatchMPPTaskRequest`
 */
std::shared_ptr<disaggregated::EstablishDisaggTaskRequest>
StorageDisaggregated::buildDisaggTaskForNode(
    const Context & db_context,
    const pingcap::coprocessor::BatchCopTask & batch_cop_task)
{
    const auto & settings = db_context.getSettingsRef();
    auto establish_req = std::make_shared<disaggregated::EstablishDisaggTaskRequest>();
    {
        disaggregated::DisaggTaskMeta * meta = establish_req->mutable_meta();
        meta->set_start_ts(sender_target_mpp_task_id.query_id.start_ts);
        meta->set_query_ts(sender_target_mpp_task_id.query_id.query_ts);
        meta->set_server_id(sender_target_mpp_task_id.query_id.server_id);
        meta->set_local_query_id(sender_target_mpp_task_id.query_id.local_query_id);
        auto * dag_context = db_context.getDAGContext();
        meta->set_task_id(dag_context->getMPPTaskId().task_id);
        meta->set_executor_id(table_scan.getTableScanExecutorID());
        const auto keyspace_id = dag_context->getKeyspaceID();
        meta->set_keyspace_id(keyspace_id);
        meta->set_api_version(keyspace_id == NullspaceID ? kvrpcpb::APIVersion::V1 : kvrpcpb::APIVersion::V2);
    }
    // how long the task is valid on the write node
    establish_req->set_timeout_s(DEFAULT_DISAGG_TASK_TIMEOUT_SEC);
    establish_req->set_address(batch_cop_task.store_addr);
    establish_req->set_schema_ver(settings.schema_version);

    RequestUtils::setUpRegionInfos(batch_cop_task, establish_req);

    {
        // Setup the encoded plan
        const auto * dag_req = context.getDAGContext()->dag_request();
        tipb::DAGRequest table_scan_req;
        table_scan_req.set_time_zone_name(dag_req->time_zone_name());
        table_scan_req.set_time_zone_offset(dag_req->time_zone_offset());
        // TODO: enable exec summary collection
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

DM::RSOperatorPtr StorageDisaggregated::buildRSOperator(
    const Context & db_context,
    const DM::ColumnDefinesPtr & columns_to_read)
{
    if (!filter_conditions.hasValue())
        return DM::EMPTY_RS_OPERATOR;

    const bool enable_rs_filter = db_context.getSettingsRef().dt_enable_rough_set_filter;
    if (!enable_rs_filter)
    {
        LOG_DEBUG(log, "Rough set filter is disabled.");
        return DM::EMPTY_RS_OPERATOR;
    }

    auto dag_query = std::make_unique<DAGQueryInfo>(
        filter_conditions.conditions,
        table_scan.getPushedDownFilters(),
        table_scan.getColumns(),
        db_context.getTimezoneInfo());
<<<<<<< HEAD
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
    if (likely(rs_operator != DM::EMPTY_RS_OPERATOR))
        LOG_DEBUG(log, "Rough set filter: {}", rs_operator->toDebugString());
    return rs_operator;
=======

    return DM::RSOperator::build(dag_query, table_scan.getColumns(), *columns_to_read, enable_rs_filter, log);
>>>>>>> e6fc04addf (Storages: Fix obtaining incorrect column information when there are virtual columns in the query (#9189))
}

void StorageDisaggregated::buildRemoteSegmentInputStreams(
    const Context & db_context,
    const DM::RNRemoteReadTaskPtr & remote_read_tasks,
    const SelectQueryInfo &,
    size_t num_streams,
    DAGPipeline & pipeline)
{
    auto io_concurrency = static_cast<size_t>(static_cast<double>(num_streams) * db_context.getSettingsRef().disagg_read_concurrency_scale);
    LOG_DEBUG(log, "Build disagg streams with {} segment tasks, num_streams={} io_concurrency={}", remote_read_tasks->numSegments(), num_streams, io_concurrency);
    // TODO: We can reduce max io_concurrency to numSegments.

    const auto & executor_id = table_scan.getTableScanExecutorID();
    // Build a RNPageReceiver to fetch the pages from all write nodes
    auto * kv_cluster = db_context.getTMTContext().getKVCluster();
    auto receiver_ctx = std::make_unique<GRPCPagesReceiverContext>(remote_read_tasks, kv_cluster);
    auto page_receiver = std::make_shared<RNPageReceiver>(
        std::move(receiver_ctx),
        /*source_num_=*/remote_read_tasks->numSegments(),
        num_streams,
        log->identifier(),
        executor_id);

    bool do_prepare = false;

    // Build the input streams to read blocks from remote segments
    auto [column_defines, extra_table_id_index] = genColumnDefinesForDisaggregatedRead(table_scan);
    auto page_preparer = std::make_shared<RNPagePreparer>(
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
    auto push_down_filter = StorageDeltaMerge::buildPushDownFilter(
        rs_operator,
        table_scan.getColumns(),
        table_scan.getPushedDownFilters(),
        *column_defines,
        db_context,
        log);
    auto read_mode = DM::DeltaMergeStore::getReadMode(db_context, table_scan.isFastScan(), table_scan.keepOrder(), push_down_filter);

    auto sub_streams_size = io_concurrency / num_streams;
    for (size_t stream_idx = 0; stream_idx < num_streams; ++stream_idx)
    {
        // Build N UnionBlockInputStream, each one collects from M underlying RemoteInputStream.
        // As a result, we will have N * M IO concurrency (N = num_streams, M = sub_streams_size).

        auto sub_streams = DM::RNRemoteSegmentThreadInputStream::buildInputStreams(
            db_context,
            remote_read_tasks,
            page_preparer,
            column_defines,
            read_tso,
            sub_streams_size,
            extra_table_id_index,
            push_down_filter,
            extra_info,
            /*tracing_id*/ log->identifier(),
            read_mode);
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

} // namespace DB
