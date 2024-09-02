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
#include <Interpreters/Context.h>
#include <Operators/UnorderedSourceOp.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/PushDownFilter.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/ReadThread/UnorderedInputStream.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/RNSegmentInputStream.h>
#include <Storages/DeltaMerge/Remote/RNSegmentSourceOp.h>
#include <Storages/DeltaMerge/Remote/RNWorkers.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/KVStore/Decode/DecodingStorageSchemaSnapshot.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/StorageDisaggregated.h>
#include <Storages/StorageDisaggregatedHelpers.h>
#include <TiDB/Schema/TiDB.h>
#include <grpcpp/support/status_code_enum.h>
#include <kvproto/disaggregated.pb.h>
#include <kvproto/kvrpcpb.pb.h>
#include <pingcap/coprocessor/Client.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/RegionCache.h>
#include <tipb/executor.pb.h>
#include <tipb/select.pb.h>

#include <atomic>
#include <magic_enum.hpp>
#include <numeric>
#include <unordered_set>
#include <variant>

namespace DB
{
namespace ErrorCodes
{
extern const int DISAGG_ESTABLISH_RETRYABLE_ERROR;
extern const int TIMEOUT_EXCEEDED;
extern const int UNKNOWN_EXCEPTION;
} // namespace ErrorCodes

namespace
{
void initDisaggTaskMeta(
    disaggregated::DisaggTaskMeta * meta,
    const MPPTaskId & sender_target_mpp_task_id,
    DAGContext * dag_context,
    const KeyspaceID keyspace_id,
    const TiDBTableScan & table_scan)
{
    meta->set_start_ts(sender_target_mpp_task_id.gather_id.query_id.start_ts);
    meta->set_query_ts(sender_target_mpp_task_id.gather_id.query_id.query_ts);
    meta->set_server_id(sender_target_mpp_task_id.gather_id.query_id.server_id);
    meta->set_local_query_id(sender_target_mpp_task_id.gather_id.query_id.local_query_id);
    meta->set_gather_id(sender_target_mpp_task_id.gather_id.gather_id);
    meta->set_task_id(dag_context->getMPPTaskId().task_id);
    meta->set_executor_id(table_scan.getTableScanExecutorID());
    meta->set_keyspace_id(keyspace_id);
    meta->set_api_version(keyspace_id == NullspaceID ? kvrpcpb::APIVersion::V1 : kvrpcpb::APIVersion::V2);
    meta->set_connection_id(sender_target_mpp_task_id.gather_id.query_id.connection_id);
    meta->set_connection_alias(sender_target_mpp_task_id.gather_id.query_id.connection_alias);
}
} // namespace

BlockInputStreams StorageDisaggregated::readThroughS3(const Context & db_context, unsigned num_streams)
{
    // Build InputStream according to the remote segment read tasks
    DAGPipeline pipeline;
    buildRemoteSegmentInputStreams(db_context, buildReadTaskWithBackoff(db_context), num_streams, pipeline);

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

void StorageDisaggregated::readThroughS3(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const Context & db_context,
    unsigned num_streams)
{
    buildRemoteSegmentSourceOps(
        exec_context,
        group_builder,
        db_context,
        buildReadTaskWithBackoff(db_context),
        num_streams);

    NamesAndTypes source_columns;
    auto header = group_builder.getCurrentHeader();
    source_columns.reserve(header.columns());
    for (const auto & col : header)
        source_columns.emplace_back(col.name, col.type);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    // Handle duration type column
    extraCast(exec_context, group_builder, *analyzer);
    // Handle filter
    filterConditions(exec_context, group_builder, *analyzer);
}

DM::SegmentReadTasks StorageDisaggregated::buildReadTaskWithBackoff(const Context & db_context)
{
    using namespace pingcap;

    auto * dag_context = context.getDAGContext();
    auto scan_context = std::make_shared<DM::ScanContext>(dag_context->getResourceGroupName());
    dag_context->scan_context_map[table_scan.getTableScanExecutorID()] = scan_context;

    DM::SegmentReadTasks read_task;

    double total_backoff_seconds = 0.0;
    SCOPE_EXIT({
        // This metric is per-read.
        GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_total_establish_backoff)
            .Observe(total_backoff_seconds);
    });

    kv::Backoffer bo(kv::copNextMaxBackoff);
    while (true)
    {
        // TODO: We could only retry failed stores.

        try
        {
            // Fetch the remote segment read tasks from write nodes
            read_task = buildReadTask(db_context, scan_context);
            break;
        }
        catch (DB::Exception & e)
        {
            if (e.code() != ErrorCodes::DISAGG_ESTABLISH_RETRYABLE_ERROR)
                throw;

            Stopwatch w_backoff;
            SCOPE_EXIT({ total_backoff_seconds += w_backoff.elapsedSeconds(); });

            LOG_INFO(log, "Meets retryable error: {}, retry to build remote read tasks", e.message());
            bo.backoff(pingcap::kv::boRegionMiss, pingcap::Exception(e.message(), e.code()));
        }
    }

    return read_task;
}

DM::SegmentReadTasks StorageDisaggregated::buildReadTask(
    const Context & db_context,
    const DM::ScanContextPtr & scan_context)
{
    std::vector<pingcap::coprocessor::BatchCopTask> batch_cop_tasks;

    // First split the read task for different write nodes.
    // For each write node, a BatchCopTask is built.
    {
        auto [remote_table_ranges, region_num] = buildRemoteTableRanges();
        scan_context->setRegionNumOfCurrentInstance(region_num);
        // only send to tiflash node with label [{"engine":"tiflash"}, {"engine-role":"write"}]
        const auto label_filter = pingcap::kv::labelFilterOnlyTiFlashWriteNode;
        batch_cop_tasks = buildBatchCopTasks(remote_table_ranges, label_filter);
        RUNTIME_CHECK(!batch_cop_tasks.empty());
    }

    std::mutex output_lock;
    DM::SegmentReadTasks output_seg_tasks;

    // Then, for each BatchCopTask, let's build read tasks concurrently.
    auto thread_manager = newThreadManager();
    for (const auto & cop_task : batch_cop_tasks)
    {
<<<<<<< HEAD
        thread_manager->schedule(true, "buildReadTaskForWriteNode", [&] {
            buildReadTaskForWriteNode(db_context, scan_context, cop_task, output_lock, output_seg_tasks);
        });
=======
        auto f = BuildReadTaskForWNPool::get().scheduleWithFuture(
            [&] { buildReadTaskForWriteNode(db_context, scan_context, cop_task, output_lock, output_seg_tasks); },
            getBuildTaskIOThreadPoolTimeout());
        futures.add(std::move(f));
>>>>>>> 738aade7e5 (Disagg: Set the waiting time for thread pool. (#9393))
    }

    // Let's wait for all threads to finish. Otherwise local variable references will be invalid.
    // The first exception will be thrown out if any, after all threads are finished, which is safe.
    thread_manager->wait();

    // Do some integrity checks for the build seg tasks. For example, we should not
    // ever read from the same store+table+segment multiple times.
    {
        // TODO
    }
    scan_context->num_segments = output_seg_tasks.size();
    return output_seg_tasks;
}

void StorageDisaggregated::buildReadTaskForWriteNode(
    const Context & db_context,
    const DM::ScanContextPtr & scan_context,
    const pingcap::coprocessor::BatchCopTask & batch_cop_task,
    std::mutex & output_lock,
    DM::SegmentReadTasks & output_seg_tasks)
{
    Stopwatch watch;

    const auto req = buildEstablishDisaggTaskReq(db_context, batch_cop_task);

    auto * cluster = context.getTMTContext().getKVCluster();
    pingcap::kv::RpcCall<pingcap::kv::RPC_NAME(EstablishDisaggTask)> rpc(cluster->rpc_client, req->address());
    disaggregated::EstablishDisaggTaskResponse resp;
    grpc::ClientContext client_context;
    rpc.setClientContext(client_context, getBuildTaskRPCTimeout());
    auto status = rpc.call(&client_context, *req, &resp);
    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED)
        throw Exception(
            ErrorCodes::TIMEOUT_EXCEEDED,
            "EstablishDisaggregated execution was interrupted, maximum execution time exceeded, wn_address={} {}",
            req->address(),
            log->identifier());
    else if (!status.ok())
        throw Exception(
            ErrorCodes::UNKNOWN_EXCEPTION,
            "EstablishDisaggTask failed, wn_address={}, errmsg={}, {}",
            req->address(),
            rpc.errMsg(status),
            log->identifier());

    const DM::DisaggTaskId snapshot_id(resp.snapshot_id());
    LOG_DEBUG(
        log,
        "Received EstablishDisaggregated response, error={} store={} snap_id={} addr={} resp.num_tables={}",
        resp.has_error(),
        resp.store_id(),
        snapshot_id,
        batch_cop_task.store_addr,
        resp.tables_size());

    GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_rpc_establish).Observe(watch.elapsedSeconds());
    watch.restart();

    if (resp.has_error())
    {
        // We meet error in the EstablishDisaggTask response.
        if (resp.error().has_error_region())
        {
            const auto & error = resp.error().error_region();
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

            throw Exception(error.msg(), ErrorCodes::DISAGG_ESTABLISH_RETRYABLE_ERROR);
        }
        else if (resp.error().has_error_locked())
        {
            using namespace pingcap;

            const auto & error = resp.error().error_locked();

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
            auto before_expired = cluster->lock_resolver->resolveLocks(
                bo,
                sender_target_mpp_task_id.gather_id.query_id.start_ts,
                locks,
                pushed);

            // TODO: Use `pushed` to bypass large txn.
            LOG_DEBUG(
                log,
                "Finished resolve locks, elapsed={}s n_locks={} pushed.size={} before_expired={}",
                w_resolve_lock.elapsedSeconds(),
                locks.size(),
                pushed.size(),
                before_expired);

            GET_METRIC(tiflash_disaggregated_breakdown_duration_seconds, type_resolve_lock)
                .Observe(w_resolve_lock.elapsedSeconds());

            throw Exception(error.msg(), ErrorCodes::DISAGG_ESTABLISH_RETRYABLE_ERROR);
        }
        else
        {
            const auto & error = resp.error().error_other();

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

    // Now we have successfully established disaggregated read for this write node.
    // Let's parse the result and generate actual segment read tasks.
    // There may be multiple tables, so we concurrently build tasks for these tables.
    auto thread_manager = newThreadManager();
    for (const auto & serialized_physical_table : resp.tables())
    {
<<<<<<< HEAD
        thread_manager->schedule(true, "buildReadTaskForWriteNodeTable", [&] {
            buildReadTaskForWriteNodeTable(
                db_context,
                scan_context,
                snapshot_id,
                resp.store_id(),
                req->address(),
                serialized_physical_table,
                output_lock,
                output_seg_tasks);
        });
=======
        auto f = BuildReadTaskForWNTablePool::get().scheduleWithFuture(
            [&] {
                buildReadTaskForWriteNodeTable(
                    db_context,
                    scan_context,
                    snapshot_id,
                    resp.store_id(),
                    req->address(),
                    serialized_physical_table,
                    output_lock,
                    output_seg_tasks);
            },
            getBuildTaskIOThreadPoolTimeout());
        futures.add(std::move(f));
>>>>>>> 738aade7e5 (Disagg: Set the waiting time for thread pool. (#9393))
    }
    thread_manager->wait();
}

void StorageDisaggregated::buildReadTaskForWriteNodeTable(
    const Context & db_context,
    const DM::ScanContextPtr & scan_context,
    const DM::DisaggTaskId & snapshot_id,
    StoreID store_id,
    const String & store_address,
    const String & serialized_physical_table,
    std::mutex & output_lock,
    DM::SegmentReadTasks & output_seg_tasks)
{
    DB::DM::RemotePb::RemotePhysicalTable table;
    auto parse_ok = table.ParseFromString(serialized_physical_table);
    RUNTIME_CHECK_MSG(parse_ok, "Failed to deserialize RemotePhysicalTable from response");

    auto thread_manager = newThreadManager();
    auto n = static_cast<size_t>(table.segments().size());

    auto table_tracing_logger = log->getChild(
        fmt::format("store_id={} keyspace={} table_id={}", store_id, table.keyspace_id(), table.table_id()));
<<<<<<< HEAD
    for (size_t idx = 0; idx < n; ++idx)
    {
        const auto & remote_seg = table.segments(idx);

        thread_manager->schedule(true, "buildRNReadSegmentTask", [&] {
            auto seg_read_task = std::make_shared<DM::SegmentReadTask>(
                table_tracing_logger,
                db_context,
                scan_context,
                remote_seg,
                snapshot_id,
                store_id,
                store_address,
                table.keyspace_id(),
                table.table_id());

            std::lock_guard lock(output_lock);
            output_seg_tasks.push_back(seg_read_task);
        });
=======

    IOPoolHelper::FutureContainer futures(log, table.segments().size());
    for (const auto & remote_seg : table.segments())
    {
        auto f = BuildReadTaskPool::get().scheduleWithFuture(
            [&]() {
                auto seg_read_task = std::make_shared<DM::SegmentReadTask>(
                    table_tracing_logger,
                    db_context,
                    scan_context,
                    remote_seg,
                    snapshot_id,
                    store_id,
                    store_address,
                    table.keyspace_id(),
                    table.table_id());
                std::lock_guard lock(output_lock);
                output_seg_tasks.push_back(seg_read_task);
            },
            getBuildTaskIOThreadPoolTimeout());
        futures.add(std::move(f));
>>>>>>> 738aade7e5 (Disagg: Set the waiting time for thread pool. (#9393))
    }

    thread_manager->wait();
}

/**
 * Build the RPC request by region, key-ranges to
 * - build snapshots on write nodes
 * - fetch the corresponding ColumnFiles' meta that read node
 *   need to read from the remote storage
 *
 * Similar to `StorageDisaggregated::buildDispatchMPPTaskRequest`
 */
std::shared_ptr<disaggregated::EstablishDisaggTaskRequest> StorageDisaggregated::buildEstablishDisaggTaskReq(
    const Context & db_context,
    const pingcap::coprocessor::BatchCopTask & batch_cop_task)
{
    const auto & settings = db_context.getSettingsRef();
    auto establish_req = std::make_shared<disaggregated::EstablishDisaggTaskRequest>();

    {
        auto * dag_context = db_context.getDAGContext();
        initDisaggTaskMeta(
            establish_req->mutable_meta(),
            sender_target_mpp_task_id,
            dag_context,
            dag_context->getKeyspaceID(),
            table_scan);
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
        std::vector<int>{},
        0,
        db_context.getTimezoneInfo());
    auto create_attr_by_column_id = [defines = columns_to_read](ColumnID column_id) -> DM::Attr {
        auto iter = std::find_if(defines->begin(), defines->end(), [column_id](const DM::ColumnDefine & d) -> bool {
            return d.id == column_id;
        });
        if (iter != defines->end())
            return DM::Attr{.col_name = iter->name, .col_id = iter->id, .type = iter->type};
        return DM::Attr{.col_name = "", .col_id = column_id, .type = DataTypePtr{}};
    };
    auto rs_operator = DM::FilterParser::parseDAGQuery(
        *dag_query,
        table_scan.getColumns(),
        std::move(create_attr_by_column_id),
        log);
    if (likely(rs_operator != DM::EMPTY_RS_OPERATOR))
        LOG_DEBUG(log, "Rough set filter: {}", rs_operator->toDebugString());
    return rs_operator;
}

std::variant<DM::Remote::RNWorkersPtr, DM::SegmentReadTaskPoolPtr> StorageDisaggregated::packSegmentReadTasks(
    const Context & db_context,
    DM::SegmentReadTasks && read_tasks,
    const DM::ColumnDefinesPtr & column_defines,
    size_t num_streams,
    int extra_table_id_index)
{
    const auto & executor_id = table_scan.getTableScanExecutorID();

    auto rs_operator = buildRSOperator(db_context, column_defines);
    auto push_down_filter = StorageDeltaMerge::buildPushDownFilter(
        rs_operator,
        table_scan.getColumns(),
        table_scan.getPushedDownFilters(),
        *column_defines,
        db_context,
        log);
    const auto read_mode = DM::DeltaMergeStore::getReadMode(
        db_context,
        table_scan.isFastScan(),
        table_scan.keepOrder(),
        push_down_filter);
    const UInt64 read_tso = sender_target_mpp_task_id.gather_id.query_id.start_ts;
    const auto enable_read_thread = db_context.getSettingsRef().dt_enable_read_thread;
    LOG_INFO(
        log,
        "packSegmentReadTasks: enable_read_thread={} read_mode={} is_fast_scan={} keep_order={} task_count={} "
        "num_streams={} column_defines={}",
        enable_read_thread,
        magic_enum::enum_name(read_mode),
        table_scan.isFastScan(),
        table_scan.keepOrder(),
        read_tasks.size(),
        num_streams,
        *column_defines);

    if (enable_read_thread)
    {
        return std::make_shared<DM::SegmentReadTaskPool>(
            extra_table_id_index,
            *column_defines,
            push_down_filter,
            read_tso,
            db_context.getSettingsRef().max_block_size,
            read_mode,
            std::move(read_tasks),
            /*after_segment_read*/ [](const DM::DMContextPtr &, const DM::SegmentPtr &) {},
            executor_id,
            /*enable_read_thread*/ true,
            num_streams,
            context.getDAGContext()->getResourceGroupName());
    }
    else
    {
        return DM::Remote::RNWorkers::create(
            db_context,
            std::move(read_tasks),
            {
                .log = log->getChild(executor_id),
                .columns_to_read = column_defines,
                .read_tso = read_tso,
                .push_down_filter = push_down_filter,
                .read_mode = read_mode,
            },
            num_streams);
    }
}

struct InputStreamBuilder
{
    const String & tracing_id;
    const DM::ColumnDefinesPtr & columns_to_read;
    int extra_table_id_index;

    BlockInputStreamPtr operator()(DM::Remote::RNWorkersPtr & workers) const
    {
        return DM::Remote::RNSegmentInputStream::create(DM::Remote::RNSegmentInputStream::Options{
            .debug_tag = tracing_id,
            .workers = workers,
            .columns_to_read = *columns_to_read,
            .extra_table_id_index = extra_table_id_index,
        });
    }

    BlockInputStreamPtr operator()(DM::SegmentReadTaskPoolPtr & read_tasks) const
    {
        return std::make_shared<DM::UnorderedInputStream>(
            read_tasks,
            *columns_to_read,
            extra_table_id_index,
            tracing_id);
    }
};


void StorageDisaggregated::buildRemoteSegmentInputStreams(
    const Context & db_context,
    DM::SegmentReadTasks && read_tasks,
    size_t num_streams,
    DAGPipeline & pipeline)
{
    // Build the input streams to read blocks from remote segments
    auto [column_defines, extra_table_id_index] = genColumnDefinesForDisaggregatedRead(table_scan);
    auto packed_read_tasks
        = packSegmentReadTasks(db_context, std::move(read_tasks), column_defines, num_streams, extra_table_id_index);
    RUNTIME_CHECK(num_streams > 0, num_streams);
    pipeline.streams.reserve(num_streams);

    InputStreamBuilder builder{
        .tracing_id = log->identifier(),
        .columns_to_read = column_defines,
        .extra_table_id_index = extra_table_id_index,
    };
    for (size_t stream_idx = 0; stream_idx < num_streams; ++stream_idx)
    {
        pipeline.streams.emplace_back(std::visit(builder, packed_read_tasks));
    }

    const auto & executor_id = table_scan.getTableScanExecutorID();
    auto * dag_context = db_context.getDAGContext();
    auto & table_scan_io_input_streams = dag_context->getInBoundIOInputStreamsMap()[executor_id];
    auto & profile_streams = dag_context->getProfileStreamsMap()[executor_id];
    pipeline.transform([&](auto & stream) {
        table_scan_io_input_streams.push_back(stream);
        profile_streams.push_back(stream);
    });
}

struct SrouceOpBuilder
{
    const String & tracing_id;
    const DM::ColumnDefinesPtr & column_defines;
    int extra_table_id_index;
    PipelineExecutorContext & exec_context;

    SourceOpPtr operator()(DM::Remote::RNWorkersPtr & workers) const
    {
        return DM::Remote::RNSegmentSourceOp::create({
            .debug_tag = tracing_id,
            .exec_context = exec_context,
            .workers = workers,
            .columns_to_read = *column_defines,
            .extra_table_id_index = extra_table_id_index,
        });
    }

    SourceOpPtr operator()(DM::SegmentReadTaskPoolPtr & read_tasks) const
    {
        return std::make_unique<UnorderedSourceOp>(
            exec_context,
            read_tasks,
            *column_defines,
            extra_table_id_index,
            tracing_id);
    }
};

void StorageDisaggregated::buildRemoteSegmentSourceOps(
    PipelineExecutorContext & exec_context,
    PipelineExecGroupBuilder & group_builder,
    const Context & db_context,
    DM::SegmentReadTasks && read_tasks,
    size_t num_streams)
{
    // Build the input streams to read blocks from remote segments
    auto [column_defines, extra_table_id_index] = genColumnDefinesForDisaggregatedRead(table_scan);
    auto packed_read_tasks
        = packSegmentReadTasks(db_context, std::move(read_tasks), column_defines, num_streams, extra_table_id_index);

    RUNTIME_CHECK(num_streams > 0, num_streams);
    SrouceOpBuilder builder{
        .tracing_id = log->identifier(),
        .column_defines = column_defines,
        .extra_table_id_index = extra_table_id_index,
        .exec_context = exec_context,
    };
    for (size_t i = 0; i < num_streams; ++i)
    {
        group_builder.addConcurrency(std::visit(builder, packed_read_tasks));
    }
    db_context.getDAGContext()->addInboundIOProfileInfos(
        table_scan.getTableScanExecutorID(),
        group_builder.getCurIOProfileInfos());
    db_context.getDAGContext()->addOperatorProfileInfos(
        table_scan.getTableScanExecutorID(),
        group_builder.getCurProfileInfos());
}

size_t StorageDisaggregated::getBuildTaskRPCTimeout() const
{
    return context.getSettingsRef().disagg_build_task_timeout;
}

size_t StorageDisaggregated::getBuildTaskIOThreadPoolTimeout() const
{
    return context.getSettingsRef().disagg_build_task_timeout * 1000000;
}

} // namespace DB
