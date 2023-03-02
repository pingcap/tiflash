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
#include <Flash/Coprocessor/FilterConditions.h>
#include <Flash/Coprocessor/GenSchemaAndColumn.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RequestUtils.h>
#include <Flash/Disaggregated/RNPagePreparer.h>
#include <Flash/Disaggregated/RNPageReceiver.h>
#include <Flash/Disaggregated/RNPageReceiverContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <Storages/DeltaMerge/Remote/DisaggTaskId.h>
#include <Storages/DeltaMerge/Remote/Proto/remote.pb.h>
#include <Storages/DeltaMerge/Remote/RNRemoteReadTask.h>
#include <Storages/DeltaMerge/Remote/RNRemoteSegmentThreadInputStream.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageDisaggregated.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <Storages/Transaction/Types.h>
#include <kvproto/disaggregated.pb.h>
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
struct RpcTypeTraits<disaggregated::EstablishDisaggTaskRequest>
{
    using RequestType = disaggregated::EstablishDisaggTaskRequest;
    using ResultType = disaggregated::EstablishDisaggTaskResponse;
    static const char * err_msg() { return "EstablishDisaggregatedTask Failed"; } // NOLINT(readability-identifier-naming)
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
extern const int REGION_EPOCH_NOT_MATCH;
} // namespace ErrorCodes

BlockInputStreams StorageDisaggregated::readFromWriteNode(
    const Context & db_context,
    unsigned num_streams)
{
    DM::RNRemoteReadTaskPtr remote_read_tasks;

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
            if (e.code() != ErrorCodes::REGION_EPOCH_NOT_MATCH)
                throw;

            bo.backoff(pingcap::kv::boRegionMiss, pingcap::Exception(e.message()));
            LOG_INFO(log, "meets region epoch not match, retry to build remote read tasks");
        }
    }

    // Build InputStream according to the remote segment read tasks
    DAGPipeline pipeline;
    buildRemoteSegmentInputStreams(db_context, remote_read_tasks, num_streams, pipeline);
    NamesAndTypes source_columns = genNamesAndTypesForExchangeReceiver(table_scan);
    filterConditions(std::move(source_columns), pipeline);
    return pipeline.streams;
}

struct DisaggregatedExecutionSummary
{
    size_t establish_rpc_ms{0};
    size_t build_remote_task_ms{0};
};

DM::RNRemoteReadTaskPtr StorageDisaggregated::buildDisaggregatedTask(
    const Context & db_context,
    const std::vector<pingcap::coprocessor::BatchCopTask> & batch_cop_tasks)
{
    size_t tasks_n = batch_cop_tasks.size();

    // Collect the response from write nodes and build the remote tasks
    std::vector<DM::RNRemoteTableReadTaskPtr> remote_tasks(tasks_n, nullptr);
    // The execution summaries
    std::vector<DisaggregatedExecutionSummary> summaries(tasks_n);

    auto thread_manager = newThreadManager();
    auto * cluster = context.getTMTContext().getKVCluster();
    const auto & executor_id = table_scan.getTableScanExecutorID();
    const DM::DisaggTaskId task_id(context.getDAGContext()->getMPPTaskId(), executor_id);

    for (size_t idx = 0; idx < tasks_n; ++idx)
    {
        thread_manager->schedule(
            true,
            "EstablishDisaggregated",
            [&, idx] {
                Stopwatch watch;

                const auto & cop_task = batch_cop_tasks[idx];
                auto req = buildDisaggregatedTaskForNode(db_context, cop_task);

                auto call = pingcap::kv::RpcCall<disaggregated::EstablishDisaggTaskRequest>(req);
                cluster->rpc_client->sendRequest(req->address(), call, DEFAULT_DISAGG_TASK_BUILD_TIMEOUT_SEC);
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
                        // Refresh region cache and throw an exception for retrying
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
                        // Meet other errors... May be not retirable?
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                                        "EstablishDisaggregatedTask failed, addr={} error={} code={}",
                                        cop_task.store_addr,
                                        resp->error().msg(),
                                        resp->error().code());
                    }
                }

                const DM::DisaggTaskId snapshot_id(resp->snapshot_id());
                LOG_DEBUG(
                    log,
                    "Received EstablishDisaggregated response, store={} snap_id={} addr={} resp.num_tables={}",
                    resp->store_id(),
                    snapshot_id,
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
                    DB::DM::RemotePb::RemotePhysicalTable table;
                    auto parse_ok = table.ParseFromString(physical_table);
                    RUNTIME_CHECK_MSG(parse_ok, "failed to deserialize RemotePhysicalTable from response");

                    Stopwatch watch_table;

                    remote_tasks[idx] = DM::RNRemoteTableReadTask::buildFrom(
                        db_context,
                        resp->store_id(),
                        cop_task.store_addr,
                        snapshot_id,
                        table,
                        log);

                    LOG_DEBUG(
                        log,
                        "Build RNRemoteTableReadTask finished, elapsed={}s store={} addr={} segments={} task_id={}",
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

    auto read_task = std::make_shared<DM::RNRemoteReadTask>(std::move(remote_tasks));

    const auto avg_establish_rpc_ms = std::accumulate(summaries.begin(), summaries.end(), 0.0, [](double lhs, const DisaggregatedExecutionSummary & rhs) -> double { return lhs + rhs.establish_rpc_ms; }) / summaries.size();
    const auto avg_build_remote_task_ms = std::accumulate(summaries.begin(), summaries.end(), 0.0, [](double lhs, const DisaggregatedExecutionSummary & rhs) -> double { return lhs + rhs.build_remote_task_ms; }) / summaries.size();
    LOG_INFO(log, "establish disaggregated task rpc cost {:.2f}ms, build remote tasks cost {:.2f}ms", avg_establish_rpc_ms, avg_build_remote_task_ms);

    return read_task;
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
StorageDisaggregated::buildDisaggregatedTaskForNode(
    const Context & db_context,
    const pingcap::coprocessor::BatchCopTask & batch_cop_task)
{
    const auto & settings = db_context.getSettingsRef();
    auto establish_req = std::make_shared<disaggregated::EstablishDisaggTaskRequest>();
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
    // how long the task is valid on the write node
    establish_req->set_timeout_s(DEFAULT_DISAGG_TASK_TIMEOUT_SEC);
    establish_req->set_address(batch_cop_task.store_addr);
    establish_req->set_schema_ver(settings.schema_version);

    RequestUtils::setUpRegionInfos(batch_cop_task, establish_req);

    {
        // Setup the encoded plan
        const auto * dag_req = context.getDAGContext()->dag_request;
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
        return DM::EMPTY_FILTER;

    const bool enable_rs_filter = db_context.getSettingsRef().dt_enable_rough_set_filter;
    if (!enable_rs_filter)
    {
        LOG_DEBUG(log, "Rough set filter is disabled.");
        return DM::EMPTY_FILTER;
    }

    auto dag_query = std::make_unique<DAGQueryInfo>(
        filter_conditions.conditions,
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
    const DM::RNRemoteReadTaskPtr & remote_read_tasks,
    size_t num_streams,
    DAGPipeline & pipeline)
{
    LOG_DEBUG(log, "build streams with {} segment tasks, num_streams={}", remote_read_tasks->numSegments(), num_streams);
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

    bool do_prepare = true;

    // Build the input streams to read blocks from remote segments
    auto [column_defines, extra_table_id_index] = genColumnDefinesForDisaggregatedRead(table_scan);
    auto page_downloader = std::make_shared<RNPagePreparer>(
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

        auto sub_streams = DM::RNRemoteSegmentThreadInputStream::buildInputStreams(
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

} // namespace DB
