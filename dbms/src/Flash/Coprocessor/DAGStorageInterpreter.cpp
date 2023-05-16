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

#include <Common/FailPoint.h>
#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/GeneratedColumnPlaceholderBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/MultiplexInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/ChunkCodec.h>
#include <Flash/Coprocessor/CoprocessorReader.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Coprocessor/RemoteRequest.h>
#include <Interpreters/Context.h>
#include <Parsers/makeDummyQuery.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/IManageableStorage.h>
#include <Storages/MutableSupport.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/TMTContext.h>
#include <TiDB/Schema/SchemaSyncer.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop


namespace DB
{
namespace FailPoints
{
extern const char region_exception_after_read_from_storage_some_error[];
extern const char region_exception_after_read_from_storage_all_error[];
extern const char pause_with_alter_locks_acquired[];
extern const char force_remote_read_for_batch_cop[];
extern const char pause_after_copr_streams_acquired[];
extern const char pause_after_copr_streams_acquired_once[];
} // namespace FailPoints

namespace
{
RegionException::RegionReadStatus GetRegionReadStatus(
    const RegionInfo & check_info,
    const RegionPtr & current_region,
    ImutRegionRangePtr & region_range)
{
    if (!current_region)
        return RegionException::RegionReadStatus::NOT_FOUND;
    auto meta_snap = current_region->dumpRegionMetaSnapshot();
    if (meta_snap.ver != check_info.region_version)
        return RegionException::RegionReadStatus::EPOCH_NOT_MATCH;
    // No need to check conf_version if its peer state is normal
    if (current_region->peerState() != raft_serverpb::PeerState::Normal)
        return RegionException::RegionReadStatus::NOT_FOUND;

    region_range = std::move(meta_snap.range);
    return RegionException::RegionReadStatus::OK;
}

std::tuple<std::optional<RegionRetryList>, RegionException::RegionReadStatus>
MakeRegionQueryInfos(
    const TablesRegionInfoMap & dag_region_infos,
    const std::unordered_set<RegionID> & region_force_retry,
    TMTContext & tmt,
    MvccQueryInfo & mvcc_info,
    bool batch_cop [[maybe_unused]])
{
    mvcc_info.regions_query_info.clear();
    RegionRetryList region_need_retry;
    RegionException::RegionReadStatus status_res = RegionException::RegionReadStatus::OK;
    for (const auto & [physical_table_id, regions] : dag_region_infos)
    {
        for (const auto & [id, r] : regions.get())
        {
            if (r.key_ranges.empty())
            {
                throw TiFlashException(
                    fmt::format("Income key ranges is empty for region: {}", r.region_id),
                    Errors::Coprocessor::BadRequest);
            }
            if (region_force_retry.count(id))
            {
                region_need_retry.emplace_back(r);
                status_res = RegionException::RegionReadStatus::NOT_FOUND;
                continue;
            }
            ImutRegionRangePtr region_range{nullptr};
            auto status = GetRegionReadStatus(r, tmt.getKVStore()->getRegion(id), region_range);
            fiu_do_on(FailPoints::force_remote_read_for_batch_cop, {
                if (batch_cop)
                    status = RegionException::RegionReadStatus::NOT_FOUND;
            });
            if (status != RegionException::RegionReadStatus::OK)
            {
                region_need_retry.emplace_back(r);
                status_res = status;
                continue;
            }
            RegionQueryInfo info(id, r.region_version, r.region_conf_version, physical_table_id);
            {
                info.range_in_table = region_range->rawKeys();
                for (const auto & p : r.key_ranges)
                {
                    TableID table_id_in_range = -1;
                    if (!computeMappedTableID(*p.first, table_id_in_range) || table_id_in_range != physical_table_id)
                    {
                        throw TiFlashException(
                            fmt::format(
                                "Income key ranges is illegal for region: {}, table id in key range is {}, table id in region is {}",
                                r.region_id,
                                table_id_in_range,
                                physical_table_id),
                            Errors::Coprocessor::BadRequest);
                    }
                    if (p.first->compare(*info.range_in_table.first) < 0 || p.second->compare(*info.range_in_table.second) > 0)
                        throw TiFlashException(
                            fmt::format("Income key ranges is illegal for region: {}", r.region_id),
                            Errors::Coprocessor::BadRequest);
                }
                info.required_handle_ranges = r.key_ranges;
                info.bypass_lock_ts = r.bypass_lock_ts;
            }
            mvcc_info.regions_query_info.emplace_back(std::move(info));
        }
    }
    mvcc_info.concurrent = mvcc_info.regions_query_info.size() > 1 ? 1.0 : 0.0;

    if (region_need_retry.empty())
        return std::make_tuple(std::nullopt, RegionException::RegionReadStatus::OK);
    else
        return std::make_tuple(std::move(region_need_retry), status_res);
}

bool hasRegionToRead(const DAGContext & dag_context, const TiDBTableScan & table_scan)
{
    bool has_region_to_read = false;
    for (const auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & table_regions_info = dag_context.getTableRegionsInfoByTableID(physical_table_id);
        if (!table_regions_info.local_regions.empty() || !table_regions_info.remote_regions.empty())
        {
            has_region_to_read = true;
            break;
        }
    }
    return has_region_to_read;
}

void setQuotaAndLimitsOnTableScan(Context & context, DAGPipeline & pipeline)
{
    const Settings & settings = context.getSettingsRef();

    IProfilingBlockInputStream::LocalLimits limits;
    limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
    limits.size_limits = SizeLimits(settings.max_rows_to_read, settings.max_bytes_to_read, settings.read_overflow_mode);
    limits.max_execution_time = settings.max_execution_time;
    limits.timeout_overflow_mode = settings.timeout_overflow_mode;

    /** Quota and minimal speed restrictions are checked on the initiating server of the request, and not on remote servers,
          *  because the initiating server has a summary of the execution of the request on all servers.
          *
          * But limits on data size to read and maximum execution time are reasonable to check both on initiator and
          *  additionally on each remote server, because these limits are checked per block of data processed,
          *  and remote servers may process way more blocks of data than are received by initiator.
          */
    limits.min_execution_speed = settings.min_execution_speed;
    limits.timeout_before_checking_execution_speed = settings.timeout_before_checking_execution_speed;

    QuotaForIntervals & quota = context.getQuota();

    pipeline.transform([&](auto & stream) {
        if (auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
        {
            p_stream->setLimits(limits);
            p_stream->setQuota(quota);
        }
    });
}

// add timezone cast for timestamp type, this is used to support session level timezone
// <has_cast, extra_cast, project_for_remote_read>
std::tuple<bool, ExpressionActionsPtr, ExpressionActionsPtr> addExtraCastsAfterTs(
    DAGExpressionAnalyzer & analyzer,
    const std::vector<ExtraCastAfterTSMode> & need_cast_column,
    const TiDBTableScan & table_scan)
{
    bool has_need_cast_column = false;
    for (auto b : need_cast_column)
        has_need_cast_column |= (b != ExtraCastAfterTSMode::None);
    if (!has_need_cast_column)
        return {false, nullptr, nullptr};

    auto original_source_columns = analyzer.getCurrentInputColumns();
    ExpressionActionsChain chain;
    analyzer.initChain(chain, original_source_columns);
    // execute timezone cast or duration cast if needed for local table scan
    if (analyzer.appendExtraCastsAfterTS(chain, need_cast_column, table_scan))
    {
        ExpressionActionsPtr extra_cast = chain.getLastActions();
        assert(extra_cast);
        chain.finalize();
        chain.clear();

        // After `analyzer.appendExtraCastsAfterTS`, analyzer.getCurrentInputColumns() has been modified.
        // For remote read, `timezone cast and duration cast` had been pushed down, don't need to execute cast expressions.
        // To keep the schema of local read streams and remote read streams the same, do project action for remote read streams.
        NamesWithAliases project_for_remote_read;
        const auto & after_cast_source_columns = analyzer.getCurrentInputColumns();
        for (size_t i = 0; i < after_cast_source_columns.size(); ++i)
            project_for_remote_read.emplace_back(original_source_columns[i].name, after_cast_source_columns[i].name);
        assert(!project_for_remote_read.empty());
        ExpressionActionsPtr project_for_cop_read = std::make_shared<ExpressionActions>(original_source_columns, analyzer.getContext().getSettingsRef());
        project_for_cop_read->add(ExpressionAction::project(project_for_remote_read));

        return {true, extra_cast, project_for_cop_read};
    }
    else
    {
        return {false, nullptr, nullptr};
    }
}

void injectFailPointForLocalRead([[maybe_unused]] const SelectQueryInfo & query_info)
{
    // Inject failpoint to throw RegionException for testing
    fiu_do_on(FailPoints::region_exception_after_read_from_storage_some_error, {
        const auto & regions_info = query_info.mvcc_query_info->regions_query_info;
        RegionException::UnavailableRegions region_ids;
        for (const auto & info : regions_info)
        {
            if (random() % 100 > 50)
                region_ids.insert(info.region_id);
        }
        throw RegionException(std::move(region_ids), RegionException::RegionReadStatus::NOT_FOUND);
    });
    fiu_do_on(FailPoints::region_exception_after_read_from_storage_all_error, {
        const auto & regions_info = query_info.mvcc_query_info->regions_query_info;
        RegionException::UnavailableRegions region_ids;
        for (const auto & info : regions_info)
            region_ids.insert(info.region_id);
        throw RegionException(std::move(region_ids), RegionException::RegionReadStatus::NOT_FOUND);
    });
}

String genErrMsgForLocalRead(
    const ManageableStoragePtr & storage,
    const TableID & table_id,
    const TableID & logical_table_id)
{
    return table_id == logical_table_id
        ? fmt::format(
            "(while creating InputStreams from storage `{}`.`{}`, table_id: {})",
            storage->getDatabaseName(),
            storage->getTableName(),
            table_id)
        : fmt::format(
            "(while creating InputStreams from storage `{}`.`{}`, table_id: {}, logical_table_id: {})",
            storage->getDatabaseName(),
            storage->getTableName(),
            table_id,
            logical_table_id);
}
} // namespace

DAGStorageInterpreter::DAGStorageInterpreter(
    Context & context_,
    const TiDBTableScan & table_scan_,
    const PushDownFilter & push_down_filter_,
    size_t max_streams_)
    : context(context_)
    , table_scan(table_scan_)
    , push_down_filter(push_down_filter_)
    , max_streams(max_streams_)
    , log(Logger::get(context.getDAGContext()->log ? context.getDAGContext()->log->identifier() : ""))
    , logical_table_id(table_scan.getLogicalTableID())
    , settings(context.getSettingsRef())
    , tmt(context.getTMTContext())
    , mvcc_query_info(new MvccQueryInfo(true, settings.read_tso))
{
    if (unlikely(!hasRegionToRead(dagContext(), table_scan)))
    {
        throw TiFlashException(
            fmt::format("Dag Request does not have region to read for table: {}", logical_table_id),
            Errors::Coprocessor::BadRequest);
    }
}

// Apply learner read to ensure we can get strong consistent with TiKV Region
// leaders. If the local Regions do not match the requested Regions, then build
// request to retry fetching data from other nodes.
void DAGStorageInterpreter::execute(DAGPipeline & pipeline)
{
    prepare();

    executeImpl(pipeline);
}

void DAGStorageInterpreter::executeImpl(DAGPipeline & pipeline)
{
    if (!mvcc_query_info->regions_query_info.empty())
    {
        dagContext().scan_context_map[table_scan.getTableScanExecutorID()] = std::make_shared<DM::ScanContext>();
        buildLocalStreams(pipeline, settings.max_block_size);
    }

    // Should build `remote_requests` and `null_stream` under protect of `table_structure_lock`.
    auto null_stream_if_empty = std::make_shared<NullBlockInputStream>(storage_for_logical_table->getSampleBlockForColumns(required_columns));

    auto remote_requests = buildRemoteRequests();

    // A failpoint to test pause before alter lock released
    FAIL_POINT_PAUSE(FailPoints::pause_with_alter_locks_acquired);
    // Release alter locks
    // The DeltaTree engine ensures that once input streams are created, the caller can get a consistent result
    // from those streams even if DDL operations are applied. Release the alter lock so that reading does not
    // block DDL operations, keep the drop lock so that the storage not to be dropped during reading.
    const TableLockHolders drop_locks = releaseAlterLocks();

    // It is impossible to have no joined stream.
    assert(pipeline.streams_with_non_joined_data.empty());
    // after buildRemoteStreams, remote read stream will be appended in pipeline.streams.
    size_t remote_read_streams_start_index = pipeline.streams.size();

    // For those regions which are not presented in this tiflash node, we will try to fetch streams by key ranges from other tiflash nodes, only happens in batch cop / mpp mode.
    if (!remote_requests.empty())
        buildRemoteStreams(remote_requests, pipeline);

    /// record local and remote io input stream
    auto & table_scan_io_input_streams = dagContext().getInBoundIOInputStreamsMap()[table_scan.getTableScanExecutorID()];
    pipeline.transform([&](auto & stream) { table_scan_io_input_streams.push_back(stream); });

    if (pipeline.streams.empty())
    {
        pipeline.streams.emplace_back(std::move(null_stream_if_empty));
        // reset remote_read_streams_start_index for null_stream_if_empty.
        remote_read_streams_start_index = 1;
    }

    /// We don't want the table to be dropped during the lifetime of this query,
    /// and sometimes if there is no local region, we will use the RemoteBlockInputStream
    /// or even the null_stream to hold the lock.
    pipeline.transform([&](auto & stream) {
        // todo do not need to hold all locks in each stream, if the stream is reading from table a
        //  it only needs to hold the lock of table a
        for (const auto & lock : drop_locks)
            stream->addTableLock(lock);
    });

    /// Set the limits and quota for reading data, the speed and time of the query.
    setQuotaAndLimitsOnTableScan(context, pipeline);
    FAIL_POINT_PAUSE(FailPoints::pause_after_copr_streams_acquired);
    FAIL_POINT_PAUSE(FailPoints::pause_after_copr_streams_acquired_once);

    /// handle generated column if necessary.
    executeGeneratedColumnPlaceholder(remote_read_streams_start_index, generated_column_infos, log, pipeline);
    /// handle timezone/duration cast for local and remote table scan.
    executeCastAfterTableScan(remote_read_streams_start_index, pipeline);
    recordProfileStreams(pipeline, table_scan.getTableScanExecutorID());

    /// handle pushed down filter for local and remote table scan.
    if (push_down_filter.hasValue())
    {
        executePushedDownFilter(remote_read_streams_start_index, pipeline);
        recordProfileStreams(pipeline, push_down_filter.executor_id);
    }
}

void DAGStorageInterpreter::prepare()
{
    // About why we do learner read before acquiring structure lock on Storage(s).
    // Assume that:
    // 1. Read threads do learner read and wait for the Raft applied index with holding a read lock
    // on "alter lock" of an IStorage X
    // 2. Raft threads try to decode data for Region in the same IStorage X, and find it need to
    // apply DDL operations which acquire write lock on "alter locks"
    // Under this situation, all Raft threads will be stuck by the read threads, but read threads
    // wait for Raft threads to push forward the applied index. Deadlocks happens!!
    // So we must do learner read without structure lock on IStorage. After learner read, acquire the
    // structure lock of IStorage(s) (to avoid concurrent issues between read threads and DDL
    // operations) and build the requested inputstreams. Once the inputstreams build, we should release
    // the alter lock to avoid blocking DDL operations.
    // TODO: If we can acquire a read-only view on the IStorage structure (both `ITableDeclaration`
    // and `TiDB::TableInfo`) we may get this process more simplified. (tiflash/issues/1853)

    // Do learner read
    const DAGContext & dag_context = *context.getDAGContext();
    if (dag_context.isBatchCop() || dag_context.isMPPTask())
        learner_read_snapshot = doBatchCopLearnerRead();
    else
        learner_read_snapshot = doCopLearnerRead();

    // Acquire read lock on `alter lock` and build the requested inputstreams
    storages_with_structure_lock = getAndLockStorages(settings.schema_version);
    assert(storages_with_structure_lock.find(logical_table_id) != storages_with_structure_lock.end());
    storage_for_logical_table = storages_with_structure_lock[logical_table_id].storage;

    std::tie(required_columns, source_columns, is_need_add_cast_column) = getColumnsForTableScan(settings.max_columns_to_read);

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
}

std::tuple<ExpressionActionsPtr, String, ExpressionActionsPtr> DAGStorageInterpreter::buildPushDownFilter()
{
    assert(push_down_filter.hasValue());

    ExpressionActionsChain chain;
    analyzer->initChain(chain, analyzer->getCurrentInputColumns());
    String filter_column_name = analyzer->appendWhere(chain, push_down_filter.conditions);
    ExpressionActionsPtr before_where = chain.getLastActions();
    chain.addStep();

    // remove useless tmp column and keep the schema of local streams and remote streams the same.
    NamesWithAliases project_cols;
    for (const auto & col : analyzer->getCurrentInputColumns())
    {
        chain.getLastStep().required_output.push_back(col.name);
        project_cols.emplace_back(col.name, col.name);
    }
    chain.getLastActions()->add(ExpressionAction::project(project_cols));
    ExpressionActionsPtr project_after_where = chain.getLastActions();
    chain.finalize();
    chain.clear();

    return {before_where, filter_column_name, project_after_where};
}

void DAGStorageInterpreter::executePushedDownFilter(
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline)
{
    auto [before_where, filter_column_name, project_after_where] = buildPushDownFilter();

    assert(pipeline.streams_with_non_joined_data.empty());
    assert(remote_read_streams_start_index <= pipeline.streams.size());
    // for remote read, filter had been pushed down, don't need to execute again.
    for (size_t i = 0; i < remote_read_streams_start_index; ++i)
    {
        auto & stream = pipeline.streams[i];
        stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, log->identifier());
        stream->setExtraInfo("push down filter");
        // after filter, do project action to keep the schema of local streams and remote streams the same.
        stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, log->identifier());
        stream->setExtraInfo("projection after push down filter");
    }
}

void DAGStorageInterpreter::executeCastAfterTableScan(
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline)
{
    // execute timezone cast or duration cast if needed for local table scan
    auto [has_cast, extra_cast, project_for_cop_read] = addExtraCastsAfterTs(*analyzer, is_need_add_cast_column, table_scan);
    if (has_cast)
    {
        assert(pipeline.streams_with_non_joined_data.empty());
        assert(remote_read_streams_start_index <= pipeline.streams.size());
        size_t i = 0;
        // local streams
        while (i < remote_read_streams_start_index)
        {
            auto & stream = pipeline.streams[i++];
            stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, log->identifier());
            stream->setExtraInfo("cast after local tableScan");
        }
        // remote streams
        while (i < pipeline.streams.size())
        {
            auto & stream = pipeline.streams[i++];
            stream = std::make_shared<ExpressionBlockInputStream>(stream, project_for_cop_read, log->identifier());
            stream->setExtraInfo("cast after remote tableScan");
        }
    }
}

std::vector<pingcap::coprocessor::copTask> DAGStorageInterpreter::buildCopTasks(const std::vector<RemoteRequest> & remote_requests)
{
    assert(!remote_requests.empty());
#ifndef NDEBUG
    const DAGSchema & schema = remote_requests[0].schema;
    auto schema_match = [&schema](const DAGSchema & other) {
        if (schema.size() != other.size())
            return false;
        for (size_t i = 0; i < schema.size(); ++i)
        {
            if (schema[i].second.tp != other[i].second.tp || schema[i].second.flag != other[i].second.flag)
                return false;
        }
        return true;
    };
    for (size_t i = 1; i < remote_requests.size(); ++i)
    {
        if (!schema_match(remote_requests[i].schema))
            throw Exception("Schema mismatch between different partitions for partition table");
    }
#endif
    pingcap::kv::Cluster * cluster = tmt.getKVCluster();
    std::vector<pingcap::coprocessor::copTask> all_tasks;
    for (const auto & remote_request : remote_requests)
    {
        pingcap::coprocessor::RequestPtr req = std::make_shared<pingcap::coprocessor::Request>();
        remote_request.dag_request.SerializeToString(&(req->data));
        req->tp = pingcap::coprocessor::ReqType::DAG;
        req->start_ts = context.getSettingsRef().read_tso;
        req->schema_version = context.getSettingsRef().schema_version;

        pingcap::kv::Backoffer bo(pingcap::kv::copBuildTaskMaxBackoff);
        pingcap::kv::StoreType store_type = pingcap::kv::StoreType::TiFlash;
        std::multimap<std::string, std::string> meta_data;
        meta_data.emplace("is_remote_read", "true");

        auto tasks = pingcap::coprocessor::buildCopTasks(bo, cluster, remote_request.key_ranges, req, store_type, &Poco::Logger::get("pingcap/coprocessor"), std::move(meta_data), [&] {
            GET_METRIC(tiflash_coprocessor_request_count, type_remote_read_sent).Increment();
        });
        all_tasks.insert(all_tasks.end(), tasks.begin(), tasks.end());
    }
    GET_METRIC(tiflash_coprocessor_request_count, type_remote_read_constructed).Increment(static_cast<double>(all_tasks.size()));
    return all_tasks;
}

void DAGStorageInterpreter::buildRemoteStreams(const std::vector<RemoteRequest> & remote_requests, DAGPipeline & pipeline)
{
    std::vector<pingcap::coprocessor::copTask> all_tasks = buildCopTasks(remote_requests);

    const DAGSchema & schema = remote_requests[0].schema;
    pingcap::kv::Cluster * cluster = tmt.getKVCluster();
    bool has_enforce_encode_type = remote_requests[0].dag_request.has_force_encode_type() && remote_requests[0].dag_request.force_encode_type();
    size_t concurrent_num = std::min<size_t>(context.getSettingsRef().max_threads, all_tasks.size());
    size_t task_per_thread = all_tasks.size() / concurrent_num;
    size_t rest_task = all_tasks.size() % concurrent_num;
    for (size_t i = 0, task_start = 0; i < concurrent_num; ++i)
    {
        size_t task_end = task_start + task_per_thread;
        if (i < rest_task)
            task_end++;
        if (task_end == task_start)
            continue;
        std::vector<pingcap::coprocessor::copTask> tasks(all_tasks.begin() + task_start, all_tasks.begin() + task_end);

        auto coprocessor_reader = std::make_shared<CoprocessorReader>(schema, cluster, tasks, has_enforce_encode_type, 1);
        context.getDAGContext()->addCoprocessorReader(coprocessor_reader);
        BlockInputStreamPtr input = std::make_shared<CoprocessorBlockInputStream>(coprocessor_reader, log->identifier(), table_scan.getTableScanExecutorID(), /*stream_id=*/0);
        pipeline.streams.push_back(input);
        task_start = task_end;
    }
}

DAGContext & DAGStorageInterpreter::dagContext() const
{
    return *context.getDAGContext();
}

void DAGStorageInterpreter::recordProfileStreams(DAGPipeline & pipeline, const String & key)
{
    auto & profile_streams = dagContext().getProfileStreamsMap()[key];
    pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
}

LearnerReadSnapshot DAGStorageInterpreter::doCopLearnerRead()
{
    if (table_scan.isPartitionTableScan())
    {
        throw TiFlashException("Cop request does not support partition table scan", DB::Errors::Coprocessor::BadRequest);
    }

    TablesRegionInfoMap regions_for_local_read;
    for (const auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        regions_for_local_read.emplace(physical_table_id, std::cref(context.getDAGContext()->getTableRegionsInfoByTableID(physical_table_id).local_regions));
    }
    auto [info_retry, status] = MakeRegionQueryInfos(
        regions_for_local_read,
        {},
        tmt,
        *mvcc_query_info,
        false);

    if (info_retry)
        throw RegionException({info_retry->begin()->get().region_id}, status);

    return doLearnerRead(logical_table_id, *mvcc_query_info, max_streams, /*for_batch_cop=*/false, context, log);
}

/// Will assign region_retry_from_local_region
LearnerReadSnapshot DAGStorageInterpreter::doBatchCopLearnerRead()
{
    TablesRegionInfoMap regions_for_local_read;
    for (const auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & local_regions = context.getDAGContext()->getTableRegionsInfoByTableID(physical_table_id).local_regions;
        regions_for_local_read.emplace(physical_table_id, std::cref(local_regions));
    }
    if (regions_for_local_read.empty())
        return {};
    std::unordered_set<RegionID> force_retry;
    for (;;)
    {
        try
        {
            region_retry_from_local_region.clear();
            auto [retry, status] = MakeRegionQueryInfos(
                regions_for_local_read,
                force_retry,
                tmt,
                *mvcc_query_info,
                true);
            UNUSED(status);

            if (retry)
            {
                region_retry_from_local_region = std::move(*retry);
                for (const auto & r : region_retry_from_local_region)
                    force_retry.emplace(r.get().region_id);
            }
            if (mvcc_query_info->regions_query_info.empty())
                return {};
            return doLearnerRead(logical_table_id, *mvcc_query_info, max_streams, /*for_batch_cop=*/true, context, log);
        }
        catch (const LockException & e)
        {
            // We can also use current thread to resolve lock, but it will block next process.
            // So, force this region retry in another thread in CoprocessorBlockInputStream.
            force_retry.emplace(e.region_id);
        }
        catch (const RegionException & e)
        {
            if (tmt.checkShuttingDown())
                throw TiFlashException("TiFlash server is terminating", Errors::Coprocessor::Internal);
            // By now, RegionException will contain all region id of MvccQueryInfo, which is needed by CHSpark.
            // When meeting RegionException, we can let MakeRegionQueryInfos to check in next loop.
            force_retry.insert(e.unavailable_region.begin(), e.unavailable_region.end());
        }
        catch (DB::Exception & e)
        {
            e.addMessage(fmt::format("(while doing learner read for table, logical table_id: {})", logical_table_id));
            throw;
        }
    }
}

std::unordered_map<TableID, SelectQueryInfo> DAGStorageInterpreter::generateSelectQueryInfos()
{
    std::unordered_map<TableID, SelectQueryInfo> ret;
    auto create_query_info = [&](Int64 table_id) -> SelectQueryInfo {
        SelectQueryInfo query_info;
        /// to avoid null point exception
        query_info.query = dagContext().dummy_ast;
        query_info.dag_query = std::make_unique<DAGQueryInfo>(
            push_down_filter.conditions,
            analyzer->getPreparedSets(),
            analyzer->getCurrentInputColumns(),
            context.getTimezoneInfo());
        query_info.req_id = fmt::format("{} table_id={}", log->identifier(), table_id);
        query_info.keep_order = table_scan.keepOrder();
        query_info.is_fast_scan = table_scan.isFastScan();
        return query_info;
    };
    if (table_scan.isPartitionTableScan())
    {
        for (const auto physical_table_id : table_scan.getPhysicalTableIDs())
        {
            SelectQueryInfo query_info = create_query_info(physical_table_id);
            query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>(mvcc_query_info->resolve_locks, mvcc_query_info->read_tso);
            ret.emplace(physical_table_id, std::move(query_info));
        }
        for (auto & r : mvcc_query_info->regions_query_info)
        {
            ret[r.physical_table_id].mvcc_query_info->regions_query_info.push_back(r);
        }
        for (auto & p : ret)
        {
            // todo mvcc_query_info->concurrent is not used anymore, should remove it later
            p.second.mvcc_query_info->concurrent = p.second.mvcc_query_info->regions_query_info.size() > 1 ? 1.0 : 0.0;
        }
    }
    else
    {
        const TableID table_id = logical_table_id;
        SelectQueryInfo query_info = create_query_info(table_id);
        query_info.mvcc_query_info = std::move(mvcc_query_info);
        ret.emplace(table_id, std::move(query_info));
    }
    return ret;
}

bool DAGStorageInterpreter::checkRetriableForBatchCopOrMPP(
    const TableID & table_id,
    const SelectQueryInfo & query_info,
    const RegionException & e,
    int num_allow_retry)
{
    const DAGContext & dag_context = *context.getDAGContext();
    assert((dag_context.isBatchCop() || dag_context.isMPPTask()));
    const auto & dag_regions = dag_context.getTableRegionsInfoByTableID(table_id).local_regions;
    FmtBuffer buffer;
    // Normally there is only few regions need to retry when super batch is enabled. Retry to read
    // from local first. However, too many retry in different places may make the whole process
    // time out of control. We limit the number of retries to 1 now.
    if (likely(num_allow_retry > 0))
    {
        auto & regions_query_info = query_info.mvcc_query_info->regions_query_info;
        for (auto iter = regions_query_info.begin(); iter != regions_query_info.end(); /**/)
        {
            if (e.unavailable_region.find(iter->region_id) != e.unavailable_region.end())
            {
                // move the error regions info from `query_info.mvcc_query_info->regions_query_info` to `region_retry_from_local_region`
                if (auto region_iter = dag_regions.find(iter->region_id); likely(region_iter != dag_regions.end()))
                {
                    region_retry_from_local_region.emplace_back(region_iter->second);
                    buffer.fmtAppend("{},", region_iter->first);
                }
                iter = regions_query_info.erase(iter);
            }
            else
            {
                ++iter;
            }
        }
        LOG_WARNING(
            log,
            "RegionException after read from storage, regions [{}], message: {}{}",
            buffer.toString(),
            e.message(),
            (regions_query_info.empty() ? "" : ", retry to read from local"));
        // no available region in local, break retry loop
        // otherwise continue to retry read from local storage
        return !regions_query_info.empty();
    }
    else
    {
        // push all regions to `region_retry_from_local_region` to retry from other tiflash nodes
        for (const auto & region : query_info.mvcc_query_info->regions_query_info)
        {
            auto iter = dag_regions.find(region.region_id);
            if (likely(iter != dag_regions.end()))
            {
                region_retry_from_local_region.emplace_back(iter->second);
                buffer.fmtAppend("{},", iter->first);
            }
        }
        LOG_WARNING(log, "RegionException after read from storage, regions [{}], message: {}", buffer.toString(), e.message());
        return false; // break retry loop
    }
}

void DAGStorageInterpreter::buildLocalStreamsForPhysicalTable(
    const TableID & table_id,
    const SelectQueryInfo & query_info,
    DAGPipeline & pipeline,
    size_t max_block_size)
{
    size_t region_num = query_info.mvcc_query_info->regions_query_info.size();
    if (region_num == 0)
        return;

    assert(storages_with_structure_lock.find(table_id) != storages_with_structure_lock.end());
    auto & storage = storages_with_structure_lock[table_id].storage;

    const DAGContext & dag_context = *context.getDAGContext();
    for (int num_allow_retry = 1; num_allow_retry >= 0; --num_allow_retry)
    {
        try
        {
            QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
            const auto & scan_context = dag_context.scan_context_map.at(table_scan.getTableScanExecutorID());

            // We want to collect performance metrics in storage level, thus we need read with scan_context here.
            // while IStorage::read() can't support it, and only StorageDeltaMerge support to read with scan_context to collect the information.
            // Thus, storage must cast to StorageDeltaMergePtr here to call the corresponding read() function.
            StorageDeltaMergePtr delta_merge_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
            RUNTIME_CHECK_MSG(delta_merge_storage != nullptr, "delta_merge_storage which cast from storage is null");
            pipeline.streams = delta_merge_storage->read(required_columns, query_info, context, from_stage, max_block_size, max_streams, scan_context);

            injectFailPointForLocalRead(query_info);

            // After getting streams from storage, we need to validate whether Regions have changed or not after learner read.
            // (by calling `validateQueryInfo`). In case the key ranges of Regions have changed (Region merge/split), those `streams`
            // may contain different data other than expected.
            validateQueryInfo(*query_info.mvcc_query_info, learner_read_snapshot, tmt, log);
            break;
        }
        catch (RegionException & e)
        {
            /// Recover from region exception for batchCop/MPP
            if (dag_context.isBatchCop() || dag_context.isMPPTask())
            {
                // clean all streams from local because we are not sure the correctness of those streams
                pipeline.streams.clear();
                if (likely(checkRetriableForBatchCopOrMPP(table_id, query_info, e, num_allow_retry)))
                    continue;
                else
                    break;
            }
            else
            {
                // Throw an exception for TiDB / TiSpark to retry
                e.addMessage(genErrMsgForLocalRead(storage, table_id, logical_table_id));
                throw;
            }
        }
        catch (DB::Exception & e)
        {
            /// Other unknown exceptions
            e.addMessage(genErrMsgForLocalRead(storage, table_id, logical_table_id));
            throw;
        }
    }
}

void DAGStorageInterpreter::buildLocalStreams(DAGPipeline & pipeline, size_t max_block_size)
{
    const DAGContext & dag_context = *context.getDAGContext();
    size_t total_local_region_num = mvcc_query_info->regions_query_info.size();
    if (total_local_region_num == 0)
        return;
    const auto table_query_infos = generateSelectQueryInfos();
    bool has_multiple_partitions = table_query_infos.size() > 1;
    // MultiPartitionStreamPool will be disabled in no partition mode or single-partition case
    std::shared_ptr<MultiPartitionStreamPool> stream_pool = has_multiple_partitions ? std::make_shared<MultiPartitionStreamPool>() : nullptr;
    for (const auto & table_query_info : table_query_infos)
    {
        DAGPipeline current_pipeline;
        const TableID table_id = table_query_info.first;
        const SelectQueryInfo & query_info = table_query_info.second;
        buildLocalStreamsForPhysicalTable(table_id, query_info, current_pipeline, max_block_size);
        if (has_multiple_partitions)
            stream_pool->addPartitionStreams(current_pipeline.streams);
        else
            pipeline.streams.insert(pipeline.streams.end(), current_pipeline.streams.begin(), current_pipeline.streams.end());
    }
    if (has_multiple_partitions)
    {
        String req_info = dag_context.isMPPTask() ? dag_context.getMPPTaskId().toString() : "";
        int exposed_streams_cnt = std::min(static_cast<int>(max_streams), stream_pool->addedStreamsCnt());
        for (int i = 0; i < exposed_streams_cnt; ++i)
        {
            pipeline.streams.push_back(std::make_shared<MultiplexInputStream>(stream_pool, req_info));
        }
    }
}

std::unordered_map<TableID, DAGStorageInterpreter::StorageWithStructureLock> DAGStorageInterpreter::getAndLockStorages(Int64 query_schema_version)
{
    std::unordered_map<TableID, DAGStorageInterpreter::StorageWithStructureLock> storages_with_lock;
    if (unlikely(query_schema_version == DEFAULT_UNSPECIFIED_SCHEMA_VERSION))
    {
        auto logical_table_storage = tmt.getStorages().get(logical_table_id);
        if (!logical_table_storage)
        {
            throw TiFlashException(fmt::format("Table {} doesn't exist.", logical_table_id), Errors::Table::NotExists);
        }
        storages_with_lock[logical_table_id] = {logical_table_storage, logical_table_storage->lockStructureForShare(context.getCurrentQueryId())};
        if (table_scan.isPartitionTableScan())
        {
            for (auto const physical_table_id : table_scan.getPhysicalTableIDs())
            {
                auto physical_table_storage = tmt.getStorages().get(physical_table_id);
                if (!physical_table_storage)
                {
                    throw TiFlashException(fmt::format("Table {} doesn't exist.", physical_table_id), Errors::Table::NotExists);
                }
                storages_with_lock[physical_table_id] = {physical_table_storage, physical_table_storage->lockStructureForShare(context.getCurrentQueryId())};
            }
        }
        return storages_with_lock;
    }

    auto global_schema_version = tmt.getSchemaSyncer()->getCurrentVersion();

    /// Align schema version under the read lock.
    /// Return: [storage, table_structure_lock, storage_schema_version, ok]
    auto get_and_lock_storage = [&](bool schema_synced, TableID table_id) -> std::tuple<ManageableStoragePtr, TableStructureLockHolder, Int64, bool> {
        /// Get storage in case it's dropped then re-created.
        // If schema synced, call getTable without try, leading to exception on table not existing.
        auto table_store = tmt.getStorages().get(table_id);
        if (!table_store)
        {
            if (schema_synced)
                throw TiFlashException(fmt::format("Table {} doesn't exist.", table_id), Errors::Table::NotExists);
            else
                return {{}, {}, {}, false};
        }

        if (unlikely(table_store->engineType() != ::TiDB::StorageEngine::DT))
        {
            throw TiFlashException(
                fmt::format(
                    "Specifying schema_version for non-managed storage: {}, table: {}, id: {} is not allowed",
                    table_store->getName(),
                    table_store->getTableName(),
                    table_id),
                Errors::Coprocessor::Internal);
        }

        auto lock = table_store->lockStructureForShare(context.getCurrentQueryId());

        /// Check schema version, requiring TiDB/TiSpark and TiFlash both use exactly the same schema.
        // We have three schema versions, two in TiFlash:
        // 1. Storage: the version that this TiFlash table (storage) was last altered.
        // 2. Global: the version that TiFlash global schema is at.
        // And one from TiDB/TiSpark:
        // 3. Query: the version that TiDB/TiSpark used for this query.
        auto storage_schema_version = table_store->getTableInfo().schema_version;
        // Not allow storage > query in any case, one example is time travel queries.
        if (storage_schema_version > query_schema_version)
            throw TiFlashException(
                fmt::format("Table {} schema version {} newer than query schema version {}", table_id, storage_schema_version, query_schema_version),
                Errors::Table::SchemaVersionError);
        // From now on we have storage <= query.
        // If schema was synced, it implies that global >= query, as mentioned above we have storage <= query, we are OK to serve.
        if (schema_synced)
            return {table_store, lock, storage_schema_version, true};
        // From now on the schema was not synced.
        // 1. storage == query, TiDB/TiSpark is using exactly the same schema that altered this table, we are just OK to serve.
        // 2. global >= query, TiDB/TiSpark is using a schema older than TiFlash global, but as mentioned above we have storage <= query,
        // meaning that the query schema is still newer than the time when this table was last altered, so we still OK to serve.
        if (storage_schema_version == query_schema_version || global_schema_version >= query_schema_version)
            return {table_store, lock, storage_schema_version, true};
        // From now on we have global < query.
        // Return false for outer to sync and retry.
        return {nullptr, {}, DEFAULT_UNSPECIFIED_SCHEMA_VERSION, false};
    };

    auto get_and_lock_storages = [&](bool schema_synced) -> std::tuple<std::vector<ManageableStoragePtr>, std::vector<TableStructureLockHolder>, std::vector<Int64>, bool> {
        std::vector<ManageableStoragePtr> table_storages;
        std::vector<TableStructureLockHolder> table_locks;
        std::vector<Int64> table_schema_versions;
        auto [logical_table_storage, logical_table_lock, logical_table_storage_schema_version, ok] = get_and_lock_storage(schema_synced, logical_table_id);
        if (!ok)
            return {{}, {}, {}, false};
        table_storages.emplace_back(std::move(logical_table_storage));
        table_locks.emplace_back(std::move(logical_table_lock));
        table_schema_versions.push_back(logical_table_storage_schema_version);
        if (!table_scan.isPartitionTableScan())
        {
            return {table_storages, table_locks, table_schema_versions, true};
        }
        for (auto const physical_table_id : table_scan.getPhysicalTableIDs())
        {
            auto [physical_table_storage, physical_table_lock, physical_table_storage_schema_version, ok] = get_and_lock_storage(schema_synced, physical_table_id);
            if (!ok)
            {
                return {{}, {}, {}, false};
            }
            table_storages.emplace_back(std::move(physical_table_storage));
            table_locks.emplace_back(std::move(physical_table_lock));
            table_schema_versions.push_back(physical_table_storage_schema_version);
        }
        return {table_storages, table_locks, table_schema_versions, true};
    };

    auto log_schema_version = [&](const String & result, const std::vector<Int64> & storage_schema_versions) {
        FmtBuffer buffer;
        buffer.fmtAppend("Table {} schema {} Schema version [storage, global, query]: [{}, {}, {}]", logical_table_id, result, storage_schema_versions[0], global_schema_version, query_schema_version);
        if (table_scan.isPartitionTableScan())
        {
            assert(storage_schema_versions.size() == 1 + table_scan.getPhysicalTableIDs().size());
            for (size_t i = 0; i < table_scan.getPhysicalTableIDs().size(); ++i)
            {
                const auto physical_table_id = table_scan.getPhysicalTableIDs()[i];
                buffer.fmtAppend(", Table {} schema {} Schema version [storage, global, query]: [{}, {}, {}]", physical_table_id, result, storage_schema_versions[1 + i], global_schema_version, query_schema_version);
            }
        }
        return buffer.toString();
    };

    auto sync_schema = [&] {
        auto start_time = Clock::now();
        GET_METRIC(tiflash_schema_trigger_count, type_cop_read).Increment();
        tmt.getSchemaSyncer()->syncSchemas(context);
        auto schema_sync_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();
        LOG_INFO(log, "Table {} schema sync cost {}ms.", logical_table_id, schema_sync_cost);
    };

    /// Try get storage and lock once.
    auto [storages, locks, storage_schema_versions, ok] = get_and_lock_storages(false);
    if (ok)
    {
        LOG_INFO(log, "{}", log_schema_version("OK, no syncing required.", storage_schema_versions));
    }
    else
    /// If first try failed, sync schema and try again.
    {
        LOG_INFO(log, "not OK, syncing schemas.");

        sync_schema();

        std::tie(storages, locks, storage_schema_versions, ok) = get_and_lock_storages(true);
        if (ok)
        {
            LOG_INFO(log, "{}", log_schema_version("OK after syncing.", storage_schema_versions));
        }
        else
            throw TiFlashException("Shouldn't reach here", Errors::Coprocessor::Internal);
    }
    for (size_t i = 0; i < storages.size(); ++i)
    {
        auto const table_id = storages[i]->getTableInfo().id;
        storages_with_lock[table_id] = {std::move(storages[i]), std::move(locks[i])};
    }
    return storages_with_lock;
}

std::tuple<Names, NamesAndTypes, std::vector<ExtraCastAfterTSMode>> DAGStorageInterpreter::getColumnsForTableScan(Int64 max_columns_to_read)
{
    // todo handle alias column
    if (max_columns_to_read && table_scan.getColumnSize() > max_columns_to_read)
    {
        throw TiFlashException(
            fmt::format("Limit for number of columns to read exceeded. Requested: {}, maximum: {}", table_scan.getColumnSize(), max_columns_to_read),
            Errors::BroadcastJoin::TooManyColumns);
    }

    Names required_columns_tmp;
    required_columns_tmp.reserve(table_scan.getColumnSize());
    NamesAndTypes source_columns_tmp;
    source_columns_tmp.reserve(table_scan.getColumnSize());
    std::vector<ExtraCastAfterTSMode> need_cast_column;
    need_cast_column.reserve(table_scan.getColumnSize());
    String handle_column_name = MutableSupport::tidb_pk_column_name;
    if (auto pk_handle_col = storage_for_logical_table->getTableInfo().getPKHandleColumn())
        handle_column_name = pk_handle_col->get().name;

    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        auto const & ci = table_scan.getColumns()[i];
        auto tidb_ci = TiDB::toTiDBColumnInfo(ci);
        ColumnID cid = ci.column_id();

        if (tidb_ci.hasGeneratedColumnFlag())
        {
            LOG_DEBUG(log, "got column({}) with generated column flag", i);
            const auto & data_type = getDataTypeByColumnInfoForComputingLayer(tidb_ci);
            const auto & col_name = GeneratedColumnPlaceholderBlockInputStream::getColumnName(i);
            generated_column_infos.push_back(std::make_tuple(i, col_name, data_type));
            source_columns_tmp.emplace_back(NameAndTypePair{col_name, data_type});
            need_cast_column.push_back(ExtraCastAfterTSMode::None);
            continue;
        }
        // Column ID -1 return the handle column
        String name;
        if (cid == TiDBPkColumnID)
            name = handle_column_name;
        else if (cid == ExtraTableIDColumnID)
            name = MutableSupport::extra_table_id_column_name;
        else
            name = storage_for_logical_table->getTableInfo().getColumnName(cid);
        if (cid == ExtraTableIDColumnID)
        {
            NameAndTypePair extra_table_id_column_pair = {name, MutableSupport::extra_table_id_column_type};
            source_columns_tmp.emplace_back(std::move(extra_table_id_column_pair));
        }
        else
        {
            auto pair = storage_for_logical_table->getColumns().getPhysical(name);
            source_columns_tmp.emplace_back(std::move(pair));
        }
        required_columns_tmp.emplace_back(std::move(name));
        if (cid != -1 && ci.tp() == TiDB::TypeTimestamp)
            need_cast_column.push_back(ExtraCastAfterTSMode::AppendTimeZoneCast);
        else if (cid != -1 && ci.tp() == TiDB::TypeTime)
            need_cast_column.push_back(ExtraCastAfterTSMode::AppendDurationCast);
        else
            need_cast_column.push_back(ExtraCastAfterTSMode::None);
    }

    return {required_columns_tmp, source_columns_tmp, need_cast_column};
}

// Build remote requests from `region_retry_from_local_region` and `table_regions_info.remote_regions`
std::vector<RemoteRequest> DAGStorageInterpreter::buildRemoteRequests()
{
    std::vector<RemoteRequest> remote_requests;
    std::unordered_map<Int64, Int64> region_id_to_table_id_map;
    std::unordered_map<Int64, RegionRetryList> retry_regions_map;
    for (const auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & table_regions_info = context.getDAGContext()->getTableRegionsInfoByTableID(physical_table_id);
        for (const auto & e : table_regions_info.local_regions)
            region_id_to_table_id_map[e.first] = physical_table_id;
        for (const auto & r : table_regions_info.remote_regions)
            retry_regions_map[physical_table_id].emplace_back(std::cref(r));
    }

    for (auto & r : region_retry_from_local_region)
    {
        retry_regions_map[region_id_to_table_id_map[r.get().region_id]].emplace_back(r);
    }

    for (const auto physical_table_id : table_scan.getPhysicalTableIDs())
    {
        const auto & retry_regions = retry_regions_map[physical_table_id];
        if (retry_regions.empty())
            continue;

        // Append the region into DAGContext to return them to the upper layer.
        // The upper layer should refresh its cache about these regions.
        for (const auto & r : retry_regions)
            context.getDAGContext()->retry_regions.push_back(r.get());

        remote_requests.push_back(RemoteRequest::build(
            retry_regions,
            *context.getDAGContext(),
            table_scan,
            storages_with_structure_lock[physical_table_id].storage->getTableInfo(),
            push_down_filter,
            log));
    }
    return remote_requests;
}

TableLockHolders DAGStorageInterpreter::releaseAlterLocks()
{
    TableLockHolders drop_locks;
    for (auto storage_with_lock : storages_with_structure_lock)
    {
        drop_locks.emplace_back(std::get<1>(std::move(storage_with_lock.second.lock).release()));
    }
    return drop_locks;
}

} // namespace DB
