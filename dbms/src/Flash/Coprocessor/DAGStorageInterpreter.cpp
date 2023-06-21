// Copyright 2023 PingCAP, Ltd.
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
#include <Flash/Coprocessor/collectOutputFieldTypes.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Operators/BlockInputStreamSourceOp.h>
#include <Operators/CoprocessorReaderSourceOp.h>
#include <Operators/ExpressionTransformOp.h>
#include <Operators/NullSourceOp.h>
#include <Operators/UnorderedSourceOp.h>
#include <Parsers/makeDummyQuery.h>
#include <Storages/DeltaMerge/ReadThread/UnorderedInputStream.h>
#include <Storages/DeltaMerge/Remote/DisaggSnapshot.h>
#include <Storages/DeltaMerge/Remote/WNDisaggSnapshotManager.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/IManageableStorage.h>
#include <Storages/MutableSupport.h>
#include <Storages/S3/S3Common.h>
#include <Storages/StorageDeltaMerge.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TypeMapping.h>
#include <TiDB/Schema/SchemaSyncer.h>
#include <TiDB/Schema/TiDBSchemaManager.h>
#include <common/logger_useful.h>
#include <kvproto/coprocessor.pb.h>
#include <tipb/select.pb.h>

namespace DB
{
namespace FailPoints
{
extern const char region_exception_after_read_from_storage_some_error[];
extern const char region_exception_after_read_from_storage_all_error[];
extern const char pause_with_alter_locks_acquired[];
extern const char force_remote_read_for_batch_cop[];
extern const char force_remote_read_for_batch_cop_once[];
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
    bool batch_cop [[maybe_unused]],
    const LoggerPtr & log)
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
                    fmt::format("Income key ranges is empty for region: {}, version {}, conf_version {}", r.region_id, r.region_version, r.region_conf_version),
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
            fiu_do_on(FailPoints::force_remote_read_for_batch_cop_once, {
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
                                "Income key ranges is illegal for region: {}, version {}, conf_version {}, table id in key range is {}, table id in region is {}",
                                r.region_id,
                                r.region_version,
                                r.region_conf_version,
                                table_id_in_range,
                                physical_table_id),
                            Errors::Coprocessor::BadRequest);
                    }
                    if (p.first->compare(*info.range_in_table.first) < 0 || p.second->compare(*info.range_in_table.second) > 0)
                    {
                        LOG_WARNING(log, fmt::format("Income key ranges is illegal for region: {}, version {}, conf_version {}, request range: [{}, {}), region range: [{}, {})", r.region_id, r.region_version, r.region_conf_version, p.first->toDebugString(), p.second->toDebugString(), info.range_in_table.first->toDebugString(), info.range_in_table.second->toDebugString()));
                        throw TiFlashException(
                            fmt::format("Income key ranges is illegal for region: {}, version {}, conf_version {}",
                                        r.region_id,
                                        r.region_version,
                                        r.region_conf_version),
                            Errors::Coprocessor::BadRequest);
                    }
                }
                info.required_handle_ranges = r.key_ranges;
                info.bypass_lock_ts = r.bypass_lock_ts;
            }
            mvcc_info.regions_query_info.emplace_back(std::move(info));
        }
    }

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

// add timezone cast for timestamp type, this is used to support session level timezone
// <has_cast, extra_cast, project_for_remote_read>
std::pair<bool, ExpressionActionsPtr> addExtraCastsAfterTs(
    DAGExpressionAnalyzer & analyzer,
    const std::vector<UInt8> & may_need_add_cast_column,
    const TiDBTableScan & table_scan)
{
    // if no column need to add cast, return directly
    if (std::find(may_need_add_cast_column.begin(), may_need_add_cast_column.end(), true) == may_need_add_cast_column.end())
        return {false, nullptr};

    ExpressionActionsChain chain;
    // execute timezone cast or duration cast if needed for local table scan
    if (analyzer.appendExtraCastsAfterTS(chain, may_need_add_cast_column, table_scan))
    {
        ExpressionActionsPtr extra_cast = chain.getLastActions();
        assert(extra_cast);
        chain.finalize();
        chain.clear();
        return {true, extra_cast};
    }
    else
    {
        return {false, nullptr};
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
        LOG_WARNING(Logger::get(), "failpoint inject region_exception_after_read_from_storage_some_error, throw RegionException with region_ids={}", region_ids);
        throw RegionException(std::move(region_ids), RegionException::RegionReadStatus::NOT_FOUND);
    });
    fiu_do_on(FailPoints::region_exception_after_read_from_storage_all_error, {
        const auto & regions_info = query_info.mvcc_query_info->regions_query_info;
        RegionException::UnavailableRegions region_ids;
        for (const auto & info : regions_info)
            region_ids.insert(info.region_id);
        LOG_WARNING(Logger::get(), "failpoint inject region_exception_after_read_from_storage_all_error, throw RegionException with region_ids={}", region_ids);
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
            "(while creating read sources from storage `{}`.`{}`, table_id={})",
            storage->getDatabaseName(),
            storage->getTableName(),
            table_id)
        : fmt::format(
            "(while creating read sources from storage `{}`.`{}`, table_id={}, logical_table_id={})",
            storage->getDatabaseName(),
            storage->getTableName(),
            table_id,
            logical_table_id);
}
} // namespace

DAGStorageInterpreter::DAGStorageInterpreter(
    Context & context_,
    const TiDBTableScan & table_scan_,
    const FilterConditions & filter_conditions_,
    size_t max_streams_)
    : context(context_)
    , table_scan(table_scan_)
    , filter_conditions(filter_conditions_)
    , max_streams(max_streams_)
    , log(Logger::get(context.getDAGContext()->log ? context.getDAGContext()->log->identifier() : ""))
    , logical_table_id(table_scan.getLogicalTableID())
    , tmt(context.getTMTContext())
    , mvcc_query_info(new MvccQueryInfo(true, context.getSettingsRef().read_tso))
{
    if (unlikely(!hasRegionToRead(dagContext(), table_scan)))
    {
        throw TiFlashException(
            fmt::format("Dag Request does not have region to read for table: {}", logical_table_id),
            Errors::Coprocessor::BadRequest);
    }
}

void DAGStorageInterpreter::execute(DAGPipeline & pipeline)
{
    prepare(); // learner read

    executeImpl(pipeline);
}

void DAGStorageInterpreter::execute(PipelineExecutorStatus & exec_status, PipelineExecGroupBuilder & group_builder)
{
    prepare(); // learner read

    return executeImpl(exec_status, group_builder);
}

void DAGStorageInterpreter::executeImpl(PipelineExecutorStatus & exec_status, PipelineExecGroupBuilder & group_builder)
{
    auto & dag_context = dagContext();

    auto scan_context = std::make_shared<DM::ScanContext>();
    dag_context.scan_context_map[table_scan.getTableScanExecutorID()] = scan_context;
    mvcc_query_info->scan_context = scan_context;

    if (!mvcc_query_info->regions_query_info.empty())
    {
        buildLocalExec(exec_status, group_builder, context.getSettingsRef().max_block_size);
    }

    // Should build `remote_requests` and `nullSourceOp` under protect of `table_structure_lock`.

    // Note that `buildRemoteRequests` must be called after `buildLocalExec` because
    // `buildLocalExec` will setup `region_retry_from_local_region` and we must
    // retry those regions or there will be data lost.
    auto remote_requests = buildRemoteRequests(scan_context);
    if (dag_context.is_disaggregated_task && !remote_requests.empty())
    {
        // This means RN is sending requests with stale region info, we simply reject the request
        // and ask RN to send requests again with correct region info. When RN updates region info,
        // RN may be sending requests to other WN.

        RegionException::UnavailableRegions region_ids;
        for (const auto & info : context.getDAGContext()->retry_regions)
            region_ids.insert(info.region_id);

        throw RegionException(
            std::move(region_ids),
            RegionException::RegionReadStatus::EPOCH_NOT_MATCH);
    }

    // A failpoint to test pause before alter lock released
    FAIL_POINT_PAUSE(FailPoints::pause_with_alter_locks_acquired);
    // Release alter locks
    // The DeltaTree engine ensures that once sourceOps are created, the caller can get a consistent result
    // from those sourceOps even if DDL operations are applied. Release the alter lock so that reading does not
    // block DDL operations, keep the drop lock so that the storage not to be dropped during reading.
    const TableLockHolders drop_locks = releaseAlterLocks();

    size_t remote_read_start_index = group_builder.concurrency();

    if (!remote_requests.empty())
        buildRemoteExec(exec_status, group_builder, remote_requests);

    /// record profiles of local and remote io source
    dag_context.addInboundIOProfileInfos(table_scan.getTableScanExecutorID(), group_builder.getCurIOProfileInfos());

    if (group_builder.empty())
    {
        group_builder.addConcurrency(std::make_unique<NullSourceOp>(exec_status, storage_for_logical_table->getSampleBlockForColumns(required_columns), log->identifier()));
        // reset remote_read_start_index for null_source_if_empty.
        remote_read_start_index = 1;
    }

    for (const auto & lock : drop_locks)
        dagContext().addTableLock(lock);

    FAIL_POINT_PAUSE(FailPoints::pause_after_copr_streams_acquired);
    FAIL_POINT_PAUSE(FailPoints::pause_after_copr_streams_acquired_once);

    /// handle generated column if necessary.
    executeGeneratedColumnPlaceholder(exec_status, group_builder, remote_read_start_index, generated_column_infos, log);
    NamesAndTypes source_columns;
    source_columns.reserve(table_scan.getColumnSize());
    const auto table_scan_output_header = group_builder.getCurrentHeader();
    for (const auto & col : table_scan_output_header)
        source_columns.emplace_back(col.name, col.type);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
    /// If there is no local source, there is no need to execute cast and push down filter, return directly.
    /// But we should make sure that the analyzer is initialized before return.
    if (remote_read_start_index == 0)
    {
        dag_context.addOperatorProfileInfos(table_scan.getTableScanExecutorID(), group_builder.getCurProfileInfos());
        if (filter_conditions.hasValue())
            dag_context.addOperatorProfileInfos(filter_conditions.executor_id, group_builder.getCurProfileInfos());
        return;
    }
    /// handle timezone/duration cast for local table scan.
    executeCastAfterTableScan(exec_status, group_builder, remote_read_start_index);
    dag_context.addOperatorProfileInfos(table_scan.getTableScanExecutorID(), group_builder.getCurProfileInfos());

    /// handle filter conditions for local and remote table scan.
    if (filter_conditions.hasValue())
    {
        ::DB::executePushedDownFilter(exec_status, group_builder, remote_read_start_index, filter_conditions, *analyzer, log);
        dag_context.addOperatorProfileInfos(filter_conditions.executor_id, group_builder.getCurProfileInfos());
    }
}

void DAGStorageInterpreter::executeImpl(DAGPipeline & pipeline)
{
    auto & dag_context = dagContext();

    auto scan_context = std::make_shared<DM::ScanContext>();
    dag_context.scan_context_map[table_scan.getTableScanExecutorID()] = scan_context;
    mvcc_query_info->scan_context = scan_context;

    if (!mvcc_query_info->regions_query_info.empty())
    {
        buildLocalStreams(pipeline, context.getSettingsRef().max_block_size);
    }

    // Should build `remote_requests` and `null_stream` under protect of `table_structure_lock`.
    auto null_stream_if_empty = std::make_shared<NullBlockInputStream>(storage_for_logical_table->getSampleBlockForColumns(required_columns));

    // Note that `buildRemoteRequests` must be called after `buildLocalStreams` because
    // `buildLocalStreams` will setup `region_retry_from_local_region` and we must
    // retry those regions or there will be data lost.
    auto remote_requests = buildRemoteRequests(scan_context);
    if (dag_context.is_disaggregated_task && !remote_requests.empty())
    {
        // This means RN is sending requests with stale region info, we simply reject the request
        // and ask RN to send requests again with correct region info. When RN updates region info,
        // RN may be sending requests to other WN.

        RegionException::UnavailableRegions region_ids;
        for (const auto & info : context.getDAGContext()->retry_regions)
            region_ids.insert(info.region_id);

        throw RegionException(
            std::move(region_ids),
            RegionException::RegionReadStatus::EPOCH_NOT_MATCH);
    }

    // A failpoint to test pause before alter lock released
    FAIL_POINT_PAUSE(FailPoints::pause_with_alter_locks_acquired);
    // Release alter locks
    // The DeltaTree engine ensures that once input streams are created, the caller can get a consistent result
    // from those streams even if DDL operations are applied. Release the alter lock so that reading does not
    // block DDL operations, keep the drop lock so that the storage not to be dropped during reading.
    const TableLockHolders drop_locks = releaseAlterLocks();

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

    for (const auto & lock : drop_locks)
        dagContext().addTableLock(lock);

    FAIL_POINT_PAUSE(FailPoints::pause_after_copr_streams_acquired);
    FAIL_POINT_PAUSE(FailPoints::pause_after_copr_streams_acquired_once);
    /// handle generated column if necessary.
    executeGeneratedColumnPlaceholder(remote_read_streams_start_index, generated_column_infos, log, pipeline);
    NamesAndTypes source_columns;
    source_columns.reserve(table_scan.getColumnSize());
    const auto table_scan_output_header = pipeline.firstStream()->getHeader();
    for (const auto & col : table_scan_output_header)
        source_columns.emplace_back(col.name, col.type);
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);
    /// If there is no local stream, there is no need to execute cast and push down filter, return directly.
    /// But we should make sure that the analyzer is initialized before return.
    if (remote_read_streams_start_index == 0)
    {
        recordProfileStreams(pipeline, table_scan.getTableScanExecutorID());
        if (filter_conditions.hasValue())
            recordProfileStreams(pipeline, filter_conditions.executor_id);
        return;
    }
    /// handle timezone/duration cast for local and remote table scan.
    executeCastAfterTableScan(remote_read_streams_start_index, pipeline);
    recordProfileStreams(pipeline, table_scan.getTableScanExecutorID());

    /// handle filter conditions for local and remote table scan.
    /// If force_push_down_all_filters_to_scan is set, we will build all filter conditions in scan.
    /// todo add runtime filter in Filter input stream
    if (filter_conditions.hasValue() && likely(!context.getSettingsRef().force_push_down_all_filters_to_scan))
    {
        ::DB::executePushedDownFilter(remote_read_streams_start_index, filter_conditions, *analyzer, log, pipeline);
        recordProfileStreams(pipeline, filter_conditions.executor_id);
    }
}

// here we assume that, if the columns' id and data type in query is the same as the columns in TiDB,
// we think we can directly do read, and don't need sync schema.
// compare the columns in table_scan with the columns in storages, to check if the current schema is satisified this query.
// column.name are always empty from table_scan, and column name is not necessary in read process, so we don't need compare the name here.
std::tuple<bool, String> compareColumns(const TiDBTableScan & table_scan, const DM::ColumnDefines & cur_columns, const DAGContext & dag_context, const LoggerPtr & log)
{
    const auto & columns = table_scan.getColumns();
    std::unordered_map<ColumnID, DM::ColumnDefine> column_id_map;
    for (const auto & column : cur_columns)
    {
        column_id_map[column.id] = column;
    }

    for (const auto & column : columns)
    {
        // Exclude virtual columns, including EXTRA_HANDLE_COLUMN_ID, VERSION_COLUMN_ID,TAG_COLUMN_ID,EXTRA_TABLE_ID_COLUMN_ID
        if (column.id < 0)
        {
            continue;
        }
        auto iter = column_id_map.find(column.id);
        if (iter == column_id_map.end())
        {
            String error_message = fmt::format("the column in the query is not found in current columns, keyspace={} table_id={} column_id={}", dag_context.getKeyspaceID(), table_scan.getLogicalTableID(), column.id);
            LOG_WARNING(log, error_message);
            return std::make_tuple(false, error_message);
        }

        if (getDataTypeByColumnInfo(column)->getName() != iter->second.type->getName())
        {
            String error_message = fmt::format("the column data type in the query is not the same as the current column, keyspace={} table_id={} column_id={} column_type={} query_column_type={}", dag_context.getKeyspaceID(), table_scan.getLogicalTableID(), column.id, iter->second.type->getName(), getDataTypeByColumnInfo(column)->getName());
            LOG_WARNING(log, error_message);
            return std::make_tuple(false, error_message);
        }
    }
    return std::make_tuple(true, "");
}

// Apply learner read to ensure we can get strong consistent with TiKV Region
// leaders. If the local Regions do not match the requested Regions, then build
// request to retry fetching data from other nodes.
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
    if (dag_context.isBatchCop() || dag_context.isMPPTask() || dag_context.is_disaggregated_task)
        learner_read_snapshot = doBatchCopLearnerRead();
    else
        learner_read_snapshot = doCopLearnerRead();

    // Acquire read lock on `alter lock` and build the requested inputstreams
    storages_with_structure_lock = getAndLockStorages(context.getSettingsRef().schema_version);
    assert(storages_with_structure_lock.find(logical_table_id) != storages_with_structure_lock.end());
    storage_for_logical_table = storages_with_structure_lock[logical_table_id].storage;

    std::tie(required_columns, may_need_add_cast_column) = getColumnsForTableScan();
}

void DAGStorageInterpreter::executeCastAfterTableScan(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    size_t remote_read_start_index)
{
    // execute timezone cast or duration cast if needed for local table scan
    auto [has_cast, extra_cast] = addExtraCastsAfterTs(*analyzer, may_need_add_cast_column, table_scan);
    if (has_cast)
    {
        RUNTIME_CHECK(remote_read_start_index <= group_builder.concurrency());
        size_t i = 0;
        // local sources
        while (i < remote_read_start_index)
        {
            auto & builder = group_builder.getCurBuilder(i++);
            builder.appendTransformOp(std::make_unique<ExpressionTransformOp>(exec_status, log->identifier(), extra_cast));
        }
    }
}

void DAGStorageInterpreter::executeCastAfterTableScan(
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline)
{
    // execute timezone cast or duration cast if needed for local table scan
    auto [has_cast, extra_cast] = addExtraCastsAfterTs(*analyzer, may_need_add_cast_column, table_scan);
    if (has_cast)
    {
        RUNTIME_CHECK(remote_read_streams_start_index <= pipeline.streams.size());
        size_t i = 0;
        // local streams
        while (i < remote_read_streams_start_index)
        {
            auto & stream = pipeline.streams[i++];
            stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, log->identifier());
            stream->setExtraInfo("cast after local tableScan");
        }
    }
}

std::vector<pingcap::coprocessor::CopTask> DAGStorageInterpreter::buildCopTasks(const std::vector<RemoteRequest> & remote_requests)
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
    std::vector<pingcap::coprocessor::CopTask> all_tasks;
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

        auto tasks = pingcap::coprocessor::buildCopTasks(bo, cluster, remote_request.key_ranges, req, store_type, dagContext().getKeyspaceID(), &Poco::Logger::get("pingcap/coprocessor"), std::move(meta_data), [&] {
            GET_METRIC(tiflash_coprocessor_request_count, type_remote_read_sent).Increment();
        });
        all_tasks.insert(all_tasks.end(), tasks.begin(), tasks.end());
    }
    GET_METRIC(tiflash_coprocessor_request_count, type_remote_read_constructed).Increment(static_cast<double>(all_tasks.size()));
    return all_tasks;
}

void DAGStorageInterpreter::buildRemoteStreams(const std::vector<RemoteRequest> & remote_requests, DAGPipeline & pipeline)
{
    std::vector<pingcap::coprocessor::CopTask> all_tasks = buildCopTasks(remote_requests);

    const DAGSchema & schema = remote_requests[0].schema;
    pingcap::kv::Cluster * cluster = tmt.getKVCluster();
    bool has_enforce_encode_type = remote_requests[0].dag_request.has_force_encode_type() && remote_requests[0].dag_request.force_encode_type();
    size_t concurrent_num = std::min<size_t>(context.getSettingsRef().max_threads, all_tasks.size());
    size_t task_per_thread = all_tasks.size() / concurrent_num;
    size_t rest_task = all_tasks.size() % concurrent_num;
    pingcap::kv::LabelFilter tiflash_label_filter = S3::ClientFactory::instance().isEnabled() ? pingcap::kv::labelFilterOnlyTiFlashWriteNode : pingcap::kv::labelFilterNoTiFlashWriteNode;
    for (size_t i = 0, task_start = 0; i < concurrent_num; ++i)
    {
        size_t task_end = task_start + task_per_thread;
        if (i < rest_task)
            ++task_end;
        if (task_end == task_start)
            continue;
        std::vector<pingcap::coprocessor::CopTask> tasks(all_tasks.begin() + task_start, all_tasks.begin() + task_end);

        auto coprocessor_reader = std::make_shared<CoprocessorReader>(schema, cluster, tasks, has_enforce_encode_type, 1, tiflash_label_filter);
        context.getDAGContext()->addCoprocessorReader(coprocessor_reader);
        BlockInputStreamPtr input = std::make_shared<CoprocessorBlockInputStream>(coprocessor_reader, log->identifier(), table_scan.getTableScanExecutorID(), /*stream_id=*/0);
        pipeline.streams.push_back(input);
        task_start = task_end;
    }
}

void DAGStorageInterpreter::buildRemoteExec(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    const std::vector<RemoteRequest> & remote_requests)
{
    std::vector<pingcap::coprocessor::CopTask> all_tasks = buildCopTasks(remote_requests);
    const DAGSchema & schema = remote_requests[0].schema;
    pingcap::kv::Cluster * cluster = tmt.getKVCluster();
    bool has_enforce_encode_type = remote_requests[0].dag_request.has_force_encode_type() && remote_requests[0].dag_request.force_encode_type();
    size_t concurrent_num = std::min<size_t>(context.getSettingsRef().max_threads, all_tasks.size());
    size_t task_per_thread = all_tasks.size() / concurrent_num;
    size_t rest_task = all_tasks.size() % concurrent_num;
    pingcap::kv::LabelFilter tiflash_label_filter = pingcap::kv::labelFilterNoTiFlashWriteNode;
    /// TODO: support reading data from write nodes
    for (size_t i = 0, task_start = 0; i < concurrent_num; ++i)
    {
        size_t task_end = task_start + task_per_thread;
        if (i < rest_task)
            ++task_end;
        if (task_end == task_start)
            continue;
        std::vector<pingcap::coprocessor::CopTask> tasks(all_tasks.begin() + task_start, all_tasks.begin() + task_end);

        auto coprocessor_reader = std::make_shared<CoprocessorReader>(schema, cluster, tasks, has_enforce_encode_type, 1, tiflash_label_filter);
        context.getDAGContext()->addCoprocessorReader(coprocessor_reader);

        group_builder.addConcurrency(std::make_unique<CoprocessorReaderSourceOp>(exec_status, log->identifier(), coprocessor_reader));
        task_start = task_end;
    }

    LOG_DEBUG(log, "remote sourceOps built");
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
        false,
        log);

    if (info_retry)
        throw RegionException({info_retry->begin()->get().region_id}, status);

    return doLearnerRead(logical_table_id, *mvcc_query_info, /*for_batch_cop=*/false, context, log);
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
                true,
                log);
            UNUSED(status);

            if (retry)
            {
                region_retry_from_local_region = std::move(*retry);
                for (const auto & r : region_retry_from_local_region)
                    force_retry.emplace(r.get().region_id);
            }
            if (mvcc_query_info->regions_query_info.empty())
                return {};
            return doLearnerRead(logical_table_id, *mvcc_query_info, /*for_batch_cop=*/true, context, log);
        }
        catch (const LockException & e)
        {
            // When this is a disaggregated read task on WN issued by RN, we need RN
            // to take care of retrying.
            if (context.getDAGContext()->is_disaggregated_task)
                throw;

            // We can also use current thread to resolve lock, but it will block next process.
            // So, force this region retry in another thread in CoprocessorBlockInputStream.
            for (const auto & lock : e.locks)
                force_retry.emplace(lock.first);
        }
        catch (const RegionException & e)
        {
            // When this is a disaggregated read task on WN issued by RN, we need RN
            // to take care of retrying.
            if (context.getDAGContext()->is_disaggregated_task)
                throw;

            if (tmt.checkShuttingDown())
                throw TiFlashException("TiFlash server is terminating", Errors::Coprocessor::Internal);
            // By now, RegionException will contain all region id of MvccQueryInfo, which is needed by CHSpark.
            // When meeting RegionException, we can let MakeRegionQueryInfos to check in next loop.
            force_retry.insert(e.unavailable_region.begin(), e.unavailable_region.end());
        }
        catch (DB::Exception & e)
        {
            e.addMessage(fmt::format("(while doing learner read for table, logical table_id={})", logical_table_id));
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
            filter_conditions.conditions,
            table_scan.getPushedDownFilters(),
            table_scan.getColumns(),
            table_scan.getRuntimeFilterIDs(),
            table_scan.getMaxWaitTimeMs(),
            context.getTimezoneInfo());
        query_info.req_id = fmt::format("{} table_id={}", log->identifier(), table_id);
        query_info.keep_order = table_scan.keepOrder();
        query_info.is_fast_scan = table_scan.isFastScan();
        return query_info;
    };
    RUNTIME_CHECK_MSG(mvcc_query_info->scan_context != nullptr, "Unexpected null scan_context");
    if (table_scan.isPartitionTableScan())
    {
        for (const auto physical_table_id : table_scan.getPhysicalTableIDs())
        {
            SelectQueryInfo query_info = create_query_info(physical_table_id);
            query_info.mvcc_query_info = std::make_unique<MvccQueryInfo>(mvcc_query_info->resolve_locks, mvcc_query_info->read_tso, mvcc_query_info->scan_context);
            ret.emplace(physical_table_id, std::move(query_info));
        }
        // Dispatch the regions_query_info to different physical table's query_info
        for (auto & r : mvcc_query_info->regions_query_info)
        {
            ret[r.physical_table_id].mvcc_query_info->regions_query_info.push_back(r);
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

DM::Remote::DisaggPhysicalTableReadSnapshotPtr
DAGStorageInterpreter::buildLocalStreamsForPhysicalTable(
    const TableID & table_id,
    const SelectQueryInfo & query_info,
    DAGPipeline & pipeline,
    size_t max_block_size)
{
    DM::Remote::DisaggPhysicalTableReadSnapshotPtr table_snap;
    size_t region_num = query_info.mvcc_query_info->regions_query_info.size();
    if (region_num == 0)
        return table_snap;

    assert(storages_with_structure_lock.find(table_id) != storages_with_structure_lock.end());
    auto & storage = storages_with_structure_lock[table_id].storage;

    const DAGContext & dag_context = *context.getDAGContext();
    for (int num_allow_retry = 1; num_allow_retry >= 0; --num_allow_retry)
    {
        try
        {
            if (!dag_context.is_disaggregated_task)
            {
                // build local inputstreams
                QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
                pipeline.streams = storage->read(required_columns, query_info, context, from_stage, max_block_size, max_streams);
            }
            else
            {
                // build a snapshot on write node
                StorageDeltaMergePtr delta_merge_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
                RUNTIME_CHECK_MSG(delta_merge_storage != nullptr, "delta_merge_storage which cast from storage is null");
                table_snap = delta_merge_storage->writeNodeBuildRemoteReadSnapshot(required_columns, query_info, context, max_streams);
                // TODO: could be shared on the logical table level
                table_snap->output_field_types = std::make_shared<std::vector<tipb::FieldType>>();
                *table_snap->output_field_types = collectOutputFieldTypes(*dag_context.dag_request);
                RUNTIME_CHECK(table_snap->output_field_types->size() == table_snap->column_defines->size(),
                              table_snap->output_field_types->size(),
                              table_snap->column_defines->size());
            }

            injectFailPointForLocalRead(query_info);

            // After getting streams from storage, we need to validate whether Regions have changed or not after learner read.
            // (by calling `validateQueryInfo`). In case the key ranges of Regions have changed (Region merge/split), those `streams`
            // may contain different data other than expected.
            validateQueryInfo(*query_info.mvcc_query_info, learner_read_snapshot, tmt, log);
            break;
        }
        catch (RegionException & e)
        {
            query_info.mvcc_query_info->scan_context->total_local_region_num -= e.unavailable_region.size();
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
    return table_snap;
}

DM::Remote::DisaggPhysicalTableReadSnapshotPtr
DAGStorageInterpreter::buildLocalExecForPhysicalTable(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    const TableID & table_id,
    const SelectQueryInfo & query_info,
    size_t max_block_size)
{
    DM::Remote::DisaggPhysicalTableReadSnapshotPtr table_snap;
    size_t region_num = query_info.mvcc_query_info->regions_query_info.size();
    if (region_num == 0)
        return table_snap;

    RUNTIME_CHECK(storages_with_structure_lock.find(table_id) != storages_with_structure_lock.end());
    auto & storage = storages_with_structure_lock[table_id].storage;

    const DAGContext & dag_context = *context.getDAGContext();
    for (int num_allow_retry = 1; num_allow_retry >= 0; --num_allow_retry)
    {
        try
        {
            if (!dag_context.is_disaggregated_task)
            {
                storage->read(
                    exec_status,
                    group_builder,
                    required_columns,
                    query_info,
                    context,
                    max_block_size,
                    max_streams);
            }
            else
            {
                // build a snapshot on write node
                StorageDeltaMergePtr delta_merge_storage = std::dynamic_pointer_cast<StorageDeltaMerge>(storage);
                RUNTIME_CHECK_MSG(delta_merge_storage != nullptr, "delta_merge_storage which cast from storage is null");
                table_snap = delta_merge_storage->writeNodeBuildRemoteReadSnapshot(required_columns, query_info, context, max_streams);
                // TODO: could be shared on the logical table level
                table_snap->output_field_types = std::make_shared<std::vector<tipb::FieldType>>();
                *table_snap->output_field_types = collectOutputFieldTypes(*dag_context.dag_request);
                RUNTIME_CHECK(table_snap->output_field_types->size() == table_snap->column_defines->size(),
                              table_snap->output_field_types->size(),
                              table_snap->column_defines->size());
            }

            injectFailPointForLocalRead(query_info);
            // After getting sourceOps from storage, we need to validate whether Regions have changed or not after learner read.
            // (by calling `validateQueryInfo`). In case the key ranges of Regions have changed (Region merge/split), those `sourceOps`
            // may contain different data other than expected.
            validateQueryInfo(*query_info.mvcc_query_info, learner_read_snapshot, tmt, log);
            break;
        }
        catch (RegionException & e)
        {
            query_info.mvcc_query_info->scan_context->total_local_region_num -= e.unavailable_region.size();
            /// Recover from region exception for batchCop/MPP
            if (dag_context.isBatchCop() || dag_context.isMPPTask())
            {
                // clean all operator from local because we are not sure the correctness of those operators
                group_builder.reset();
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
    return table_snap;
}

void DAGStorageInterpreter::buildLocalStreams(DAGPipeline & pipeline, size_t max_block_size)
{
    const DAGContext & dag_context = *context.getDAGContext();
    size_t total_local_region_num = mvcc_query_info->regions_query_info.size();
    if (total_local_region_num == 0)
        return;
    mvcc_query_info->scan_context->total_local_region_num = total_local_region_num;
    const auto table_query_infos = generateSelectQueryInfos();
    bool has_multiple_partitions = table_query_infos.size() > 1;
    // MultiPartitionStreamPool will be disabled in no partition mode or single-partition case
    std::shared_ptr<MultiPartitionStreamPool> stream_pool = has_multiple_partitions ? std::make_shared<MultiPartitionStreamPool>() : nullptr;

    auto disaggregated_snap = std::make_shared<DM::Remote::DisaggReadSnapshot>();
    for (const auto & table_query_info : table_query_infos)
    {
        DAGPipeline current_pipeline;
        const TableID table_id = table_query_info.first;
        const SelectQueryInfo & query_info = table_query_info.second;
        auto table_snap = buildLocalStreamsForPhysicalTable(table_id, query_info, current_pipeline, max_block_size);
        if (table_snap)
        {
            disaggregated_snap->addTask(table_id, std::move(table_snap));
        }
        if (has_multiple_partitions)
            stream_pool->addPartitionStreams(current_pipeline.streams);
        else
            pipeline.streams.insert(pipeline.streams.end(), current_pipeline.streams.begin(), current_pipeline.streams.end());
    }

    LOG_DEBUG(
        log,
        "local streams built, is_disaggregated_task={} snap_id={}",
        dag_context.is_disaggregated_task,
        dag_context.is_disaggregated_task ? *dag_context.getDisaggTaskId() : DM::DisaggTaskId::unknown_disaggregated_task_id);

    if (dag_context.is_disaggregated_task)
    {
        // register the snapshot to manager
        auto snaps = context.getSharedContextDisagg()->wn_snapshot_manager;
        const auto & snap_id = *dag_context.getDisaggTaskId();
        auto timeout_s = context.getSettingsRef().disagg_task_snapshot_timeout;
        auto expired_at = Clock::now() + std::chrono::seconds(timeout_s);
        bool register_snapshot_ok = snaps->registerSnapshot(snap_id, disaggregated_snap, expired_at);
        RUNTIME_CHECK_MSG(register_snapshot_ok, "Disaggregated task has been registered, snap_id={}", snap_id);
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

void DAGStorageInterpreter::buildLocalExec(
    PipelineExecutorStatus & exec_status,
    PipelineExecGroupBuilder & group_builder,
    size_t max_block_size)
{
    const DAGContext & dag_context = *context.getDAGContext();
    size_t total_local_region_num = mvcc_query_info->regions_query_info.size();
    if (total_local_region_num == 0)
        return;
    mvcc_query_info->scan_context->total_local_region_num = total_local_region_num;
    const auto table_query_infos = generateSelectQueryInfos();

    auto disaggregated_snap = std::make_shared<DM::Remote::DisaggReadSnapshot>();
    // TODO Improve the performance of partition table in extreme case.
    // ref https://github.com/pingcap/tiflash/issues/4474
    for (const auto & table_query_info : table_query_infos)
    {
        PipelineExecGroupBuilder builder;
        const TableID table_id = table_query_info.first;
        const SelectQueryInfo & query_info = table_query_info.second;
        auto table_snap = buildLocalExecForPhysicalTable(exec_status, builder, table_id, query_info, max_block_size);
        if (table_snap)
        {
            disaggregated_snap->addTask(table_id, std::move(table_snap));
        }

        group_builder.merge(std::move(builder));
    }

    LOG_DEBUG(
        log,
        "local sourceOps built, is_disaggregated_task={}",
        dag_context.is_disaggregated_task);

    if (dag_context.is_disaggregated_task)
    {
        // register the snapshot to manager
        auto snaps = context.getSharedContextDisagg()->wn_snapshot_manager;
        const auto & snap_id = *dag_context.getDisaggTaskId();
        auto timeout_s = context.getSettingsRef().disagg_task_snapshot_timeout;
        auto expired_at = Clock::now() + std::chrono::seconds(timeout_s);
        bool register_snapshot_ok = snaps->registerSnapshot(snap_id, disaggregated_snap, expired_at);
        RUNTIME_CHECK_MSG(register_snapshot_ok, "Disaggregated task has been registered, snap_id={}", snap_id);
    }
}

std::unordered_map<TableID, DAGStorageInterpreter::StorageWithStructureLock> DAGStorageInterpreter::getAndLockStorages(Int64 query_schema_version)
{
    const auto keyspace_id = context.getDAGContext()->getKeyspaceID();
    std::unordered_map<TableID, DAGStorageInterpreter::StorageWithStructureLock> storages_with_lock;
    if (unlikely(query_schema_version == DEFAULT_UNSPECIFIED_SCHEMA_VERSION))
    {
        auto logical_table_storage = tmt.getStorages().get(keyspace_id, logical_table_id);
        if (!logical_table_storage)
        {
            throw TiFlashException(fmt::format("Table {} doesn't exist.", logical_table_id), Errors::Table::NotExists);
        }
        storages_with_lock[logical_table_id] = {logical_table_storage, logical_table_storage->lockStructureForShare(context.getCurrentQueryId())};
        if (table_scan.isPartitionTableScan())
        {
            for (auto const physical_table_id : table_scan.getPhysicalTableIDs())
            {
                auto physical_table_storage = tmt.getStorages().get(keyspace_id, physical_table_id);
                if (!physical_table_storage)
                {
                    throw TiFlashException(fmt::format("Table {} doesn't exist.", physical_table_id), Errors::Table::NotExists);
                }
                storages_with_lock[physical_table_id] = {physical_table_storage, physical_table_storage->lockStructureForShare(context.getCurrentQueryId())};
            }
        }
        return storages_with_lock;
    }

    /// Return: [storage, table_structure_lock]
    auto get_and_lock_storage = [&](bool schema_synced, TableID table_id) -> std::tuple<ManageableStoragePtr, TableStructureLockHolder> {
        /// Get storage in case it's dropped then re-created.
        // If schema synced, call getTable without try, leading to exception on table not existing.
        auto table_store = tmt.getStorages().get(keyspace_id, table_id);
        if (!table_store)
        {
            if (schema_synced)
                throw TiFlashException(fmt::format("Table {} doesn't exist.", table_id), Errors::Table::NotExists);
            else
                return {{}, {}};
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

        // check the columns in table_scan and table_store, to check whether we need to sync table schema.
        auto [are_columns_matched, error_message] = compareColumns(table_scan, table_store->getStoreColumnDefines(), dagContext(), log);

        if (are_columns_matched)
        {
            return std::make_tuple(table_store, lock);
        }

        // columns not match but we have synced schema, it means the schema in tiflash is newer than that in query
        if (schema_synced)
        {
            throw TiFlashException(fmt::format("The schema does not match the query, details: {}", error_message), Errors::Table::SchemaVersionError);
        }

        // let caller sync schema
        return {nullptr, {}};
    };

    auto get_and_lock_storages = [&](bool schema_synced) -> std::tuple<std::vector<ManageableStoragePtr>, std::vector<TableStructureLockHolder>, std::vector<TableID>> {
        std::vector<ManageableStoragePtr> table_storages;
        std::vector<TableStructureLockHolder> table_locks;

        std::vector<TableID> need_sync_table_ids;

        auto [logical_table_storage, logical_table_lock] = get_and_lock_storage(schema_synced, logical_table_id);
        if (logical_table_storage == nullptr)
        {
            need_sync_table_ids.push_back(logical_table_id);
        }
        else
        {
            table_storages.emplace_back(std::move(logical_table_storage));
            table_locks.emplace_back(std::move(logical_table_lock));
        }

        if (!table_scan.isPartitionTableScan())
        {
            return {table_storages, table_locks, need_sync_table_ids};
        }
        for (auto const physical_table_id : table_scan.getPhysicalTableIDs())
        {
            auto [physical_table_storage, physical_table_lock] = get_and_lock_storage(schema_synced, physical_table_id);
            if (physical_table_storage == nullptr)
            {
                need_sync_table_ids.push_back(physical_table_id);
            }
            else
            {
                table_storages.emplace_back(std::move(physical_table_storage));
                table_locks.emplace_back(std::move(physical_table_lock));
            }
        }
        return {table_storages, table_locks, need_sync_table_ids};
    };

    auto sync_schema = [&](TableID table_id) {
        GET_KEYSPACE_METRIC(tiflash_schema_trigger_count, type_cop_read, dagContext().getKeyspaceID()).Increment();
        auto start_time = Clock::now();
        tmt.getSchemaSyncerManager()->syncTableSchema(context, dagContext().getKeyspaceID(), table_id);
        auto schema_sync_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();
        LOG_INFO(log, "Table {} schema sync cost {} ms.", logical_table_id, schema_sync_cost);
    };

    /// Try get storage and lock once.
    auto [storages, locks, need_sync_table_ids] = get_and_lock_storages(false);
    if (need_sync_table_ids.empty())
    {
        LOG_INFO(log, "OK, no syncing required.");
    }
    else
    /// If first try failed, sync schema and try again.
    {
        LOG_INFO(log, "not OK, syncing schemas.");

        for (auto & table_id : need_sync_table_ids)
        {
            sync_schema(table_id);
        }


        std::tie(storages, locks, need_sync_table_ids) = get_and_lock_storages(true);
        if (need_sync_table_ids.empty())
        {
            LOG_INFO(log, "OK after syncing.");
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

std::pair<Names, std::vector<UInt8>> DAGStorageInterpreter::getColumnsForTableScan()
{
    // Get handle column name.
    String handle_column_name = MutableSupport::tidb_pk_column_name;
    if (auto pk_handle_col = storage_for_logical_table->getTableInfo().getPKHandleColumn())
        handle_column_name = pk_handle_col->get().name;

    // Get column names for table scan.
    Names required_columns_tmp;
    required_columns_tmp.reserve(table_scan.getColumnSize());
    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        auto const & ci = table_scan.getColumns()[i];
        const ColumnID cid = ci.id;

        if (ci.hasGeneratedColumnFlag())
        {
            LOG_DEBUG(log, "got column({}) with generated column flag", i);
            const auto & data_type = getDataTypeByColumnInfoForComputingLayer(ci);
            const auto & col_name = GeneratedColumnPlaceholderBlockInputStream::getColumnName(i);
            generated_column_infos.push_back(std::make_tuple(i, col_name, data_type));
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
        required_columns_tmp.emplace_back(std::move(name));
    }

    // Get the columns that may need to add extra cast.
    std::unordered_set<ColumnID> filter_col_id_set;
    for (const auto & expr : table_scan.getPushedDownFilters())
        getColumnIDsFromExpr(expr, table_scan.getColumns(), filter_col_id_set);
    if (unlikely(context.getSettingsRef().force_push_down_all_filters_to_scan))
    {
        for (const auto & expr : filter_conditions.conditions)
            getColumnIDsFromExpr(expr, table_scan.getColumns(), filter_col_id_set);
    }
    std::vector<UInt8> may_need_add_cast_column_tmp;
    may_need_add_cast_column_tmp.reserve(table_scan.getColumnSize());
    // If the column is not generated column, not in the filter columns and column id is not -1, then it may need cast.
    for (const auto & col : table_scan.getColumns())
        may_need_add_cast_column_tmp.push_back(!col.hasGeneratedColumnFlag() && !filter_col_id_set.contains(col.id) && col.id != -1);

    return {required_columns_tmp, may_need_add_cast_column_tmp};
}

// Build remote requests from `region_retry_from_local_region` and `table_regions_info.remote_regions`
std::vector<RemoteRequest> DAGStorageInterpreter::buildRemoteRequests(const DM::ScanContextPtr & scan_context)
{
    std::vector<RemoteRequest> remote_requests;
    std::unordered_map<RegionID, TableID> region_id_to_table_id_map;
    std::unordered_map<TableID, RegionRetryList> retry_regions_map;
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
        scan_context->total_remote_region_num += retry_regions.size();
        // Append the region into DAGContext to return them to the upper layer.
        // The upper layer should refresh its cache about these regions.
        for (const auto & r : retry_regions)
            context.getDAGContext()->retry_regions.push_back(r.get());

        remote_requests.push_back(RemoteRequest::build(
            retry_regions,
            *context.getDAGContext(),
            table_scan,
            storages_with_structure_lock[physical_table_id].storage->getTableInfo(),
            filter_conditions,
            log));
    }
    LOG_DEBUG(log, "remote request size: {}", remote_requests.size());
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
