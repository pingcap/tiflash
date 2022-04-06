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
#include <Common/Logger.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/ExpressionBlockInputStream.h>
#include <DataStreams/FilterBlockInputStream.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/TiRemoteBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGExpressionAnalyzer.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Flash/Coprocessor/TableScanInterpreterHelper.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/TiDB.h>
#include <common/logger_useful.h>
#include <kvproto/coprocessor.pb.h>
#include <pingcap/coprocessor/Client.h>

#include <memory>
#include <vector>

namespace DB
{
namespace FailPoints
{
extern const char pause_after_copr_streams_acquired[];
extern const char minimum_block_size_for_cross_join[];
} // namespace FailPoints

namespace TableScanInterpreterHelper
{
namespace
{
bool schemaMatch(const DAGSchema & left, const DAGSchema & right)
{
    if (left.size() != right.size())
        return false;
    for (size_t i = 0; i < left.size(); i++)
    {
        const auto & left_ci = left[i];
        const auto & right_ci = right[i];
        if (left_ci.second.tp != right_ci.second.tp)
            return false;
        if (left_ci.second.flag != right_ci.second.flag)
            return false;
    }
    return true;
}

// add timezone cast for timestamp type, this is used to support session level timezone
bool addExtraCastsAfterTs(
    DAGExpressionAnalyzer & analyzer,
    const std::vector<ExtraCastAfterTSMode> & need_cast_column,
    ExpressionActionsChain & chain,
    const TiDBTableScan & table_scan)
{
    bool has_need_cast_column = false;
    for (auto b : need_cast_column)
    {
        has_need_cast_column |= (b != ExtraCastAfterTSMode::None);
    }
    if (!has_need_cast_column)
        return false;
    return analyzer.appendExtraCastsAfterTS(chain, need_cast_column, table_scan);
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
        if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
        {
            p_stream->setLimits(limits);
            p_stream->setQuota(quota);
        }
    });
}

ExpressionActionsPtr generateProjectExpressionActions(
    const BlockInputStreamPtr & stream,
    const Context & context,
    const NamesWithAliases & project_cols)
{
    auto columns = stream->getHeader();
    NamesAndTypesList input_column;
    for (const auto & column : columns.getColumnsWithTypeAndName())
    {
        input_column.emplace_back(column.name, column.type);
    }
    ExpressionActionsPtr project = std::make_shared<ExpressionActions>(input_column, context.getSettingsRef());
    project->add(ExpressionAction::project(project_cols));
    return project;
}

void recordProfileStreams(DAGContext & dag_context, DAGPipeline & pipeline, const String & key)
{
    auto & profile_streams = dag_context.getProfileStreamsMap()[key];
    pipeline.transform([&profile_streams](auto & stream) { profile_streams.push_back(stream); });
}

void executeRemoteQueryImpl(
    const Context & context,
    DAGPipeline & pipeline,
    const String & table_scan_executor_id,
    std::vector<RemoteRequest> & remote_requests)
{
    assert(!remote_requests.empty());
    DAGSchema & schema = remote_requests[0].schema;
#ifndef NDEBUG
    for (size_t i = 1; i < remote_requests.size(); ++i)
    {
        if (!schemaMatch(schema, remote_requests[i].schema))
            throw Exception("Schema mismatch between different partitions for partition table");
    }
#endif
    bool has_enforce_encode_type = remote_requests[0].dag_request.has_force_encode_type() && remote_requests[0].dag_request.force_encode_type();
    pingcap::kv::Cluster * cluster = context.getTMTContext().getKVCluster();
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
        auto tasks = pingcap::coprocessor::buildCopTasks(bo, cluster, remote_request.key_ranges, req, store_type, &Poco::Logger::get("pingcap/coprocessor"));
        all_tasks.insert(all_tasks.end(), tasks.begin(), tasks.end());
    }

    size_t concurrent_num = std::min<size_t>(context.getSettingsRef().max_threads, all_tasks.size());
    size_t task_per_thread = all_tasks.size() / concurrent_num;
    size_t rest_task = all_tasks.size() % concurrent_num;
    const String & req_id = context.getDAGContext()->log->identifier();
    for (size_t i = 0, task_start = 0; i < concurrent_num; i++)
    {
        size_t task_end = task_start + task_per_thread;
        if (i < rest_task)
            task_end++;
        if (task_end == task_start)
            continue;
        std::vector<pingcap::coprocessor::copTask> tasks(all_tasks.begin() + task_start, all_tasks.begin() + task_end);

        auto coprocessor_reader = std::make_shared<CoprocessorReader>(schema, cluster, tasks, has_enforce_encode_type, 1);
        BlockInputStreamPtr input = std::make_shared<CoprocessorBlockInputStream>(coprocessor_reader, req_id, table_scan_executor_id);
        pipeline.streams.push_back(input);
        task_start = task_end;
    }
}

void executeCastAfterTableScan(
    const Context & context,
    const TiDBTableScan & table_scan,
    DAGExpressionAnalyzer & analyzer,
    const std::vector<ExtraCastAfterTSMode> & is_need_add_cast_column,
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline)
{
    auto original_source_columns = analyzer.getCurrentInputColumns();

    ExpressionActionsChain chain;
    analyzer.initChain(chain, original_source_columns);

    // execute timezone cast or duration cast if needed for local table scan
    if (addExtraCastsAfterTs(analyzer, is_need_add_cast_column, chain, table_scan))
    {
        ExpressionActionsPtr extra_cast = chain.getLastActions();
        chain.finalize();
        chain.clear();

        // After `addExtraCastsAfterTs`, analyzer->getCurrentInputColumns() has been modified.
        // For remote read, `timezone cast and duration cast` had been pushed down, don't need to execute cast expressions.
        // To keep the schema of local read streams and remote read streams the same, do project action for remote read streams.
        NamesWithAliases project_for_remote_read;
        const auto & after_cast_source_columns = analyzer.getCurrentInputColumns();
        for (size_t i = 0; i < after_cast_source_columns.size(); ++i)
        {
            project_for_remote_read.emplace_back(original_source_columns[i].name, after_cast_source_columns[i].name);
        }
        assert(!project_for_remote_read.empty());
        assert(pipeline.streams_with_non_joined_data.empty());
        assert(remote_read_streams_start_index <= pipeline.streams.size());
        size_t i = 0;
        const String req_id = context.getDAGContext()->log->identifier();
        // local streams
        while (i < remote_read_streams_start_index)
        {
            auto & stream = pipeline.streams[i++];
            stream = std::make_shared<ExpressionBlockInputStream>(stream, extra_cast, req_id);
        }
        // remote streams
        if (i < pipeline.streams.size())
        {
            ExpressionActionsPtr project_for_cop_read = generateProjectExpressionActions(
                pipeline.streams[i],
                context,
                project_for_remote_read);
            while (i < pipeline.streams.size())
            {
                auto & stream = pipeline.streams[i++];
                stream = std::make_shared<ExpressionBlockInputStream>(stream, project_for_cop_read, req_id);
            }
        }
    }
}

void executePushedDownFilter(
    DAGExpressionAnalyzer & analyzer,
    const std::vector<const tipb::Expr *> & conditions,
    size_t remote_read_streams_start_index,
    DAGPipeline & pipeline,
    const String & req_id)
{
    ExpressionActionsChain chain;
    analyzer.initChain(chain, analyzer.getCurrentInputColumns());
    String filter_column_name = analyzer.appendWhere(chain, conditions);
    ExpressionActionsPtr before_where = chain.getLastActions();
    chain.addStep();

    // remove useless tmp column and keep the schema of local streams and remote streams the same.
    NamesWithAliases project_cols;
    for (const auto & col : analyzer.getCurrentInputColumns())
    {
        chain.getLastStep().required_output.push_back(col.name);
        project_cols.emplace_back(col.name, col.name);
    }
    chain.getLastActions()->add(ExpressionAction::project(project_cols));
    ExpressionActionsPtr project_after_where = chain.getLastActions();
    chain.finalize();
    chain.clear();

    assert(pipeline.streams_with_non_joined_data.empty());
    assert(remote_read_streams_start_index <= pipeline.streams.size());
    // for remote read, filter had been pushed down, don't need to execute again.
    for (size_t i = 0; i < remote_read_streams_start_index; ++i)
    {
        auto & stream = pipeline.streams[i];
        stream = std::make_shared<FilterBlockInputStream>(stream, before_where, filter_column_name, req_id);
        // after filter, do project action to keep the schema of local streams and remote streams the same.
        stream = std::make_shared<ExpressionBlockInputStream>(stream, project_after_where, req_id);
    }
}
} // namespace

std::unique_ptr<DAGExpressionAnalyzer> handleTableScan(
    Context & context,
    const TiDBTableScan & table_scan,
    IDsAndStorageWithStructureLocks && storages_with_structure_lock,
    const NamesAndTypes & source_columns,
    const String & filter_executor_id,
    const std::vector<const tipb::Expr *> & conditions,
    DAGPipeline & pipeline,
    size_t max_streams)
{
    DAGContext & dag_context = *context.getDAGContext();
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
    if (!has_region_to_read)
        throw TiFlashException(
            fmt::format("Dag Request does not have region to read for table: {}", table_scan.getLogicalTableID()),
            Errors::Coprocessor::BadRequest);

    DAGStorageInterpreter storage_interpreter(context, table_scan, std::move(storages_with_structure_lock), source_columns, filter_executor_id, conditions, max_streams);
    storage_interpreter.execute(pipeline);

    std::unique_ptr<DAGExpressionAnalyzer> analyzer = std::move(storage_interpreter.analyzer);

    auto remote_requests = std::move(storage_interpreter.remote_requests);
    auto null_stream_if_empty = std::move(storage_interpreter.null_stream_if_empty);

    // It is impossible to have no joined stream.
    assert(pipeline.streams_with_non_joined_data.empty());
    // after executeRemoteQueryImpl, remote read stream will be appended in pipeline.streams.
    size_t remote_read_streams_start_index = pipeline.streams.size();

    // For those regions which are not presented in this tiflash node, we will try to fetch streams by key ranges from other tiflash nodes, only happens in batch cop / mpp mode.
    if (!remote_requests.empty())
        executeRemoteQueryImpl(context, pipeline, table_scan.getTableScanExecutorID(), remote_requests);

    /// record local and remote io input stream
    auto & table_scan_io_input_streams = dag_context.getInBoundIOInputStreamsMap()[table_scan.getTableScanExecutorID()];
    pipeline.transform([&](auto & stream) { table_scan_io_input_streams.push_back(stream); });

    if (pipeline.streams.empty())
    {
        pipeline.streams.emplace_back(null_stream_if_empty);
        // reset remote_read_streams_start_index for null_stream_if_empty.
        remote_read_streams_start_index = 1;
    }

    /// Theoretically we could move addTableLock to DAGStorageInterpreter, but we don't wants to the table to be dropped
    /// during the lifetime of this query, and sometimes if there is no local region, we will use the RemoteBlockInputStream
    /// or even the null_stream to hold the lock, so I would like too keep the addTableLock in DAGQueryBlockInterpreter
    pipeline.transform([&](auto & stream) {
        // todo do not need to hold all locks in each stream, if the stream is reading from table a
        //  it only needs to hold the lock of table a
        for (auto & lock : storage_interpreter.drop_locks)
            stream->addTableLock(lock);
    });

    /// Set the limits and quota for reading data, the speed and time of the query.
    setQuotaAndLimitsOnTableScan(context, pipeline);
    FAIL_POINT_PAUSE(FailPoints::pause_after_copr_streams_acquired);

    /// handle timezone/duration cast for local and remote table scan.
    executeCastAfterTableScan(
        context,
        table_scan,
        *analyzer,
        storage_interpreter.is_need_add_cast_column,
        remote_read_streams_start_index,
        pipeline);
    recordProfileStreams(dag_context, pipeline, table_scan.getTableScanExecutorID());

    /// handle pushed down filter for local and remote table scan.
    if (!conditions.empty())
    {
        executePushedDownFilter(*analyzer, conditions, remote_read_streams_start_index, pipeline, dag_context.log->identifier());
        recordProfileStreams(dag_context, pipeline, filter_executor_id);
    }

    return analyzer;
}

std::unordered_map<TableID, StorageWithStructureLock> getAndLockStorages(
    Context & context,
    const TiDBTableScan & tidb_table_scan)
{
    const auto & settings = context.getSettingsRef();
    Int64 query_schema_version = settings.schema_version;
    TMTContext & tmt = context.getTMTContext();
    auto log = Logger::get("getAndLockStorages", context.getDAGContext()->getMPPTaskId().toString());
    std::unordered_map<TableID, StorageWithStructureLock> storages_with_lock;
    Int64 logical_table_id = tidb_table_scan.getLogicalTableID();
    if (unlikely(query_schema_version == DEFAULT_UNSPECIFIED_SCHEMA_VERSION))
    {
        auto logical_table_storage = tmt.getStorages().get(logical_table_id);
        if (!logical_table_storage)
        {
            throw TiFlashException("Table " + std::to_string(logical_table_id) + " doesn't exist.", Errors::Table::NotExists);
        }
        storages_with_lock[logical_table_id] = {logical_table_storage, logical_table_storage->lockStructureForShare(context.getCurrentQueryId())};
        if (tidb_table_scan.isPartitionTableScan())
        {
            for (auto const physical_table_id : tidb_table_scan.getPhysicalTableIDs())
            {
                auto physical_table_storage = tmt.getStorages().get(physical_table_id);
                if (!physical_table_storage)
                {
                    throw TiFlashException("Table " + std::to_string(physical_table_id) + " doesn't exist.", Errors::Table::NotExists);
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
                throw TiFlashException("Table " + std::to_string(table_id) + " doesn't exist.", Errors::Table::NotExists);
            else
                return {{}, {}, {}, false};
        }

        if (table_store->engineType() != ::TiDB::StorageEngine::TMT && table_store->engineType() != ::TiDB::StorageEngine::DT)
        {
            throw TiFlashException("Specifying schema_version for non-managed storage: " + table_store->getName()
                                       + ", table: " + table_store->getTableName() + ", id: " + DB::toString(table_id) + " is not allowed",
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
            throw TiFlashException("Table " + std::to_string(table_id) + " schema version " + std::to_string(storage_schema_version)
                                       + " newer than query schema version " + std::to_string(query_schema_version),
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
        if (!tidb_table_scan.isPartitionTableScan())
        {
            return {table_storages, table_locks, table_schema_versions, true};
        }
        for (auto const physical_table_id : tidb_table_scan.getPhysicalTableIDs())
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
        buffer.fmtAppend("{} Table {} schema {} Schema version [storage, global, query]: [{}, {}, {}]", __PRETTY_FUNCTION__, logical_table_id, result, storage_schema_versions[0], global_schema_version, query_schema_version);
        if (tidb_table_scan.isPartitionTableScan())
        {
            assert(storage_schema_versions.size() == 1 + tidb_table_scan.getPhysicalTableIDs().size());
            for (size_t i = 0; i < tidb_table_scan.getPhysicalTableIDs().size(); ++i)
            {
                const auto physical_table_id = tidb_table_scan.getPhysicalTableIDs()[i];
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
        LOG_FMT_INFO(log, "Table {} schema sync cost {}ms.", logical_table_id, schema_sync_cost);
    };

    /// Try get storage and lock once.
    auto [storages, locks, storage_schema_versions, ok] = get_and_lock_storages(false);
    if (ok)
    {
        LOG_FMT_INFO(log, "{}", log_schema_version("OK, no syncing required.", storage_schema_versions));
    }
    else
    /// If first try failed, sync schema and try again.
    {
        LOG_FMT_INFO(log, "not OK, syncing schemas.");

        sync_schema();

        std::tie(storages, locks, storage_schema_versions, ok) = get_and_lock_storages(true);
        if (ok)
        {
            LOG_FMT_INFO(log, "{}", log_schema_version("OK after syncing.", storage_schema_versions));
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

NamesAndTypes getSchemaForTableScan(
    const Context & context,
    const TiDBTableScan & table_scan,
    IDsAndStorageWithStructureLocks & storages_with_structure_lock)
{
    const Settings & settings = context.getSettingsRef();
    Int64 max_columns_to_read = settings.max_columns_to_read;

    assert(storages_with_structure_lock.find(table_scan.getLogicalTableID()) != storages_with_structure_lock.end());
    auto storage_for_logical_table = storages_with_structure_lock[table_scan.getLogicalTableID()].storage;

    // todo handle alias column
    if (max_columns_to_read && table_scan.getColumnSize() > max_columns_to_read)
    {
        throw TiFlashException(
            fmt::format("Limit for number of columns to read exceeded. Requested: {}, maximum: {}", table_scan.getColumnSize(), max_columns_to_read),
            Errors::BroadcastJoin::TooManyColumns);
    }

    NamesAndTypes schema;
    String handle_column_name = MutableSupport::tidb_pk_column_name;
    if (auto pk_handle_col = storage_for_logical_table->getTableInfo().getPKHandleColumn())
        handle_column_name = pk_handle_col->get().name;

    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        auto const & ci = table_scan.getColumns()[i];
        ColumnID cid = ci.column_id();

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
            schema.emplace_back(std::move(extra_table_id_column_pair));
        }
        else
        {
            auto pair = storage_for_logical_table->getColumns().getPhysical(name);
            schema.emplace_back(std::move(pair));
        }
    }

    return schema;
}
} // namespace TableScanInterpreterHelper
} // namespace DB