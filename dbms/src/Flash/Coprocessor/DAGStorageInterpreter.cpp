#include <Common/FailPoint.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/NullBlockInputStream.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Parsers/makeDummyQuery.h>
#include <Storages/MutableSupport.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/SchemaSyncer.h>

namespace DB
{

namespace FailPoints
{
extern const char region_exception_after_read_from_storage_some_error[];
extern const char region_exception_after_read_from_storage_all_error[];
extern const char pause_after_learner_read[];
} // namespace FailPoints

namespace
{
RegionException::RegionReadStatus GetRegionReadStatus(
    const RegionInfo & check_info, const RegionPtr & current_region, ImutRegionRangePtr & region_range)
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
MakeRegionQueryInfos(const RegionInfoMap & dag_region_infos, const std::unordered_set<RegionID> & region_force_retry, TMTContext & tmt,
    MvccQueryInfo & mvcc_info, TableID table_id)
{
    mvcc_info.regions_query_info.clear();
    RegionRetryList region_need_retry;
    RegionException::RegionReadStatus status_res = RegionException::RegionReadStatus::OK;
    for (auto & [id, r] : dag_region_infos)
    {
        if (r.key_ranges.empty())
        {
            throw TiFlashException(
                "Income key ranges is empty for region: " + std::to_string(r.region_id), Errors::Coprocessor::BadRequest);
        }
        if (region_force_retry.count(id))
        {
            region_need_retry.emplace_back(r);
            status_res = RegionException::RegionReadStatus::NOT_FOUND;
            continue;
        }
        ImutRegionRangePtr region_range{nullptr};
        if (auto status = GetRegionReadStatus(r, tmt.getKVStore()->getRegion(id), region_range);
            status != RegionException::RegionReadStatus::OK)
        {
            region_need_retry.emplace_back(r);
            status_res = status;
            continue;
        }
        RegionQueryInfo info;
        {
            info.region_id = id;
            info.version = r.region_version;
            info.conf_version = r.region_conf_version;
            info.range_in_table = region_range->rawKeys();
            for (const auto & p : r.key_ranges)
            {
                TableID table_id_in_range = -1;
                if (!computeMappedTableID(*p.first, table_id_in_range) || table_id_in_range != table_id)
                {
                    throw TiFlashException("Income key ranges is illegal for region: " + std::to_string(r.region_id)
                            + ", table id in key range is " + std::to_string(table_id_in_range) + ", table id in region is "
                            + std::to_string(table_id),
                        Errors::Coprocessor::BadRequest);
                }
                if (p.first->compare(*info.range_in_table.first) < 0 || p.second->compare(*info.range_in_table.second) > 0)
                    throw TiFlashException(
                        "Income key ranges is illegal for region: " + std::to_string(r.region_id), Errors::Coprocessor::BadRequest);
            }
            info.required_handle_ranges = r.key_ranges;
            info.bypass_lock_ts = r.bypass_lock_ts;
        }
        mvcc_info.regions_query_info.emplace_back(std::move(info));
    }
    mvcc_info.concurrent = mvcc_info.regions_query_info.size() > 1 ? 1.0 : 0.0;

    if (region_need_retry.empty())
        return std::make_tuple(std::nullopt, RegionException::RegionReadStatus::OK);
    else
        return std::make_tuple(std::move(region_need_retry), status_res);
}

} // namespace

DAGStorageInterpreter::DAGStorageInterpreter(
    Context & context_,
    const DAGQuerySource & dag_,
    const DAGQueryBlock & query_block_,
    const tipb::TableScan & ts,
    const std::vector<const tipb::Expr *> & conditions_,
    size_t max_streams_,
    Poco::Logger * log_,
    std::shared_ptr<MPPTaskLog> mpp_task_log_)
    : context(context_),
      dag(dag_),
      query_block(query_block_),
      table_scan(ts),
      conditions(conditions_),
      max_streams(max_streams_),
      log(log_),
      mpp_task_log(mpp_task_log_),
      table_id(ts.table_id()),
      settings(context.getSettingsRef()),
      tmt(context.getTMTContext()),
      mvcc_query_info(new MvccQueryInfo(true, settings.read_tso))
{
}

void DAGStorageInterpreter::execute(DAGPipeline & pipeline)
{
    if (dag.isBatchCop())
        learner_read_snapshot = doBatchCopLearnerRead();
    else
        learner_read_snapshot = doCopLearnerRead();

    std::tie(storage, table_structure_lock) = getAndLockStorage(settings.schema_version);

    storage->addMPPTaskLog(mpp_task_log);

    std::tie(required_columns, source_columns, is_timestamp_column, handle_column_name) = getColumnsForTableScan(settings.max_columns_to_read);

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    FAIL_POINT_PAUSE(FailPoints::pause_after_learner_read);

    if (!mvcc_query_info->regions_query_info.empty())
        doLocalRead(pipeline, settings.max_block_size);

    for (auto & region_info : dag.getRetryRegions())
        region_retry.emplace_back(region_info);

    null_stream_if_empty = std::make_shared<NullBlockInputStream>(storage->getSampleBlockForColumns(required_columns));

    // Should build these vars under protect of `table_structure_lock`.
    std::tie(dag_request, dag_schema) = buildRemoteTS();
}

LearnerReadSnapshot DAGStorageInterpreter::doCopLearnerRead()
{
    auto [info_retry, status] = MakeRegionQueryInfos(
        dag.getRegions(),
        {},
        tmt,
        *mvcc_query_info,
        table_id);

    if (info_retry)
        throw RegionException({info_retry->begin()->get().region_id}, status);

    return doLearnerRead(table_id, *mvcc_query_info, max_streams, tmt, log);
}

/// Will assign region_retry
LearnerReadSnapshot DAGStorageInterpreter::doBatchCopLearnerRead()
{
    std::unordered_set<RegionID> force_retry;
    for (;;)
    {
        try
        {
            region_retry.clear();
            auto [retry, status] = MakeRegionQueryInfos(
                dag.getRegions(),
                force_retry,
                tmt,
                *mvcc_query_info,
                table_id);
            UNUSED(status);

            if (retry)
            {
                region_retry = std::move(*retry);
                for (const auto & r : region_retry)
                    force_retry.emplace(r.get().region_id);
            }
            if (mvcc_query_info->regions_query_info.empty())
                return {};
            return doLearnerRead(table_id, *mvcc_query_info, max_streams, tmt, log);
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
            e.addMessage("(while doing learner read for table, table_id: " + DB::toString(table_id) + ")");
            throw;
        }
    }
}

void DAGStorageInterpreter::doLocalRead(DAGPipeline & pipeline, size_t max_block_size)
{
    SelectQueryInfo query_info;
    /// to avoid null point exception
    query_info.query = makeDummyQuery();
    query_info.dag_query = std::make_unique<DAGQueryInfo>(
        conditions,
        analyzer->getPreparedSets(),
        analyzer->getCurrentInputColumns(),
        context.getTimezoneInfo());
    query_info.mvcc_query_info = std::move(mvcc_query_info);

    QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
    // TODO: Note that if storage is (Txn)MergeTree, and any region exception thrown, we won't do retry here.
    // Now we only support DeltaTree in production environment and don't do any extra check for storage type here.

    int num_allow_retry = 1;
    while (true)
    {
        try
        {
            pipeline.streams = storage->read(required_columns, query_info, context, from_stage, max_block_size, max_streams);

            // After getting streams from storage, we need to validate whether regions have changed or not after learner read.
            // In case the versions of regions have changed, those `streams` may contain different data other than expected.
            // Like after region merge/split.

            // Inject failpoint to throw RegionException
            fiu_do_on(FailPoints::region_exception_after_read_from_storage_some_error, {
                const auto & regions_info = query_info.mvcc_query_info->regions_query_info;
                RegionException::UnavailableRegions region_ids;
                for (const auto & info : regions_info)
                {
                    if (rand() % 100 > 50)
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
            validateQueryInfo(*query_info.mvcc_query_info, learner_read_snapshot, tmt, log);
            break;
        }
        catch (RegionException & e)
        {
            /// Recover from region exception when super batch is enable
            if (dag.isBatchCop())
            {
                // clean all streams from local because we are not sure the correctness of those streams
                pipeline.streams.clear();
                const auto & dag_regions = dag.getRegions();
                std::stringstream ss;
                // Normally there is only few regions need to retry when super batch is enabled. Retry to read
                // from local first. However, too many retry in different places may make the whole process
                // time out of control. We limit the number of retries to 1 now.
                if (likely(num_allow_retry > 0))
                {
                    --num_allow_retry;
                    auto & regions_query_info = query_info.mvcc_query_info->regions_query_info;
                    for (auto iter = regions_query_info.begin(); iter != regions_query_info.end(); /**/)
                    {
                        if (e.unavailable_region.find(iter->region_id) != e.unavailable_region.end())
                        {
                            // move the error regions info from `query_info.mvcc_query_info->regions_query_info` to `region_retry`
                            if (auto region_iter = dag_regions.find(iter->region_id); likely(region_iter != dag_regions.end()))
                            {
                                region_retry.emplace_back(region_iter->second);
                                ss << region_iter->first << ",";
                            }
                            iter = regions_query_info.erase(iter);
                        }
                        else
                        {
                            ++iter;
                        }
                    }
                    LOG_WARNING(log,
                        "RegionException after read from storage, regions ["
                            << ss.str() << "], message: " << e.message()
                            << (regions_query_info.empty() ? "" : ", retry to read from local"));
                    if (unlikely(regions_query_info.empty()))
                        break; // no available region in local, break retry loop
                    continue;  // continue to retry read from local storage
                }
                else
                {
                    // push all regions to `region_retry` to retry from other tiflash nodes
                    for (const auto & region : query_info.mvcc_query_info->regions_query_info)
                    {
                        auto iter = dag_regions.find(region.region_id);
                        if (likely(iter != dag_regions.end()))
                        {
                            region_retry.emplace_back(iter->second);
                            ss << iter->first << ",";
                        }
                    }
                    LOG_WARNING(log, "RegionException after read from storage, regions [" << ss.str() << "], message: " << e.message());
                    break; // break retry loop
                }
            }
            else
            {
                // Throw an exception for TiDB / TiSpark to retry
                e.addMessage("(while creating InputStreams from storage `" + storage->getDatabaseName() + "`.`" + storage->getTableName()
                    + "`, table_id: " + DB::toString(table_id) + ")");
                throw;
            }
        }
        catch (DB::Exception & e)
        {
            /// Other unknown exceptions
            e.addMessage("(while creating InputStreams from storage `" + storage->getDatabaseName() + "`.`" + storage->getTableName()
                + "`, table_id: " + DB::toString(table_id) + ")");
            throw;
        }
    }
}

std::tuple<ManageableStoragePtr, TableStructureLockHolder> DAGStorageInterpreter::getAndLockStorage(Int64 query_schema_version)
{
    /// Get current schema version in schema syncer for a chance to shortcut.
    if (unlikely(query_schema_version == DEFAULT_UNSPECIFIED_SCHEMA_VERSION))
    {
        auto storage_ = tmt.getStorages().get(table_id);
        if (!storage_)
        {
            throw TiFlashException("Table " + std::to_string(table_id) + " doesn't exist.", Errors::Table::NotExists);
        }
        return {storage_, storage_->lockStructureForShare(context.getCurrentQueryId())};
    }

    auto global_schema_version = tmt.getSchemaSyncer()->getCurrentVersion();

    /// Align schema version under the read lock.
    /// Return: [storage, table_structure_lock, storage_schema_version, ok]
    auto get_and_lock_storage = [&](bool schema_synced) -> std::tuple<ManageableStoragePtr, TableStructureLockHolder, Int64, bool> {
        /// Get storage in case it's dropped then re-created.
        // If schema synced, call getTable without try, leading to exception on table not existing.
        auto storage_ = tmt.getStorages().get(table_id);
        if (!storage_)
        {
            if (schema_synced)
                throw TiFlashException("Table " + std::to_string(table_id) + " doesn't exist.", Errors::Table::NotExists);
            else
                return {nullptr, TableStructureLockHolder{}, DEFAULT_UNSPECIFIED_SCHEMA_VERSION, false};
        }

        if (storage_->engineType() != ::TiDB::StorageEngine::TMT && storage_->engineType() != ::TiDB::StorageEngine::DT)
        {
            throw TiFlashException("Specifying schema_version for non-managed storage: " + storage_->getName()
                    + ", table: " + storage_->getTableName() + ", id: " + DB::toString(table_id) + " is not allowed",
                Errors::Coprocessor::Internal);
        }

        auto lock = storage_->lockStructureForShare(context.getCurrentQueryId());

        /// Check schema version, requiring TiDB/TiSpark and TiFlash both use exactly the same schema.
        // We have three schema versions, two in TiFlash:
        // 1. Storage: the version that this TiFlash table (storage) was last altered.
        // 2. Global: the version that TiFlash global schema is at.
        // And one from TiDB/TiSpark:
        // 3. Query: the version that TiDB/TiSpark used for this query.
        auto storage_schema_version = storage_->getTableInfo().schema_version;
        // Not allow storage > query in any case, one example is time travel queries.
        if (storage_schema_version > query_schema_version)
            throw TiFlashException("Table " + std::to_string(table_id) + " schema version " + std::to_string(storage_schema_version)
                    + " newer than query schema version " + std::to_string(query_schema_version),
                Errors::Table::SchemaVersionError);
        // From now on we have storage <= query.
        // If schema was synced, it implies that global >= query, as mentioned above we have storage <= query, we are OK to serve.
        if (schema_synced)
            return {storage_, lock, storage_schema_version, true};
        // From now on the schema was not synced.
        // 1. storage == query, TiDB/TiSpark is using exactly the same schema that altered this table, we are just OK to serve.
        // 2. global >= query, TiDB/TiSpark is using a schema older than TiFlash global, but as mentioned above we have storage <= query,
        // meaning that the query schema is still newer than the time when this table was last altered, so we still OK to serve.
        if (storage_schema_version == query_schema_version || global_schema_version >= query_schema_version)
            return {storage_, lock, storage_schema_version, true};
        // From now on we have global < query.
        // Return false for outer to sync and retry.
        return {nullptr, TableStructureLockHolder{}, storage_schema_version, false};
    };

    auto log_schema_version = [&](const String & result, Int64 storage_schema_version) {
        LOG_DEBUG(log,
            __PRETTY_FUNCTION__ << " Table " << table_id << " schema " << result << " Schema version [storage, global, query]: "
                                << "[" << storage_schema_version << ", " << global_schema_version << ", " << query_schema_version << "].");
    };

    auto sync_schema = [&] {
        auto start_time = Clock::now();
        GET_METRIC(tiflash_schema_trigger_count, type_cop_read).Increment();
        tmt.getSchemaSyncer()->syncSchemas(context);
        auto schema_sync_cost = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start_time).count();

        LOG_DEBUG(log, __PRETTY_FUNCTION__ << " Table " << table_id << " schema sync cost " << schema_sync_cost << "ms.");
    };

    /// Try get storage and lock once.
    auto [storage_, lock, storage_schema_version, ok] = get_and_lock_storage(false);
    if (ok)
    {
        log_schema_version("OK, no syncing required.", storage_schema_version);
        return {storage_, lock};
    }

    /// If first try failed, sync schema and try again.
    {
        log_schema_version("not OK, syncing schemas.", storage_schema_version);

        sync_schema();

        std::tie(storage_, lock, storage_schema_version, ok) = get_and_lock_storage(true);
        if (ok)
        {
            log_schema_version("OK after syncing.", storage_schema_version);
            return {storage_, lock};
        }

        throw TiFlashException("Shouldn't reach here", Errors::Coprocessor::Internal);
    }
}

std::tuple<Names, NamesAndTypes, BoolVec, String> DAGStorageInterpreter::getColumnsForTableScan(Int64 max_columns_to_read)
{
    // todo handle alias column
    if (max_columns_to_read && table_scan.columns().size() > max_columns_to_read)
    {
        throw TiFlashException("Limit for number of columns to read exceeded. "
                               "Requested: "
                + toString(table_scan.columns().size()) + ", maximum: " + toString(max_columns_to_read),
            Errors::BroadcastJoin::TooManyColumns);
    }

    Names required_columns_;
    NamesAndTypes source_columns_;
    BoolVec timestamp_column_flag;
    String handle_column_name_ = MutableSupport::tidb_pk_column_name;
    if (auto pk_handle_col = storage->getTableInfo().getPKHandleColumn())
        handle_column_name_ = pk_handle_col->get().name;

    for (Int32 i = 0; i < table_scan.columns().size(); i++)
    {
        auto const & ci = table_scan.columns(i);
        ColumnID cid = ci.column_id();

        // Column ID -1 return the handle column
        String name = cid == -1 ? handle_column_name_ : storage->getTableInfo().getColumnName(cid);
        auto pair = storage->getColumns().getPhysical(name);
        required_columns_.emplace_back(std::move(name));
        source_columns_.emplace_back(std::move(pair));
        timestamp_column_flag.push_back(cid != -1 && ci.tp() == TiDB::TypeTimestamp);
    }

    return {required_columns_, source_columns_, timestamp_column_flag, handle_column_name_};
}

std::tuple<std::optional<tipb::DAGRequest>, std::optional<DAGSchema>> DAGStorageInterpreter::buildRemoteTS()
{
    if (region_retry.empty())
        return std::make_tuple(std::nullopt, std::nullopt);

    for (const auto & r : region_retry)
    {
        context.getQueryContext().getDAGContext()->retry_regions.push_back(r.get());
    }
    LOG_DEBUG(log, ({
        std::stringstream ss;
        ss << "Start to retry " << region_retry.size() << " regions (";
        for (auto & r : region_retry)
            ss << r.get().region_id << ",";
        ss << ")";
        ss.str();
    }));

    DAGSchema schema;
    tipb::DAGRequest dag_req;

    {
        const auto & table_info = storage->getTableInfo();
        tipb::Executor * ts_exec = dag_req.add_executors();
        ts_exec->set_tp(tipb::ExecType::TypeTableScan);
        ts_exec->set_executor_id(query_block.source->executor_id());
        *(ts_exec->mutable_tbl_scan()) = table_scan;

        for (int i = 0; i < table_scan.columns().size(); ++i)
        {
            const auto & col = table_scan.columns(i);
            auto col_id = col.column_id();

            if (col_id == DB::TiDBPkColumnID)
            {
                ColumnInfo ci;
                ci.tp = TiDB::TypeLongLong;
                ci.setPriKeyFlag();
                ci.setNotNullFlag();
                schema.emplace_back(std::make_pair(handle_column_name, std::move(ci)));
            }
            else
            {
                auto & col_info = table_info.getColumnInfo(col_id);
                schema.emplace_back(std::make_pair(col_info.name, col_info));
            }
            dag_req.add_output_offsets(i);
        }
        dag_req.set_encode_type(tipb::EncodeType::TypeCHBlock);
    }
    /// do not collect execution summaries because in this case because the execution summaries
    /// will be collected by CoprocessorBlockInputStream
    dag_req.set_collect_execution_summaries(false);
    return std::make_tuple(dag_req, schema);
}


} // namespace DB

