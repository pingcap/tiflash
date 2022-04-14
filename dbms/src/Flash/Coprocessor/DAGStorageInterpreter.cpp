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
#include <Common/TiFlashMetrics.h>
#include <DataStreams/NullBlockInputStream.h>
#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Flash/Coprocessor/DAGStorageInterpreter.h>
#include <Parsers/makeDummyQuery.h>
#include <Storages/IManageableStorage.h>
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
extern const char force_remote_read_for_batch_cop[];
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
                    "Income key ranges is empty for region: " + std::to_string(r.region_id),
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
                            "Income key ranges is illegal for region: " + std::to_string(r.region_id)
                                + ", table id in key range is " + std::to_string(table_id_in_range) + ", table id in region is "
                                + std::to_string(physical_table_id),
                            Errors::Coprocessor::BadRequest);
                    }
                    if (p.first->compare(*info.range_in_table.first) < 0 || p.second->compare(*info.range_in_table.second) > 0)
                        throw TiFlashException(
                            "Income key ranges is illegal for region: " + std::to_string(r.region_id),
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

} // namespace

DAGStorageInterpreter::DAGStorageInterpreter(
    Context & context_,
    const TiDBTableScan & table_scan_,
    const String & pushed_down_filter_id_,
    const std::vector<const tipb::Expr *> & pushed_down_conditions_,
    size_t max_streams_)
    : context(context_)
    , table_scan(table_scan_)
    , pushed_down_filter_id(pushed_down_filter_id_)
    , pushed_down_conditions(pushed_down_conditions_)
    , max_streams(max_streams_)
    , log(Logger::get("DAGStorageInterpreter", context.getDAGContext()->log ? context.getDAGContext()->log->identifier() : ""))
    , logical_table_id(table_scan.getLogicalTableID())
    , settings(context.getSettingsRef())
    , tmt(context.getTMTContext())
    , mvcc_query_info(new MvccQueryInfo(true, settings.read_tso))
{
}

void DAGStorageInterpreter::execute(DAGPipeline & pipeline)
{
    const DAGContext & dag_context = *context.getDAGContext();
    if (dag_context.isBatchCop() || dag_context.isMPPTask())
        learner_read_snapshot = doBatchCopLearnerRead();
    else
        learner_read_snapshot = doCopLearnerRead();

    storages_with_structure_lock = getAndLockStorages(settings.schema_version);
    assert(storages_with_structure_lock.find(logical_table_id) != storages_with_structure_lock.end());
    storage_for_logical_table = storages_with_structure_lock[logical_table_id].storage;

    std::tie(required_columns, source_columns, is_need_add_cast_column) = getColumnsForTableScan(settings.max_columns_to_read);

    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    FAIL_POINT_PAUSE(FailPoints::pause_after_learner_read);

    if (!mvcc_query_info->regions_query_info.empty())
        doLocalRead(pipeline, settings.max_block_size);

    null_stream_if_empty = std::make_shared<NullBlockInputStream>(storage_for_logical_table->getSampleBlockForColumns(required_columns));

    // Should build these vars under protect of `table_structure_lock`.
    buildRemoteRequests();

    releaseAlterLocks();
}

LearnerReadSnapshot DAGStorageInterpreter::doCopLearnerRead()
{
    if (table_scan.isPartitionTableScan())
    {
        throw Exception("Cop request does not support partition table scan");
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

    return doLearnerRead(logical_table_id, *mvcc_query_info, max_streams, false, context, log);
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
            return doLearnerRead(logical_table_id, *mvcc_query_info, max_streams, true, context, log);
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
            e.addMessage("(while doing learner read for table, logical table_id: " + DB::toString(logical_table_id) + ")");
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
        query_info.query = makeDummyQuery();
        query_info.dag_query = std::make_unique<DAGQueryInfo>(
            pushed_down_conditions,
            analyzer->getPreparedSets(),
            analyzer->getCurrentInputColumns(),
            context.getTimezoneInfo());
        query_info.req_id = fmt::format("{} Table<{}>", log->identifier(), table_id);
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

void DAGStorageInterpreter::doLocalRead(DAGPipeline & pipeline, size_t max_block_size)
{
    const DAGContext & dag_context = *context.getDAGContext();
    size_t total_local_region_num = mvcc_query_info->regions_query_info.size();
    if (total_local_region_num == 0)
        return;
    auto table_query_infos = generateSelectQueryInfos();
    for (auto & table_query_info : table_query_infos)
    {
        DAGPipeline current_pipeline;
        TableID table_id = table_query_info.first;
        SelectQueryInfo & query_info = table_query_info.second;
        size_t region_num = query_info.mvcc_query_info->regions_query_info.size();
        if (region_num == 0)
            continue;
        /// calculate weighted max_streams for each partition, note at least 1 stream is needed for each partition
        size_t current_max_streams = table_query_infos.size() == 1 ? max_streams : (max_streams * region_num + total_local_region_num - 1) / total_local_region_num;

        QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
        assert(storages_with_structure_lock.find(table_id) != storages_with_structure_lock.end());
        auto & storage = storages_with_structure_lock[table_id].storage;

        int num_allow_retry = 1;
        while (true)
        {
            try
            {
                current_pipeline.streams = storage->read(required_columns, query_info, context, from_stage, max_block_size, current_max_streams);

                // After getting streams from storage, we need to validate whether regions have changed or not after learner read.
                // In case the versions of regions have changed, those `streams` may contain different data other than expected.
                // Like after region merge/split.

                // Inject failpoint to throw RegionException
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
                validateQueryInfo(*query_info.mvcc_query_info, learner_read_snapshot, tmt, log);
                break;
            }
            catch (RegionException & e)
            {
                /// Recover from region exception when super batch is enable
                if (dag_context.isBatchCop() || dag_context.isMPPTask())
                {
                    // clean all streams from local because we are not sure the correctness of those streams
                    current_pipeline.streams.clear();
                    const auto & dag_regions = dag_context.getTableRegionsInfoByTableID(table_id).local_regions;
                    FmtBuffer buffer;
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
                        LOG_FMT_WARNING(
                            log,
                            "RegionException after read from storage, regions [{}], message: {}{}",
                            buffer.toString(),
                            e.message(),
                            (regions_query_info.empty() ? "" : ", retry to read from local"));
                        if (unlikely(regions_query_info.empty()))
                            break; // no available region in local, break retry loop
                        continue; // continue to retry read from local storage
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
                        LOG_FMT_WARNING(log, "RegionException after read from storage, regions [{}], message: {}", buffer.toString(), e.message());
                        break; // break retry loop
                    }
                }
                else
                {
                    // Throw an exception for TiDB / TiSpark to retry
                    if (table_id == logical_table_id)
                        e.addMessage("(while creating InputStreams from storage `" + storage->getDatabaseName() + "`.`" + storage->getTableName()
                                     + "`, table_id: " + DB::toString(table_id) + ")");
                    else
                        e.addMessage("(while creating InputStreams from storage `" + storage->getDatabaseName() + "`.`" + storage->getTableName()
                                     + "`, table_id: " + DB::toString(table_id) + ", logical_table_id: " + DB::toString(logical_table_id) + ")");
                    throw;
                }
            }
            catch (DB::Exception & e)
            {
                /// Other unknown exceptions
                if (table_id == logical_table_id)
                    e.addMessage("(while creating InputStreams from storage `" + storage->getDatabaseName() + "`.`" + storage->getTableName()
                                 + "`, table_id: " + DB::toString(table_id) + ")");
                else
                    e.addMessage("(while creating InputStreams from storage `" + storage->getDatabaseName() + "`.`" + storage->getTableName()
                                 + "`, table_id: " + DB::toString(table_id) + ", logical_table_id: " + DB::toString(logical_table_id) + ")");
                throw;
            }
        }
        pipeline.streams.insert(pipeline.streams.end(), current_pipeline.streams.begin(), current_pipeline.streams.end());
    }
}

IDsAndStorageWithStructureLocks DAGStorageInterpreter::getAndLockStorages(Int64 query_schema_version)
{
    IDsAndStorageWithStructureLocks storages_with_lock;
    if (unlikely(query_schema_version == DEFAULT_UNSPECIFIED_SCHEMA_VERSION))
    {
        auto logical_table_storage = tmt.getStorages().get(logical_table_id);
        if (!logical_table_storage)
        {
            throw TiFlashException("Table " + std::to_string(logical_table_id) + " doesn't exist.", Errors::Table::NotExists);
        }
        storages_with_lock[logical_table_id] = {logical_table_storage, logical_table_storage->lockStructureForShare(context.getCurrentQueryId())};
        if (table_scan.isPartitionTableScan())
        {
            for (auto const physical_table_id : table_scan.getPhysicalTableIDs())
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

std::tuple<Names, NamesAndTypes, std::vector<ExtraCastAfterTSMode>> DAGStorageInterpreter::getColumnsForTableScan(Int64 max_columns_to_read)
{
    // todo handle alias column
    if (max_columns_to_read && table_scan.getColumnSize() > max_columns_to_read)
    {
        throw TiFlashException("Limit for number of columns to read exceeded. "
                               "Requested: "
                                   + toString(table_scan.getColumnSize()) + ", maximum: " + toString(max_columns_to_read),
                               Errors::BroadcastJoin::TooManyColumns);
    }

    Names required_columns_tmp;
    NamesAndTypes source_columns_tmp;
    std::vector<ExtraCastAfterTSMode> need_cast_column;
    need_cast_column.reserve(table_scan.getColumnSize());
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

void DAGStorageInterpreter::buildRemoteRequests()
{
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

        for (const auto & r : retry_regions)
            context.getDAGContext()->retry_regions.push_back(r.get());

        remote_requests.push_back(RemoteRequest::build(
            retry_regions,
            *context.getDAGContext(),
            table_scan,
            storages_with_structure_lock[physical_table_id].storage->getTableInfo(),
            pushed_down_filter_id,
            pushed_down_conditions,
            log));
    }
}

void DAGStorageInterpreter::releaseAlterLocks()
{
    // The DeltaTree engine ensures that once input streams are created, the caller can get a consistent result
    // from those streams even if DDL operations are applied. Release the alter lock so that reading does not
    // block DDL operations, keep the drop lock so that the storage not to be dropped during reading.
    for (auto storage_with_lock : storages_with_structure_lock)
    {
        drop_locks.emplace_back(std::get<1>(std::move(storage_with_lock.second.lock).release()));
    }
}

} // namespace DB
