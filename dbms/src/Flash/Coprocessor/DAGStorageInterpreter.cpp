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

std::vector<ExtraCastAfterTSMode> getExtraCastAfterTSModeFromTS(const TiDBTableScan & table_scan)
{
    std::vector<ExtraCastAfterTSMode> need_cast_column;
    need_cast_column.reserve(table_scan.getColumnSize());
    for (Int32 i = 0; i < table_scan.getColumnSize(); ++i)
    {
        auto const & ci = table_scan.getColumns()[i];
        ColumnID cid = ci.column_id();

        // Column ID -1 return the handle column
        if (cid != -1 && ci.tp() == TiDB::TypeTimestamp)
            need_cast_column.push_back(ExtraCastAfterTSMode::AppendTimeZoneCast);
        else if (cid != -1 && ci.tp() == TiDB::TypeTime)
            need_cast_column.push_back(ExtraCastAfterTSMode::AppendDurationCast);
        else
            need_cast_column.push_back(ExtraCastAfterTSMode::None);
    }

    return need_cast_column;
}
} // namespace

DAGStorageInterpreter::DAGStorageInterpreter(
    Context & context_,
    TiDBStorageTable & storage_table_,
    const String & pushed_down_filter_id_,
    const std::vector<const tipb::Expr *> & pushed_down_conditions_,
    size_t max_streams_)
    : context(context_)
    , storage_table(storage_table_)
    , pushed_down_filter_id(pushed_down_filter_id_)
    , pushed_down_conditions(pushed_down_conditions_)
    , max_streams(max_streams_)
    , log(Logger::get("DAGStorageInterpreter", context.getDAGContext()->log ? context.getDAGContext()->log->identifier() : ""))
    , logical_table_id(storage_table.getTiDBTableScan().getLogicalTableID())
    , settings(context.getSettingsRef())
    , tmt(context.getTMTContext())
    , mvcc_query_info(std::move(storage_table_.getTiDBReadSnapshot().moveMvccQueryInfo()))
    , region_retry_from_local_region(std::move(storage_table.getTiDBReadSnapshot().moveRegionRetryFromLocalRegion()))
    , source_columns(storage_table.getSchema())
{
    is_need_add_cast_column = getExtraCastAfterTSModeFromTS(storage_table.getTiDBTableScan());
}

void DAGStorageInterpreter::execute(DAGPipeline & pipeline)
{
    analyzer = std::make_unique<DAGExpressionAnalyzer>(std::move(source_columns), context);

    FAIL_POINT_PAUSE(FailPoints::pause_after_learner_read);

    if (!mvcc_query_info->regions_query_info.empty())
        doLocalRead(pipeline, settings.max_block_size);

    null_stream_if_empty = std::make_shared<NullBlockInputStream>(storage_table.getSampleBlock());

    // Should build these vars under protect of `table_structure_lock`.
    buildRemoteRequests();
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
    if (storage_table.getTiDBTableScan().isPartitionTableScan())
    {
        for (const auto physical_table_id : storage_table.getTiDBTableScan().getPhysicalTableIDs())
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
        const auto & storage = storage_table.getStorage(table_id);

        int num_allow_retry = 1;
        while (true)
        {
            try
            {
                current_pipeline.streams = storage->read(storage_table.getScanRequiredColumns(), query_info, context, from_stage, max_block_size, current_max_streams);

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
                storage_table.getTiDBReadSnapshot().validateQueryInfo(*query_info.mvcc_query_info);
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

void DAGStorageInterpreter::buildRemoteRequests()
{
    std::unordered_map<Int64, Int64> region_id_to_table_id_map;
    std::unordered_map<Int64, RegionRetryList> retry_regions_map;
    for (const auto physical_table_id : storage_table.getTiDBTableScan().getPhysicalTableIDs())
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


    for (const auto physical_table_id : storage_table.getTiDBTableScan().getPhysicalTableIDs())
    {
        const auto & retry_regions = retry_regions_map[physical_table_id];
        if (retry_regions.empty())
            continue;

        for (const auto & r : retry_regions)
            context.getDAGContext()->retry_regions.push_back(r.get());

        remote_requests.push_back(RemoteRequest::build(
            retry_regions,
            *context.getDAGContext(),
            storage_table.getTiDBTableScan(),
            storage_table.getStorage(physical_table_id)->getTableInfo(),
            pushed_down_filter_id,
            pushed_down_conditions,
            log));
    }
}

} // namespace DB
