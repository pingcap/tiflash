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

#pragma once

#include <Common/Exception.h>
#include <Flash/Coprocessor/TiDBReadSnapshot.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/LearnerRead.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{
TiDBReadSnapshot::LearnerReadSnapshot(
    Context & context_,
    const TiDBTableScan & tidb_table_scan_,
    const String & req_id)
    : context(context_)
    , tmt(context.getTMTContext())
    , tidb_table_scan(tidb_table_scan_)
    , log(Logger::get("TiDBReadSnapshot", req_id))
    , mvcc_query_info(new MvccQueryInfo(true, context_.getSettingsRef().read_tso))
{
    const auto & dag_context = *context.getDAGContext();
    learner_read_snapshot = dag_context.isBatchCop() || dag_context.isMPPTask()
        ? doBatchCopLearnerRead()
        : doCopLearnerRead();
}

void TiDBReadSnapshot::validateQueryInfo(const MvccQueryInfo & mvcc_query_info)
{
    RUNTIME_ASSERT(snapshot_status == SnapshotStatus::read || snapshot_status == SnapshotStatus::validated, log, "snapshot_status must be read/validated status.");
    validateQueryInfo(mvcc_query_info, learner_read_snapshot, tmt, log);
    snapshot_status = SnapshotStatus::validated;
}

void TiDBReadSnapshot::releaseLearnerReadSnapshot()
{
    RUNTIME_ASSERT(snapshot_status == SnapshotStatus::validated, log, "snapshot_status must be validated status.");
    learner_read_snapshot.clear();
    snapshot_status = SnapshotStatus::released;
}

LearnerReadSnapshot TiDBReadSnapshot::doCopLearnerRead()
{
    if (tidb_table_scan.isPartitionTableScan())
    {
        throw Exception("Cop request does not support partition table scan");
    }
    TablesRegionInfoMap regions_for_local_read;
    for (const auto physical_table_id : tidb_table_scan.getPhysicalTableIDs())
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

    return doLearnerRead(tidb_table_scan.getLogicalTableID(), *mvcc_query_info, max_streams, false, context, log);
}

/// Will assign region_retry_from_local_region
LearnerReadSnapshot TiDBReadSnapshot::doBatchCopLearnerRead()
{
    TablesRegionInfoMap regions_for_local_read;
    for (const auto physical_table_id : tidb_table_scan.getPhysicalTableIDs())
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
            return doLearnerRead(tidb_table_scan.getLogicalTableID(), *mvcc_query_info, max_streams, true, context, log);
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
            e.addMessage(fmt::format("(while doing learner read for table, logical table_id: {})", tidb_table_scan.getLogicalTableID()));
            throw;
        }
    }
}
} // namespace DB