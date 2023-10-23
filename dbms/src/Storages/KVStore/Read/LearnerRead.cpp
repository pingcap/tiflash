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
#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Read/LearnerRead.h>
#include <Storages/KVStore/Read/LearnerReadWorker.h>
#include <Storages/KVStore/Read/RegionException.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/RegionQueryInfo.h>
#include <common/logger_useful.h>
#include <fmt/chrono.h>


namespace DB
{

LearnerReadSnapshot doLearnerRead(
    const TableID logical_table_id,
    MvccQueryInfo & mvcc_query_info,
    bool for_batch_cop,
    Context & context,
    const LoggerPtr & log)
{
    assert(log != nullptr);
    // disagg compute node should not execute learner read
    RUNTIME_ASSERT(
        !(context.getSharedContextDisagg()->isDisaggregatedComputeMode()
          && context.getSharedContextDisagg()->use_autoscaler));
    RUNTIME_CHECK_MSG(
        !mvcc_query_info.regions_query_info.empty(),
        "no regions to be query, table_id={}",
        logical_table_id);

    const bool is_wn_disagg_read = context.getDAGContext() ? context.getDAGContext()->is_disaggregated_task : false;

    auto & tmt = context.getTMTContext();
    LearnerReadWorker worker(mvcc_query_info, tmt, for_batch_cop, is_wn_disagg_read, log);
    LearnerReadSnapshot regions_snapshot = worker.buildRegionsSnapshot();
    const auto & [start_time, end_time] = worker.waitUntilDataAvailable( //
        regions_snapshot,
        tmt.batchReadIndexTimeout(),
        tmt.waitIndexTimeout());

    if (auto * dag_context = context.getDAGContext())
    {
        // TODO(observability): add info about the number of stale read regions
        dag_context->has_read_wait_index = true;
        dag_context->read_wait_index_start_timestamp = start_time;
        dag_context->read_wait_index_end_timestamp = end_time;
    }

    return regions_snapshot;
}

/// Ensure regions' info after read.
void validateQueryInfo(
    const MvccQueryInfo & mvcc_query_info,
    const LearnerReadSnapshot & regions_snapshot,
    TMTContext & tmt,
    const LoggerPtr & log)
{
    RegionException::UnavailableRegions fail_region_ids;
    RegionException::RegionReadStatus fail_status = RegionException::RegionReadStatus::OK;
    std::string fail_extra_msg;

    for (const auto & region_query_info : mvcc_query_info.regions_query_info)
    {
        RegionException::RegionReadStatus status = RegionException::RegionReadStatus::OK;
        auto region = tmt.getKVStore()->getRegion(region_query_info.region_id);
        if (auto iter = regions_snapshot.find(region_query_info.region_id); //
            iter == regions_snapshot.end() || iter->second != region)
        {
            // If snapshot is applied during learner read, we should abort with an exception later.
            status = RegionException::RegionReadStatus::NOT_FOUND;
        }
        else if (region->version() != region_query_info.version)
        {
            // ABA problem may cause because one region is removed and inserted back.
            // if the version of region is changed, the `streams` may has less data because of compaction.
            status = RegionException::RegionReadStatus::EPOCH_NOT_MATCH;
            fail_extra_msg = fmt::format("{} != {}", region->version(), region_query_info.version);
        }

        if (status != RegionException::RegionReadStatus::OK)
        {
            fail_region_ids.emplace(region_query_info.region_id);
            fail_status = status;
            LOG_WARNING(
                log,
                "Check after snapshot acquired from storage, region_id={} version={} handle_range={} status={} "
                "fail_msg={}",
                region_query_info.region_id,
                region_query_info.version,
                RecordKVFormat::DecodedTiKVKeyRangeToDebugString(region_query_info.range_in_table),
                magic_enum::enum_name(status),
                fail_extra_msg);
        }
    }

    if (!fail_region_ids.empty())
    {
        throw RegionException(std::move(fail_region_ids), fail_status, fail_extra_msg.c_str());
    }
}

MvccQueryInfo::MvccQueryInfo(bool resolve_locks_, UInt64 read_tso_, DM::ScanContextPtr scan_ctx)
    : read_tso(read_tso_)
    , resolve_locks(read_tso_ == std::numeric_limits<UInt64>::max() ? false : resolve_locks_)
    , scan_context(std::move(scan_ctx))
{
    // using `std::numeric_limits::max()` to resolve lock may break basic logic.
}

} // namespace DB
