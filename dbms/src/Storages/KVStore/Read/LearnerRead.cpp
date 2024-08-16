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

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashException.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>
#include <Interpreters/SharedContexts/Disagg.h>
#include <Poco/Message.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/MultiRaft/RegionExecutionResult.h>
#include <Storages/KVStore/Read/LearnerRead.h>
#include <Storages/KVStore/Read/LockException.h>
#include <Storages/KVStore/Read/RegionException.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/KVStore/Utils.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <fmt/chrono.h>


namespace DB
{

struct UnavailableRegions
{
    UnavailableRegions(bool for_batch_cop_, bool is_wn_disagg_read_)
        : batch_cop(for_batch_cop_)
        , is_wn_disagg_read(is_wn_disagg_read_)
    {}

    void add(RegionID id, RegionException::RegionReadStatus status_)
    {
        status = status_;
        ids.emplace(id);
    }

    size_t size() const { return ids.size(); }

    bool empty() const { return ids.empty(); }

    bool contains(RegionID region_id) const { return ids.contains(region_id); }

    void addRegionLock(RegionID region_id_, LockInfoPtr && region_lock_)
    {
        region_locks.emplace_back(region_id_, std::move(region_lock_));
        ids.emplace(region_id_);
    }

    void tryThrowRegionException()
    {
        // For batch-cop request (not handled by disagg write node), all unavailable regions, include the ones with lock exception, should be collected and retry next round.
        // For normal cop request, which only contains one region, LockException should be thrown directly and let upper layer (like client-c, tidb, tispark) handle it.
        // For batch-cop request (handled by disagg write node), LockException should be thrown directly and let upper layer (disagg read node) handle it.

        if (is_wn_disagg_read && !region_locks.empty())
            throw LockException(std::move(region_locks));

        if (!batch_cop && !region_locks.empty())
            throw LockException(std::move(region_locks));

        if (!ids.empty())
            throw RegionException(std::move(ids), status);
    }

    void addRegionWaitIndexTimeout(const RegionID region_id, UInt64 index_to_wait, UInt64 current_applied_index)
    {
        if (!batch_cop)
        {
            // If server is being terminated / time-out, add the region_id into `unavailable_regions` to other store.
            add(region_id, RegionException::RegionReadStatus::NOT_FOUND);
            return;
        }

        // When wait index timeout happens, we return a `TiFlashException` instead of `RegionException` to break
        // the read request from retrying.
        // TODO: later maybe we can return SERVER_IS_BUSY to the client
        throw TiFlashException(
            Errors::Coprocessor::RegionError,
            "Region unavailable, region_id={} wait_index={} applied_index={}",
            region_id,
            index_to_wait,
            current_applied_index);
    }

private:
    const bool batch_cop;
    const bool is_wn_disagg_read;

    RegionException::UnavailableRegions ids;
    std::vector<std::pair<RegionID, LockInfoPtr>> region_locks;
    RegionException::RegionReadStatus status{RegionException::RegionReadStatus::NOT_FOUND};
};

class MvccQueryInfoWrap : boost::noncopyable
{
    using Base = MvccQueryInfo;
    Base & inner;
    std::optional<Base::RegionsQueryInfo> regions_info;
    // Points to either `regions_info` or `mvcc_query_info.regions_query_info`.
    Base::RegionsQueryInfo * regions_query_info_ptr;

public:
    MvccQueryInfoWrap(Base & mvcc_query_info, TMTContext & tmt, const TiDB::TableID logical_table_id)
        : inner(mvcc_query_info)
    {
        if (likely(!inner.regions_query_info.empty()))
        {
            regions_query_info_ptr = &inner.regions_query_info;
        }
        else
        {
            regions_info = Base::RegionsQueryInfo();
            regions_query_info_ptr = &*regions_info;
            // Only for (integration) test, because regions_query_info should never be empty if query is from TiDB or TiSpark.
            // todo support partition table
            auto regions = tmt.getRegionTable().getRegionsByTable(NullspaceID, logical_table_id);
            regions_query_info_ptr->reserve(regions.size());
            for (const auto & [id, region] : regions)
            {
                if (region == nullptr)
                    continue;
                regions_query_info_ptr->emplace_back(RegionQueryInfo{
                    id,
                    region->version(),
                    region->confVer(),
                    logical_table_id,
                    region->getRange()->rawKeys(),
                    {}});
            }
        }
    }
    Base * operator->() { return &inner; }

    const Base::RegionsQueryInfo & getRegionsInfo() const { return *regions_query_info_ptr; }
    void addReadIndexRes(RegionID region_id, UInt64 read_index) { inner.read_index_res[region_id] = read_index; }
    UInt64 getReadIndexRes(RegionID region_id) const
    {
        if (auto it = inner.read_index_res.find(region_id); it != inner.read_index_res.end())
            return it->second;
        return 0;
    }
};

struct LearnerReadStatistics
{
    UInt64 read_index_elapsed_ms = 0;
    UInt64 wait_index_elapsed_ms = 0;

    // num_regions == num_read_index_request + num_cached_read_index + num_stale_read
    UInt64 num_regions = 0;
    //
    UInt64 num_read_index_request = 0;
    UInt64 num_cached_read_index = 0;
    UInt64 num_stale_read = 0;
};

namespace
{

void addressBatchReadIndexError(
    std::unordered_map<RegionID, kvrpcpb::ReadIndexResponse> & batch_read_index_result,
    UnavailableRegions & unavailable_regions,
    MvccQueryInfoWrap & mvcc_query_info,
    LoggerPtr log)
{
    // if size of batch_read_index_result is not equal with batch_read_index_req, there must be region_error/lock, find and return directly.
    for (auto & [region_id, resp] : batch_read_index_result)
    {
        if (resp.has_region_error())
        {
            const auto & region_error = resp.region_error();
            auto region_status = RegionException::RegionReadStatus::OTHER;
            if (region_error.has_epoch_not_match())
                region_status = RegionException::RegionReadStatus::EPOCH_NOT_MATCH;
            else if (region_error.has_not_leader())
                region_status = RegionException::RegionReadStatus::NOT_LEADER;
            else if (region_error.has_region_not_found())
                region_status = RegionException::RegionReadStatus::NOT_FOUND_TIKV;
            // Below errors seldomly happens in raftstore-v1, however, we are not sure if they will happen in v2.
            else if (region_error.has_flashbackinprogress() || region_error.has_flashbacknotprepared())
            {
                region_status = RegionException::RegionReadStatus::FLASHBACK;
            }
            else if (region_error.has_bucket_version_not_match())
            {
                LOG_DEBUG(
                    log,
                    "meet abnormal region error {}, [region_id={}]",
                    resp.region_error().DebugString(),
                    region_id);
                region_status = RegionException::RegionReadStatus::BUCKET_EPOCH_NOT_MATCH;
            }
            else if (region_error.has_key_not_in_region())
            {
                LOG_DEBUG(
                    log,
                    "meet abnormal region error {}, [region_id={}]",
                    resp.region_error().DebugString(),
                    region_id);
                region_status = RegionException::RegionReadStatus::KEY_NOT_IN_REGION;
            }
            else if (
                region_error.has_server_is_busy() || region_error.has_raft_entry_too_large()
                || region_error.has_region_not_initialized() || region_error.has_disk_full()
                || region_error.has_read_index_not_ready() || region_error.has_proposal_in_merging_mode())
            {
                LOG_DEBUG(
                    log,
                    "meet abnormal region error {}, [region_id={}]",
                    resp.region_error().DebugString(),
                    region_id);
                region_status = RegionException::RegionReadStatus::TIKV_SERVER_ISSUE;
            }
            else
            {
                LOG_DEBUG(
                    log,
                    "meet abnormal region error {}, [region_id={}]",
                    resp.region_error().DebugString(),
                    region_id);
            }
            unavailable_regions.add(region_id, region_status);
        }
        else if (resp.has_locked())
        {
            unavailable_regions.addRegionLock(region_id, LockInfoPtr(resp.release_locked()));
        }
        else
        {
            // cache read-index to avoid useless overhead about retry.
            // resp.read_index() is 0 when stale read, skip it to avoid overwriting read_index res in last retry.
            if (resp.read_index() != 0)
            {
                mvcc_query_info.addReadIndexRes(region_id, resp.read_index());
            }
        }
    }
}

std::vector<kvrpcpb::ReadIndexRequest> buildBatchReadIndexReq(
    RegionTable & region_table,
    MvccQueryInfoWrap & mvcc_query_info,
    size_t num_regions,
    const MvccQueryInfo::RegionsQueryInfo & regions_info,
    LearnerReadSnapshot & regions_snapshot,
    std::unordered_map<RegionID, kvrpcpb::ReadIndexResponse> & batch_read_index_result,
    LearnerReadStatistics & stats)
{
    std::vector<kvrpcpb::ReadIndexRequest> batch_read_index_req;
    batch_read_index_req.reserve(num_regions);

    // If using `std::numeric_limits<uint64_t>::max()`, set `start-ts` 0 to get the latest index but let read-index-worker do not record as history.
    auto read_index_tso
        = mvcc_query_info->read_tso == std::numeric_limits<uint64_t>::max() ? 0 : mvcc_query_info->read_tso;
    for (size_t region_idx = 0; region_idx < num_regions; ++region_idx)
    {
        const auto & region_to_query = regions_info[region_idx];
        const RegionID region_id = region_to_query.region_id;
        // don't stale read in test scenarios.
        bool can_stale_read = mvcc_query_info->read_tso != std::numeric_limits<uint64_t>::max()
            && read_index_tso <= region_table.getSelfSafeTS(region_id);
        if (!can_stale_read)
        {
            if (auto ori_read_index = mvcc_query_info.getReadIndexRes(region_id); ori_read_index)
            {
                auto resp = kvrpcpb::ReadIndexResponse();
                resp.set_read_index(ori_read_index);
                batch_read_index_result.emplace(region_id, std::move(resp));
                ++stats.num_cached_read_index;
            }
            else
            {
                auto & region = regions_snapshot.find(region_id)->second;
                batch_read_index_req.emplace_back(GenRegionReadIndexReq(*region, read_index_tso));
                ++stats.num_read_index_request;
            }
        }
        else
        {
            batch_read_index_result.emplace(region_id, kvrpcpb::ReadIndexResponse());
            ++stats.num_stale_read;
        }
    }
    return batch_read_index_req;
}

void doBatchReadIndex(
    std::vector<kvrpcpb::ReadIndexRequest> & batch_read_index_req,
    std::unordered_map<RegionID, kvrpcpb::ReadIndexResponse> & batch_read_index_result,
    KVStorePtr & kvstore,
    TMTContext & tmt)
{
    const auto & make_default_batch_read_index_result = [&](bool with_region_error) {
        for (const auto & req : batch_read_index_req)
        {
            auto resp = kvrpcpb::ReadIndexResponse();
            if (with_region_error)
                resp.mutable_region_error()->mutable_region_not_found();
            batch_read_index_result.emplace(req.context().region_id(), std::move(resp));
        }
    };
    [&]() {
        if (!tmt.checkRunning(std::memory_order_relaxed))
        {
            make_default_batch_read_index_result(true);
            return;
        }
        kvstore->addReadIndexEvent(1);
        SCOPE_EXIT({ kvstore->addReadIndexEvent(-1); });
        if (!tmt.checkRunning())
        {
            make_default_batch_read_index_result(true);
            return;
        }

        /// Blocking learner read. Note that learner read must be performed ahead of data read,
        /// otherwise the desired index will be blocked by the lock of data read.
        if (kvstore->getProxyHelper())
        {
            auto res = kvstore->batchReadIndex(batch_read_index_req, tmt.batchReadIndexTimeout());
            for (auto && [resp, region_id] : res)
            {
                batch_read_index_result.emplace(region_id, std::move(resp));
            }
        }
        else
        {
            // Only in mock test, `proxy_helper` will be `nullptr`. Set `read_index` to 0 and skip waiting.
            make_default_batch_read_index_result(false);
        }
    }();
}

} // namespace

LearnerReadSnapshot doLearnerRead(
    const TiDB::TableID logical_table_id,
    MvccQueryInfo & mvcc_query_info_,
    bool for_batch_cop,
    Context & context,
    const LoggerPtr & log)
{
    assert(log != nullptr);
    RUNTIME_ASSERT(
        !(context.getSharedContextDisagg()->isDisaggregatedComputeMode()
          && context.getSharedContextDisagg()->use_autoscaler));

    const bool is_wn_disagg_read = context.getDAGContext() ? context.getDAGContext()->is_disaggregated_task : false;

    auto & tmt = context.getTMTContext();

    MvccQueryInfoWrap mvcc_query_info(mvcc_query_info_, tmt, logical_table_id);
    const auto & regions_info = mvcc_query_info.getRegionsInfo();

    KVStorePtr & kvstore = tmt.getKVStore();
    LearnerReadSnapshot regions_snapshot;
    // check region is not null and store region map.
    for (const auto & info : regions_info)
    {
        auto region = kvstore->getRegion(info.region_id);
        if (region == nullptr)
        {
            LOG_WARNING(log, "region not found in KVStore, region_id={}", info.region_id);
            throw RegionException({info.region_id}, RegionException::RegionReadStatus::NOT_FOUND);
        }
        regions_snapshot.emplace(info.region_id, std::move(region));
    }
    // make sure regions are not duplicated.
    if (unlikely(regions_snapshot.size() != regions_info.size()))
        throw TiFlashException(
            Errors::Coprocessor::BadRequest,
            "Duplicate region id, n_request={} n_actual={}",
            regions_info.size(),
            regions_snapshot.size());

    const auto start_time = Clock::now();
    UnavailableRegions unavailable_regions(for_batch_cop, is_wn_disagg_read);
    LearnerReadStatistics stats;
    stats.num_regions = regions_info.size();
    // TODO: refactor this enormous lambda into smaller parts
    {
        Stopwatch batch_wait_data_watch;
        Stopwatch watch;

        /// read index
        std::unordered_map<RegionID, kvrpcpb::ReadIndexResponse> batch_read_index_result;
        RegionTable & region_table = tmt.getRegionTable();

        std::vector<kvrpcpb::ReadIndexRequest> batch_read_index_req = buildBatchReadIndexReq(
            region_table,
            mvcc_query_info,
            stats.num_regions,
            regions_info,
            regions_snapshot,
            batch_read_index_result,
            stats);

        GET_METRIC(tiflash_stale_read_count).Increment(stats.num_stale_read);
        GET_METRIC(tiflash_raft_read_index_count).Increment(batch_read_index_req.size());

        doBatchReadIndex(batch_read_index_req, batch_read_index_result, kvstore, tmt);

        {
            stats.read_index_elapsed_ms = watch.elapsedMilliseconds();
            GET_METRIC(tiflash_raft_read_index_duration_seconds).Observe(stats.read_index_elapsed_ms / 1000.0);
            const auto log_lvl
                = unavailable_regions.empty() ? Poco::Message::PRIO_DEBUG : Poco::Message::PRIO_INFORMATION;
            LOG_IMPL(
                log,
                log_lvl,
                "[Learner Read] Batch read index, num_regions={} num_requests={} num_stale_read={} num_cached_index={} "
                "num_unavailable={} "
                "cost={}ms",
                stats.num_regions,
                stats.num_read_index_request,
                stats.num_stale_read,
                stats.num_cached_read_index,
                unavailable_regions.size(),
                stats.read_index_elapsed_ms);
        }

        addressBatchReadIndexError(batch_read_index_result, unavailable_regions, mvcc_query_info, log);

        /// wait index
        watch.restart(); // restart to count the elapsed of wait index

        const auto wait_index_timeout_ms = tmt.waitIndexTimeout();
        for (size_t region_idx = 0; region_idx < stats.num_regions; ++region_idx)
        {
            const auto & region_to_query = regions_info[region_idx];
            // if region is unavailable, skip wait index.
            if (unavailable_regions.contains(region_to_query.region_id))
                continue;

            auto & region = regions_snapshot.find(region_to_query.region_id)->second;

            const auto total_wait_index_elapsed_ms = watch.elapsedMilliseconds();
            const auto index_to_wait = batch_read_index_result.find(region_to_query.region_id)->second.read_index();
            if (wait_index_timeout_ms == 0 || total_wait_index_elapsed_ms <= wait_index_timeout_ms)
            {
                // Wait index timeout is disabled; or timeout is enabled but not happen yet, wait index for
                // a specify Region.
                auto [wait_res, time_cost] = region->waitIndex(
                    index_to_wait,
                    tmt.waitIndexTimeout(),
                    [&tmt]() { return tmt.checkRunning(); },
                    log);
                if (wait_res != WaitIndexStatus::Finished)
                {
                    auto current = region->appliedIndex();
                    unavailable_regions.addRegionWaitIndexTimeout(region_to_query.region_id, index_to_wait, current);
                    continue; // error happens, check next region quickly
                }
                if (time_cost > 0)
                {
                    // Only record information if wait-index does happen
                    GET_METRIC(tiflash_raft_wait_index_duration_seconds).Observe(time_cost);
                }
            }
            else
            {
                // Wait index timeout is enabled && timeout happens, simply check the Region index instead of wait index
                // for Regions one by one.
                if (!region->checkIndex(index_to_wait))
                {
                    auto current = region->appliedIndex();
                    unavailable_regions.addRegionWaitIndexTimeout(region_to_query.region_id, index_to_wait, current);
                }
                continue; // timeout happens, check next region quickly
            }

            if (unlikely(!mvcc_query_info->resolve_locks))
            {
                continue;
            }

            // Try to resolve locks and flush data into storage layer
            const Int64 physical_table_id = region_to_query.physical_table_id;
            auto res = RegionTable::resolveLocksAndWriteRegion(
                tmt,
                physical_table_id,
                region,
                mvcc_query_info->read_tso,
                region_to_query.bypass_lock_ts,
                region_to_query.version,
                region_to_query.conf_version,
                log);

            std::visit(
                variant_op::overloaded{
                    [&](LockInfoPtr & lock) { unavailable_regions.addRegionLock(region->id(), std::move(lock)); },
                    [&](RegionException::RegionReadStatus & status) {
                        if (status != RegionException::RegionReadStatus::OK)
                        {
                            LOG_WARNING(
                                log,
                                "Check memory cache, region_id={} version={} handle_range={} status={}",
                                region_to_query.region_id,
                                region_to_query.version,
                                RecordKVFormat::DecodedTiKVKeyRangeToDebugString(region_to_query.range_in_table),
                                magic_enum::enum_name(status));
                            unavailable_regions.add(region->id(), status);
                        }
                    },
                },
                res);
        } // wait index for next region
        GET_METRIC(tiflash_syncing_data_freshness).Observe(batch_wait_data_watch.elapsedSeconds()); // For DBaaS SLI
        stats.wait_index_elapsed_ms = watch.elapsedMilliseconds();

        const auto log_lvl = unavailable_regions.empty() ? Poco::Message::PRIO_DEBUG : Poco::Message::PRIO_INFORMATION;
        LOG_IMPL(
            log,
            log_lvl,
            "[Learner Read] Finish wait index and resolve locks, wait_cost={}ms n_regions={} n_unavailable={}",
            stats.wait_index_elapsed_ms,
            stats.num_regions,
            unavailable_regions.size());
    }

    // Throw Region exception if there are any unavailable regions, the exception will be handled in the
    // following methods
    // - `CoprocessorHandler::execute`
    // - `FlashService::EstablishDisaggTask`
    // - `DAGDriver::execute`
    // - `DAGStorageInterpreter::doBatchCopLearnerRead`
    // - `DAGStorageInterpreter::buildLocalStreamsForPhysicalTable`
    // - `DAGStorageInterpreter::buildLocalExecForPhysicalTable`
    unavailable_regions.tryThrowRegionException();

    const auto end_time = Clock::now();
    const auto time_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    // Use info level if read wait index run slow or any unavailable region exists
    const auto log_lvl = (time_elapsed_ms > 1000 || !unavailable_regions.empty()) //
        ? Poco::Message::PRIO_INFORMATION
        : Poco::Message::PRIO_DEBUG;
    LOG_IMPL(
        log,
        log_lvl,
        "[Learner Read] batch read index | wait index"
        " total_cost={} read_cost={} wait_cost={} n_regions={} n_stale_read={} n_unavailable={}",
        time_elapsed_ms,
        stats.read_index_elapsed_ms,
        stats.wait_index_elapsed_ms,
        stats.num_regions,
        stats.num_stale_read,
        unavailable_regions.size());

    if (auto * dag_context = context.getDAGContext())
    {
        mvcc_query_info->scan_context->num_stale_read = stats.num_stale_read;
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
        }

        if (status != RegionException::RegionReadStatus::OK)
        {
            fail_region_ids.emplace(region_query_info.region_id);
            fail_status = status;
            LOG_WARNING(
                log,
                "Check after snapshot acquired from storage, region_id={} version={} handle_range={} status={}",
                region_query_info.region_id,
                region_query_info.version,
                RecordKVFormat::DecodedTiKVKeyRangeToDebugString(region_query_info.range_in_table),
                magic_enum::enum_name(status));
        }
    }

    if (!fail_region_ids.empty())
    {
        throw RegionException(std::move(fail_region_ids), fail_status);
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
