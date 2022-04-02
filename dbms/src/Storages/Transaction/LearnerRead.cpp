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

#include <Common/Logger.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LearnerRead.h>
#include <Storages/Transaction/LockException.h>
#include <Storages/Transaction/ProxyFFI.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/RegionExecutionResult.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/Types.h>
#include <Storages/Transaction/Utils.h>
#include <common/ThreadPool.h>
#include <common/likely.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <ext/scope_guard.h>

namespace DB
{
class LockWrap
{
    mutable std::mutex mutex;
    bool need_lock{true};

protected:
    std::unique_lock<std::mutex> genLockGuard() const
    {
        if (need_lock)
            return std::unique_lock(mutex);
        return {};
    }

public:
    void setNoNeedLock() { need_lock = false; }
};

struct UnavailableRegions : public LockWrap
{
    using Result = RegionException::UnavailableRegions;

    void add(RegionID id, RegionException::RegionReadStatus status_)
    {
        status = status_;
        auto lock = genLockGuard();
        doAdd(id);
    }

    size_t size() const
    {
        auto lock = genLockGuard();
        return ids.size();
    }

    bool empty() const { return size() == 0; }

    void setRegionLock(RegionID region_id_, LockInfoPtr && region_lock_)
    {
        auto lock = genLockGuard();
        region_lock = std::pair(region_id_, std::move(region_lock_));
        doAdd(region_id_);
    }

    void tryThrowRegionException(bool batch_cop)
    {
        auto lock = genLockGuard();

        // For batch-cop request, all unavailable regions, include the ones with lock exception, should be collected and retry next round.
        // For normal cop request, which only contains one region, LockException should be thrown directly and let upper layer(like client-c, tidb, tispark) handle it.
        if (!batch_cop && region_lock)
            throw LockException(region_lock->first, std::move(region_lock->second));

        if (!ids.empty())
            throw RegionException(std::move(ids), status);
    }

    bool contains(RegionID region_id) const
    {
        auto lock = genLockGuard();
        return ids.count(region_id);
    }

private:
    inline void doAdd(RegionID id) { ids.emplace(id); }

    RegionException::UnavailableRegions ids;
    std::optional<std::pair<RegionID, LockInfoPtr>> region_lock;
    std::atomic<RegionException::RegionReadStatus> status{RegionException::RegionReadStatus::NOT_FOUND}; // NOLINT
};

class MvccQueryInfoWrap
    : boost::noncopyable
    , public LockWrap
{
    using Base = MvccQueryInfo;
    Base & inner;
    std::optional<Base::RegionsQueryInfo> regions_info;
    Base::RegionsQueryInfo * regions_info_ptr;

public:
    MvccQueryInfoWrap(Base & mvcc_query_info, TMTContext & tmt, const TiDB::TableID logical_table_id)
        : inner(mvcc_query_info)
    {
        if (likely(!inner.regions_query_info.empty()))
        {
            regions_info_ptr = &inner.regions_query_info;
        }
        else
        {
            regions_info = Base::RegionsQueryInfo();
            regions_info_ptr = &*regions_info;
            // Only for test, because regions_query_info should never be empty if query is from TiDB or TiSpark.
            // todo support partition table
            auto regions = tmt.getRegionTable().getRegionsByTable(logical_table_id);
            regions_info_ptr->reserve(regions.size());
            for (const auto & [id, region] : regions)
            {
                if (region == nullptr)
                    continue;
                regions_info_ptr->emplace_back(
                    RegionQueryInfo{id, region->version(), region->confVer(), logical_table_id, region->getRange()->rawKeys(), {}});
            }
        }
    }
    Base * operator->() { return &inner; }

    const Base::RegionsQueryInfo & getRegionsInfo() const { return *regions_info_ptr; }
    void addReadIndexRes(RegionID region_id, UInt64 read_index)
    {
        auto lock = genLockGuard();
        inner.read_index_res[region_id] = read_index;
    }
    UInt64 getReadIndexRes(RegionID region_id) const
    {
        auto lock = genLockGuard();
        if (auto it = inner.read_index_res.find(region_id); it != inner.read_index_res.end())
            return it->second;
        return 0;
    }
};

LearnerReadSnapshot doLearnerRead(
    const TiDB::TableID logical_table_id,
    MvccQueryInfo & mvcc_query_info_,
    size_t num_streams,
    bool for_batch_cop,
    Context & context,
    const LoggerPtr & log)
{
    assert(log != nullptr);

    auto & tmt = context.getTMTContext();

    MvccQueryInfoWrap mvcc_query_info(mvcc_query_info_, tmt, logical_table_id);
    const auto & regions_info = mvcc_query_info.getRegionsInfo();

    // adjust concurrency by num of regions or num of streams * mvcc_query_info.concurrent
    size_t concurrent_num = std::max(1, std::min(static_cast<size_t>(num_streams * mvcc_query_info->concurrent), regions_info.size()));

    // use single thread to do replica read by default because there is some overhead from thread pool itself.
    concurrent_num = std::min(tmt.replicaReadMaxThread(), concurrent_num);

    KVStorePtr & kvstore = tmt.getKVStore();
    LearnerReadSnapshot regions_snapshot;
    // check region is not null and store region map.
    for (const auto & info : regions_info)
    {
        auto region = kvstore->getRegion(info.region_id);
        if (region == nullptr)
        {
            LOG_FMT_WARNING(log, "[region {}] is not found in KVStore, try again", info.region_id);
            throw RegionException({info.region_id}, RegionException::RegionReadStatus::NOT_FOUND);
        }
        regions_snapshot.emplace(info.region_id, std::move(region));
    }
    // make sure regions are not duplicated.
    if (unlikely(regions_snapshot.size() != regions_info.size()))
        throw Exception("Duplicate region id", ErrorCodes::LOGICAL_ERROR);

    const size_t num_regions = regions_info.size();

    const size_t batch_size = num_regions / concurrent_num;
    UnavailableRegions unavailable_regions;
    const auto batch_wait_index = [&](const size_t region_begin_idx) -> void {
        Stopwatch batch_wait_data_watch;
        Stopwatch watch;

        const size_t region_end_idx = std::min(region_begin_idx + batch_size, num_regions);
        const size_t ori_batch_region_size = region_end_idx - region_begin_idx;
        std::unordered_map<RegionID, kvrpcpb::ReadIndexResponse> batch_read_index_result;

        std::vector<kvrpcpb::ReadIndexRequest> batch_read_index_req;
        batch_read_index_req.reserve(ori_batch_region_size);

        {
            // If using `std::numeric_limits<uint64_t>::max()`, set `start-ts` 0 to get the latest index but let read-index-worker do not record as history.
            auto read_index_tso = mvcc_query_info->read_tso == std::numeric_limits<uint64_t>::max() ? 0 : mvcc_query_info->read_tso;

            for (size_t region_idx = region_begin_idx; region_idx < region_end_idx; ++region_idx)
            {
                const auto & region_to_query = regions_info[region_idx];
                const RegionID region_id = region_to_query.region_id;
                if (auto ori_read_index = mvcc_query_info.getReadIndexRes(region_id); ori_read_index)
                {
                    auto resp = kvrpcpb::ReadIndexResponse();
                    resp.set_read_index(ori_read_index);
                    batch_read_index_result.emplace(region_id, std::move(resp));
                }
                else
                {
                    auto & region = regions_snapshot.find(region_id)->second;
                    batch_read_index_req.emplace_back(GenRegionReadIndexReq(*region, read_index_tso));
                }
            }
        }

        GET_METRIC(tiflash_raft_read_index_count).Increment(batch_read_index_req.size());

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

        {
            auto read_index_elapsed_ms = watch.elapsedMilliseconds();
            GET_METRIC(tiflash_raft_read_index_duration_seconds).Observe(read_index_elapsed_ms / 1000.0);
            const size_t cached_size = ori_batch_region_size - batch_read_index_req.size();

            LOG_FMT_DEBUG(
                log,
                "Batch read index, original size {}, send & get {} message, cost {}ms{}",
                ori_batch_region_size,
                batch_read_index_req.size(),
                read_index_elapsed_ms,
                (cached_size != 0) ? (fmt::format(", {} in cache", cached_size)) : "");

            watch.restart(); // restart to count the elapsed of wait index
        }

        // if size of batch_read_index_result is not equal with batch_read_index_req, there must be region_error/lock, find and return directly.
        for (auto & [region_id, resp] : batch_read_index_result)
        {
            if (resp.has_region_error())
            {
                const auto & region_error = resp.region_error();
                auto region_status = RegionException::RegionReadStatus::NOT_FOUND;
                if (region_error.has_epoch_not_match())
                    region_status = RegionException::RegionReadStatus::EPOCH_NOT_MATCH;
                unavailable_regions.add(region_id, region_status);
            }
            else if (resp.has_locked())
            {
                unavailable_regions.setRegionLock(region_id, LockInfoPtr(resp.release_locked()));
            }
            else
            {
                // cache read-index to avoid useless overhead about retry.
                mvcc_query_info.addReadIndexRes(region_id, resp.read_index());
            }
        }

        auto handle_wait_timeout_region = [&unavailable_regions, for_batch_cop](const DB::RegionID region_id) {
            if (!for_batch_cop)
            {
                // If server is being terminated / time-out, add the region_id into `unavailable_regions` to other store.
                unavailable_regions.add(region_id, RegionException::RegionReadStatus::NOT_FOUND);
                return;
            }
            // TODO: Maybe collect all the Regions that happen wait index timeout instead of just throwing one Region id
            throw TiFlashException(fmt::format("Region {} is unavailable", region_id), Errors::Coprocessor::RegionError);
        };
        const auto wait_index_timeout_ms = tmt.waitIndexTimeout();
        for (size_t region_idx = region_begin_idx, read_index_res_idx = 0; region_idx < region_end_idx; ++region_idx, ++read_index_res_idx)
        {
            const auto & region_to_query = regions_info[region_idx];
            Int64 physical_table_id = region_to_query.physical_table_id;

            // if region is unavailable, skip wait index.
            if (unavailable_regions.contains(region_to_query.region_id))
                continue;

            auto & region = regions_snapshot.find(region_to_query.region_id)->second;

            auto total_wait_index_elapsed_ms = watch.elapsedMilliseconds();
            auto index_to_wait = batch_read_index_result.find(region_to_query.region_id)->second.read_index();
            if (wait_index_timeout_ms == 0 || total_wait_index_elapsed_ms <= wait_index_timeout_ms)
            {
                // Wait index timeout is disabled; or timeout is enabled but not happen yet, wait index for
                // a specify Region.
                auto [wait_res, time_cost] = region->waitIndex(index_to_wait, tmt.waitIndexTimeout(), [&tmt]() { return tmt.checkRunning(); });
                if (wait_res != WaitIndexResult::Finished)
                {
                    handle_wait_timeout_region(region_to_query.region_id);
                    continue;
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
                    handle_wait_timeout_region(region_to_query.region_id);
                    continue;
                }
            }

            // Try to resolve locks and flush data into storage layer
            if (mvcc_query_info->resolve_locks)
            {
                auto res = RegionTable::resolveLocksAndWriteRegion(
                    tmt,
                    physical_table_id,
                    region,
                    mvcc_query_info->read_tso,
                    region_to_query.bypass_lock_ts,
                    region_to_query.version,
                    region_to_query.conf_version,
                    log->getLog());

                std::visit(
                    variant_op::overloaded{
                        [&](LockInfoPtr & lock) { unavailable_regions.setRegionLock(region->id(), std::move(lock)); },
                        [&](RegionException::RegionReadStatus & status) {
                            if (status != RegionException::RegionReadStatus::OK)
                            {
                                LOG_FMT_WARNING(
                                    log,
                                    "Check memory cache, region {}, version {}, handle range {}, status {}",
                                    region_to_query.region_id,
                                    region_to_query.version,
                                    RecordKVFormat::DecodedTiKVKeyRangeToDebugString(region_to_query.range_in_table),
                                    RegionException::RegionReadStatusString(status));
                                unavailable_regions.add(region->id(), status);
                            }
                        },
                    },
                    res);
            }
        }
        GET_METRIC(tiflash_syncing_data_freshness).Observe(batch_wait_data_watch.elapsedSeconds()); // For DBaaS SLI
        auto wait_index_elapsed_ms = watch.elapsedMilliseconds();
        LOG_FMT_DEBUG(
            log,
            "Finish wait index | resolve locks | check memory cache for {} regions, cost {}ms, {} unavailable regions",
            batch_read_index_req.size(),
            wait_index_elapsed_ms,
            unavailable_regions.size());
    };

    auto start_time = Clock::now();
    if (concurrent_num <= 1)
    {
        mvcc_query_info.setNoNeedLock();
        unavailable_regions.setNoNeedLock();
        batch_wait_index(0);
    }
    else
    {
        ::ThreadPool pool(concurrent_num);
        for (size_t region_begin_idx = 0; region_begin_idx < num_regions; region_begin_idx += batch_size)
        {
            pool.schedule([&batch_wait_index, region_begin_idx] { batch_wait_index(region_begin_idx); });
        }
        pool.wait();
    }

    unavailable_regions.tryThrowRegionException(for_batch_cop);

    auto end_time = Clock::now();
    LOG_FMT_DEBUG(
        log,
        "[Learner Read] batch read index | wait index cost {} ms totally, regions_num={}, concurrency={}",
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count(),
        num_regions,
        concurrent_num);

    if (auto * dag_context = context.getDAGContext())
    {
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
            LOG_FMT_WARNING(
                log,
                "Check after read from Storage, region {}, version {}, handle range {}, status {}",
                region_query_info.region_id,
                region_query_info.version,
                RecordKVFormat::DecodedTiKVKeyRangeToDebugString(region_query_info.range_in_table),
                RegionException::RegionReadStatusString(status));
        }
    }

    if (!fail_region_ids.empty())
    {
        throw RegionException(std::move(fail_region_ids), fail_status);
    }
}

MvccQueryInfo::MvccQueryInfo(bool resolve_locks_, UInt64 read_tso_)
    : read_tso(read_tso_)
    , resolve_locks(read_tso_ == std::numeric_limits<UInt64>::max() ? false : resolve_locks_)
{
    // using `std::numeric_limits::max()` to resolve lock may break basic logic.
}

} // namespace DB
