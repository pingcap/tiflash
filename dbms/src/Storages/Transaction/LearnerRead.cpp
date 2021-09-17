#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
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
#include <fmt/format.h>
#include <kvproto/kvrpcpb.pb.h>

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

    void tryThrowRegionException(const MvccQueryInfo::RegionsQueryInfo & regions_info)
    {
        auto lock = genLockGuard();

        // For batch-cop request, all unavailable regions, include the ones with lock exception, should be collected and retry next round.
        // For normal cop request, which only contains one region, LockException should be thrown directly and let upper layer(like client-c, tidb, tispark) handle it.
        if (regions_info.size() == 1 && region_lock)
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
    std::atomic<RegionException::RegionReadStatus> status{RegionException::RegionReadStatus::NOT_FOUND};
};

class MvccQueryInfoWrap
    : private boost::noncopyable
    , public LockWrap
{
    using Base = MvccQueryInfo;
    Base & inner;
    std::optional<Base::RegionsQueryInfo> regions_info;
    Base::RegionsQueryInfo * regions_info_ptr;

    size_t concurrent = 1;

public:
    MvccQueryInfoWrap(Base & mvcc_query_info, TMTContext & tmt, const TiDB::TableID table_id, size_t num_streams)
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
            auto regions = tmt.getRegionTable().getRegionsByTable(table_id);
            regions_info_ptr->reserve(regions.size());
            for (const auto & [id, region] : regions)
            {
                if (region == nullptr)
                    continue;
                regions_info_ptr->emplace_back(
                    RegionQueryInfo{id, region->version(), region->confVer(), region->getRange()->rawKeys(), {}});
            }
        }

        // Adjust concurrency by the min of num of regions and num of streams * mvcc_query_info.concurrent
        concurrent = std::min(static_cast<size_t>(num_streams * inner.concurrent), regions_info_ptr->size());
        // We apply batch read index in the proxy side, limit the concurrency in the TiFlash side to reduce the overhead
        concurrent = std::min(tmt.replicaReadMaxThread(), concurrent);
        concurrent = std::max(1, concurrent);
    }
    Base * operator->() { return &inner; }

    size_t getConcurrentNum() const { return concurrent; }

    const Base::RegionsQueryInfo & getRegionsInfo() const { return *regions_info_ptr; }


    LearnerReadSnapshot getRegionsSnapshot(const KVStorePtr & kvstore, Poco::Logger * log)
    {
        // Check region is available in kvstore and get a snapshot.
        LearnerReadSnapshot regions_snapshot;
        for (const auto & info : *regions_info_ptr)
        {
            auto region = kvstore->getRegion(info.region_id);
            if (region == nullptr)
            {
                LOG_WARNING(log, "[region " << info.region_id << "] is not found in KVStore, try again");
                throw RegionException({info.region_id}, RegionException::RegionReadStatus::NOT_FOUND);
            }
            regions_snapshot.emplace(info.region_id, std::move(region));
        }
        // make sure regions are not duplicated.
        if (unlikely(regions_snapshot.size() != regions_info_ptr->size()))
            throw Exception("Duplicate region id", ErrorCodes::LOGICAL_ERROR);
        return regions_snapshot;
    }

    using ReadIndexRequests = std::vector<kvrpcpb::ReadIndexRequest>;
    using BatchReadIndexResult = std::unordered_map<RegionID, kvrpcpb::ReadIndexResponse>;
    ReadIndexRequests genBatchReadIndexRequests(
        const size_t region_begin_idx,
        const size_t region_end_idx,
        const LearnerReadSnapshot & regions_snapshot,
        BatchReadIndexResult & batch_read_index_result)
    {
        ReadIndexRequests reqs;
        reqs.reserve(region_end_idx - region_begin_idx); // TODO: eliminate it
        const auto & regions_info = getRegionsInfo();
        {
            auto lock = genLockGuard();
            for (size_t region_idx = region_begin_idx; region_idx < region_end_idx; ++region_idx)
            {
                const auto & region_to_query = regions_info[region_idx];
                const RegionID region_id = region_to_query.region_id;
                if (auto it = inner.read_index_res.find(region_id); it != inner.read_index_res.end() && it->second != 0)
                {
                    // Get the cached read index result from `inner.read_index_res`
                    auto resp = kvrpcpb::ReadIndexResponse();
                    resp.set_read_index(it->second);
                    batch_read_index_result.emplace(region_id, std::move(resp));
                }
                else
                {
                    // Generate a new read index requst
                    const auto & region = regions_snapshot.find(region_id)->second;
                    reqs.emplace_back(GenRegionReadIndexReq(*region, inner.read_tso));
                }
            }
        }
        return reqs;
    }

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

using BatchReadIndexRequests = MvccQueryInfoWrap::ReadIndexRequests;
using BatchReadIndexResult = MvccQueryInfoWrap::BatchReadIndexResult;
BatchReadIndexResult doBatchReadIndex(const BatchReadIndexRequests & batch_read_index_req, TMTContext & tmt)
{
    const auto & make_default_batch_read_index_result = [&]() -> BatchReadIndexResult {
        BatchReadIndexResult batch_read_index_result;
        for (const auto & req : batch_read_index_req)
        {
            batch_read_index_result.emplace(req.context().region_id(), kvrpcpb::ReadIndexResponse());
        }
        return batch_read_index_result;
    };

    auto & kvstore = tmt.getKVStore();
    if (!tmt.checkRunning(std::memory_order_relaxed))
    {
        return make_default_batch_read_index_result();
    }

    kvstore->addReadIndexEvent(1);
    SCOPE_EXIT({ kvstore->addReadIndexEvent(-1); });
    if (!tmt.checkRunning())
    {
        return make_default_batch_read_index_result();
    }

    const auto * proxy_helper = kvstore->getProxyHelper();
    if (proxy_helper == nullptr)
    {
        return make_default_batch_read_index_result();
    }

    /// Blocking learner read. Note that learner read must be performed ahead of data read,
    /// otherwise the desired index will be blocked by the lock of data read.
    BatchReadIndexResult batch_read_index_result;
    auto res = proxy_helper->batchReadIndex(batch_read_index_req, tmt.batchReadIndexTimeout());
    for (auto && [resp, region_id] : res)
    {
        GET_METRIC(tiflash_raft_read_index_count).Increment(batch_read_index_req.size());
        batch_read_index_result.emplace(region_id, std::move(resp));
    }
    return batch_read_index_result;
}

LearnerReadSnapshot doLearnerRead(
    const TiDB::TableID table_id,
    MvccQueryInfo & mvcc_query_info_,
    size_t num_streams,
    bool wait_index_timeout_as_region_not_found,
    TMTContext & tmt,
    Poco::Logger * log)
{
    assert(log != nullptr);

    MvccQueryInfoWrap mvcc_query_info(mvcc_query_info_, tmt, table_id, num_streams);
    KVStorePtr & kvstore = tmt.getKVStore();
    LearnerReadSnapshot regions_snapshot = mvcc_query_info.getRegionsSnapshot(kvstore, log);

    const size_t num_regions = regions_snapshot.size();
    const size_t concurrent_num = mvcc_query_info.getConcurrentNum();
    const size_t batch_size = num_regions / concurrent_num;
    const auto & regions_info = mvcc_query_info.getRegionsInfo();

    UnavailableRegions unavailable_regions;
    const auto batch_wait_index = [&](const size_t region_begin_idx) -> void {
        Stopwatch batch_wait_data_watch;
        Stopwatch watch;

        const size_t region_end_idx = std::min(region_begin_idx + batch_size, num_regions);
        const size_t ori_batch_region_size = region_end_idx - region_begin_idx;

        // FIXME: If proxy is not ready, `batch_read_index_result` would be full of Raft index "0".
        // It will make trouble for later checks.
        BatchReadIndexResult batch_read_index_result;
        {
            auto batch_read_index_req = mvcc_query_info.genBatchReadIndexRequests(
                region_begin_idx,
                region_end_idx,
                regions_snapshot,
                batch_read_index_result);

            batch_read_index_result = doBatchReadIndex(batch_read_index_req, tmt);
            const size_t cached_size = ori_batch_region_size - batch_read_index_req.size();
            auto read_index_elapsed_ms = watch.elapsedMilliseconds();
            GET_METRIC(tiflash_raft_read_index_duration_seconds).Observe(read_index_elapsed_ms / 1000.0);
            LOG_DEBUG(
                log,
                fmt::format("Batch read index, original size {}, send & get {} message, cost {}ms",
                            ori_batch_region_size,
                            batch_read_index_result.size(),
                            read_index_elapsed_ms);
                do {
                    if (cached_size)
                    {
                        oss_internal_rare << fmt::format(", {} in cache", cached_size);
                    }
                } while (0));
        }

        // If there are region_error/unresolved transaction lock, mark those Region unavailable.
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
                // FIXME: Not cache read index == 0
                mvcc_query_info.addReadIndexRes(region_id, resp.read_index());
            }
        }

        watch.restart(); // restart to count the elapsed of wait index

        auto handle_wait_timeout_region = [&unavailable_regions, wait_index_timeout_as_region_not_found](const DB::RegionID region_id) {
            if (wait_index_timeout_as_region_not_found)
            {
                // If server is being terminated / time-out, add the region_id into `unavailable_regions` to other store.
                unavailable_regions.add(region_id, RegionException::RegionReadStatus::NOT_FOUND);
                return;
            }
            // TODO: Maybe collect all the Regions that happen wait index timeout instead of just throwing one Region id
            throw TiFlashException(fmt::format("Region {} is unavailable", region_id), Errors::Coprocessor::RegionError);
        };
        const auto wait_index_timeout_ms = tmt.waitIndexTimeout();
        for (size_t region_idx = region_begin_idx; region_idx < region_end_idx; ++region_idx)
        {
            const auto & region_to_query = regions_info[region_idx];

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
                auto [wait_res, time_cost] = region->waitIndex(index_to_wait, tmt);
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
                    table_id,
                    region,
                    mvcc_query_info->read_tso,
                    region_to_query.bypass_lock_ts,
                    region_to_query.version,
                    region_to_query.conf_version,
                    log);

                std::visit(variant_op::overloaded{
                               [&](LockInfoPtr & lock) { unavailable_regions.setRegionLock(region->id(), std::move(lock)); },
                               [&](RegionException::RegionReadStatus & status) {
                                   if (status != RegionException::RegionReadStatus::OK)
                                   {
                                       LOG_WARNING(log,
                                                   "Check memory cache, region "
                                                       << region_to_query.region_id << ", version " << region_to_query.version << ", handle range "
                                                       << RecordKVFormat::DecodedTiKVKeyRangeToDebugString(region_to_query.range_in_table)
                                                       << ", status " << RegionException::RegionReadStatusString(status));
                                       unavailable_regions.add(region->id(), status);
                                   }
                               },
                           },
                           res);
            }
        }
        GET_METRIC(tiflash_syncing_data_freshness).Observe(batch_wait_data_watch.elapsedSeconds()); // For DBaaS SLI
        auto wait_index_elapsed_ms = watch.elapsedMilliseconds();
        LOG_DEBUG(log,
                  fmt::format(
                      "Finish wait index | resolve locks | check memory cache for {} regions, cost {}ms, {} unavailable regions",
                      ori_batch_region_size,
                      wait_index_elapsed_ms,
                      unavailable_regions.size()));
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

    unavailable_regions.tryThrowRegionException(regions_info);

    auto end_time = Clock::now();
    LOG_DEBUG(log,
              "[Learner Read] batch read index | wait index cost "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count()
                  << " ms totally, regions_num=" << num_regions << ", concurrency=" << concurrent_num);

    return regions_snapshot;
}

/// Ensure regions' info after getting streams from storage layer.
void validateQueryInfo(
    const MvccQueryInfo & mvcc_query_info,
    const LearnerReadSnapshot & regions_snapshot,
    TMTContext & tmt,
    Poco::Logger * log)
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
            LOG_WARNING(log,
                        "Check after read from Storage, region "
                            << region_query_info.region_id << ", version " << region_query_info.version //
                            << ", handle range " << RecordKVFormat::DecodedTiKVKeyRangeToDebugString(region_query_info.range_in_table)
                            << ", status " << RegionException::RegionReadStatusString(status));
        }
    }

    if (!fail_region_ids.empty())
    {
        throw RegionException(std::move(fail_region_ids), fail_status);
    }
}

} // namespace DB
