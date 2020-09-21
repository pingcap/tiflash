#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/KVStore.h>
#include <Storages/Transaction/LearnerRead.h>
#include <Storages/Transaction/RegionException.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/ThreadPool.h>
#include <common/likely.h>

namespace DB
{

void throwRetryRegion(const MvccQueryInfo::RegionsQueryInfo & regions_info, RegionException::RegionReadStatus status)
{
    std::vector<RegionID> region_ids;
    region_ids.reserve(regions_info.size());
    for (const auto & info : regions_info)
        region_ids.push_back(info.region_id);
    throw RegionException(std::move(region_ids), status);
}

/// Check whether region is invalid or not.
RegionException::RegionReadStatus isValidRegion(const RegionQueryInfo & region_to_query, const RegionPtr & region_in_mem)
{
    if (region_in_mem->peerState() != raft_serverpb::PeerState::Normal)
        return RegionException::RegionReadStatus::NOT_FOUND;

    const auto & [version, conf_ver, key_range] = region_in_mem->dumpVersionRange();
    (void)key_range;
    if (version != region_to_query.version || conf_ver != region_to_query.conf_version)
        return RegionException::RegionReadStatus::VERSION_ERROR;

    return RegionException::RegionReadStatus::OK;
}

LearnerReadSnapshot doLearnerRead(const TiDB::TableID table_id, //
    const MvccQueryInfo & mvcc_query_info,                      //
    size_t num_streams, TMTContext & tmt, Poco::Logger * log)
{
    assert(log != nullptr);

    const bool resolve_locks = mvcc_query_info.resolve_locks;
    const Timestamp start_ts = mvcc_query_info.read_tso;
    MvccQueryInfo::RegionsQueryInfo regions_info;
    if (likely(!mvcc_query_info.regions_query_info.empty()))
    {
        regions_info = mvcc_query_info.regions_query_info;
    }
    else
    {
        // Only for test, because regions_query_info should never be empty if query is from TiDB or TiSpark.
        auto regions = tmt.getRegionTable().getRegionsByTable(table_id);
        regions_info.reserve(regions.size());
        for (const auto & [id, region] : regions)
        {
            if (region == nullptr)
                continue;
            regions_info.emplace_back(RegionQueryInfo{id, region->version(), region->confVer(), region->getRange()->rawKeys(), {}});
        }
    }

    // adjust concurrency by num of regions or num of streams * mvcc_query_info.concurrent
    size_t concurrent_num = std::max(1, std::min(static_cast<size_t>(num_streams * mvcc_query_info.concurrent), regions_info.size()));

    KVStorePtr & kvstore = tmt.getKVStore();
    LearnerReadSnapshot regions_snapshot;
    // check region is not null and store region map.
    for (const auto & info : regions_info)
    {
        auto region = kvstore->getRegion(info.region_id);
        if (region == nullptr)
        {
            LOG_WARNING(log, "[region " << info.region_id << "] is not found in KVStore, try again");
            throwRetryRegion(regions_info, RegionException::RegionReadStatus::NOT_FOUND);
        }
        regions_snapshot.emplace(info.region_id, std::move(region));
    }
    // make sure regions are not duplicated.
    if (unlikely(regions_snapshot.size() != regions_info.size()))
        throw Exception("Duplicate region id", ErrorCodes::LOGICAL_ERROR);

    auto metrics = tmt.getContext().getTiFlashMetrics();
    const size_t num_regions = regions_info.size();
    const size_t batch_size = num_regions / concurrent_num;
    std::atomic_uint8_t region_status = RegionException::RegionReadStatus::OK;
    const auto batch_wait_index = [&, resolve_locks, start_ts](const size_t region_begin_idx) -> void {
        const size_t region_end_idx = std::min(region_begin_idx + batch_size, num_regions);
        for (size_t region_idx = region_begin_idx; region_idx < region_end_idx; ++region_idx)
        {
            // If any threads meets an error, just return.
            if (region_status != RegionException::RegionReadStatus::OK)
                return;

            RegionQueryInfo & region_to_query = regions_info[region_idx];
            const RegionID region_id = region_to_query.region_id;
            auto region = regions_snapshot[region_id];

            auto status = isValidRegion(region_to_query, region);
            if (status != RegionException::RegionReadStatus::OK)
            {
                region_status = status;
                LOG_WARNING(log,
                    "Check memory cache, region "
                        << region_id << ", version " << region_to_query.version << ", handle range ["
                        << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<true>(*region_to_query.range_in_table.first) << ", "
                        << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<false>(*region_to_query.range_in_table.second)
                        << ") , status " << RegionException::RegionReadStatusString(status));
                return;
            }

            GET_METRIC(metrics, tiflash_raft_read_index_count).Increment();
            Stopwatch read_index_watch;

            /// Blocking learner read. Note that learner read must be performed ahead of data read,
            /// otherwise the desired index will be blocked by the lock of data read.
            auto read_index_result = region->learnerRead();
            GET_METRIC(metrics, tiflash_raft_read_index_duration_seconds).Observe(read_index_watch.elapsedSeconds());
            if (read_index_result.region_unavailable)
            {
                // client-c detect region removed. Set region_status and continue.
                region_status = RegionException::RegionReadStatus::NOT_FOUND;
                continue;
            }
            else if (read_index_result.region_epoch_not_match)
            {
                region_status = RegionException::RegionReadStatus::VERSION_ERROR;
                continue;
            }
            else
            {
                Stopwatch wait_index_watch;
                if (region->waitIndex(read_index_result.read_index, tmt.getTerminated()))
                {
                    region_status = RegionException::RegionReadStatus::NOT_FOUND;
                    continue;
                }
                GET_METRIC(metrics, tiflash_raft_wait_index_duration_seconds).Observe(wait_index_watch.elapsedSeconds());
            }
            if (resolve_locks)
            {
                status = RegionTable::resolveLocksAndWriteRegion( //
                    tmt,                                          //
                    table_id,                                     //
                    region,                                       //
                    start_ts,                                     //
                    region_to_query.bypass_lock_ts,               //
                    region_to_query.version,                      //
                    region_to_query.conf_version,                 //
                    region_to_query.range_in_table, log);

                if (status != RegionException::RegionReadStatus::OK)
                {
                    LOG_WARNING(log,
                        "Check memory cache, region "
                            << region_id << ", version " << region_to_query.version << ", handle range ["
                            << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<true>(*region_to_query.range_in_table.first) << ", "
                            << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<false>(*region_to_query.range_in_table.second)
                            << ") , status " << RegionException::RegionReadStatusString(status));
                    region_status = status;
                }
            }
        }
    };
    auto start_time = Clock::now();
    if (concurrent_num <= 1)
    {
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

    // Check if any region is invalid, TiDB / TiSpark should refresh region cache and retry.
    if (region_status != RegionException::RegionReadStatus::OK)
        throwRetryRegion(regions_info, static_cast<RegionException::RegionReadStatus>(region_status.load()));

    auto end_time = Clock::now();
    LOG_DEBUG(log,
        "[Learner Read] wait index cost " << std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count()
                                          << " ms, regions_num=" << num_regions << ", concurrency=" << concurrent_num);

    return regions_snapshot;
}

/// Ensure regions' info after read.
void validateQueryInfo(
    const MvccQueryInfo & mvcc_query_info, const LearnerReadSnapshot & regions_snapshot, TMTContext & tmt, Poco::Logger * log)
{
    std::vector<RegionID> fail_region_ids;
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
            status = RegionException::RegionReadStatus::VERSION_ERROR;
        }

        if (status != RegionException::RegionReadStatus::OK)
        {
            fail_region_ids.emplace_back(region_query_info.region_id);
            fail_status = status;
            LOG_WARNING(log,
                "Check after read from Storage, region "
                    << region_query_info.region_id << ", version " << region_query_info.version //
                    << ", handle range ["
                    << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<true>(*region_query_info.range_in_table.first) << ", "
                    << RecordKVFormat::DecodedTiKVKeyToReadableHandleString<false>(*region_query_info.range_in_table.second) << "), status "
                    << RegionException::RegionReadStatusString(status));
        }
    }

    if (!fail_region_ids.empty())
    {
        throw RegionException(std::move(fail_region_ids), fail_status);
    }
}

} // namespace DB
