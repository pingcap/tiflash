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

#include <Common/TiFlashMetrics.h>
#include <Poco/Message.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Read/LearnerReadWorker.h>
#include <Storages/KVStore/Read/LockException.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/KVStore/Types.h>
#include <Storages/RegionQueryInfo.h>
#include <common/likely.h>

namespace DB
{

void UnavailableRegions::tryThrowRegionException()
{
    // For batch-cop request (not handled by disagg write node), all unavailable regions, include the ones with lock exception, should be collected and retry next round.
    // For normal cop request, which only contains one region, LockException should be thrown directly and let upper layer (like client-c, tidb, tispark) handle it.
    // For batch-cop request (handled by disagg write node), LockException should be thrown directly and let upper layer (disagg read node) handle it.

    if (is_wn_disagg_read && !region_locks.empty())
        throw LockException(std::move(region_locks));

    if (!batch_cop && !region_locks.empty())
        throw LockException(std::move(region_locks));

    if (!ids.empty())
        throw RegionException(std::move(ids), status, extra_msg.c_str());
}

void UnavailableRegions::addRegionWaitIndexTimeout(
    const RegionID region_id,
    UInt64 index_to_wait,
    UInt64 current_applied_index)
{
    if (!batch_cop)
    {
        // If server is being terminated / time-out, add the region_id into `unavailable_regions` to other store.
        addStatus(region_id, RegionException::RegionReadStatus::NOT_FOUND, "");
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

LearnerReadWorker::LearnerReadWorker(
    MvccQueryInfo & mvcc_query_info_,
    TMTContext & tmt_,
    bool for_batch_cop,
    bool is_wn_disagg_read,
    const LoggerPtr & log_)
    : mvcc_query_info(mvcc_query_info_)
    , tmt(tmt_)
    , kvstore(tmt.getKVStore())
    , log(log_)
    , unavailable_regions(for_batch_cop, is_wn_disagg_read)
{
    assert(log != nullptr);
    stats.num_regions = mvcc_query_info.regions_query_info.size();
}

LearnerReadSnapshot LearnerReadWorker::buildRegionsSnapshot()
{
    LearnerReadSnapshot regions_snapshot;
    // check region is not null and store region map.
    const auto & regions_info = mvcc_query_info.regions_query_info;
    for (const auto & info : regions_info)
    {
        auto region = kvstore->getRegion(info.region_id);
        if (region == nullptr)
        {
            LOG_WARNING(log, "region not found in KVStore, region_id={}", info.region_id);
            throw RegionException({info.region_id}, RegionException::RegionReadStatus::NOT_FOUND, nullptr);
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
    return regions_snapshot;
}

std::vector<kvrpcpb::ReadIndexRequest> LearnerReadWorker::buildBatchReadIndexReq(
    const RegionTable & region_table,
    const LearnerReadSnapshot & regions_snapshot,
    RegionsReadIndexResult & batch_read_index_result)
{
    const auto & regions_info = mvcc_query_info.regions_query_info;
    std::vector<kvrpcpb::ReadIndexRequest> batch_read_index_req;
    batch_read_index_req.reserve(regions_info.size());

    // If using `std::numeric_limits<uint64_t>::max()`, set `start-ts` 0 to get the latest index but let read-index-worker do not record as history.
    auto read_index_tso
        = mvcc_query_info.read_tso == std::numeric_limits<uint64_t>::max() ? 0 : mvcc_query_info.read_tso;
    for (const auto & region_to_query : regions_info)
    {
        const RegionID region_id = region_to_query.region_id;
        // don't stale read in test scenarios.
        bool can_stale_read = mvcc_query_info.read_tso != std::numeric_limits<uint64_t>::max()
            && read_index_tso <= region_table.getSelfSafeTS(region_id);
        if (can_stale_read)
        {
            batch_read_index_result.emplace(region_id, kvrpcpb::ReadIndexResponse());
            ++stats.num_stale_read;
            continue;
        }

        if (auto ori_read_index = mvcc_query_info.getReadIndexRes(region_id); ori_read_index)
        {
            GET_METRIC(tiflash_raft_read_index_events_count, type_use_cache).Increment();
            // the read index result from cache
            auto resp = kvrpcpb::ReadIndexResponse();
            resp.set_read_index(ori_read_index);
            batch_read_index_result.emplace(region_id, std::move(resp));
            ++stats.num_cached_read_index;
        }
        else
        {
            // generate request for read index
            const auto & region = regions_snapshot.find(region_id)->second;
            batch_read_index_req.emplace_back(GenRegionReadIndexReq(*region, read_index_tso));
            ++stats.num_read_index_request;
        }
    }
    assert(stats.num_regions == stats.num_stale_read + stats.num_cached_read_index + stats.num_read_index_request);
    return batch_read_index_req;
}

void LearnerReadWorker::doBatchReadIndex(
    const std::vector<kvrpcpb::ReadIndexRequest> & batch_read_index_req,
    const UInt64 timeout_ms,
    RegionsReadIndexResult & batch_read_index_result)
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
    kvstore->addReadIndexEvent(1);
    SCOPE_EXIT({ kvstore->addReadIndexEvent(-1); });
    if (!tmt.checkRunning())
    {
        make_default_batch_read_index_result(true);
        return;
    }

    if (!kvstore->getProxyHelper())
    {
        // Only in mock test, `proxy_helper` will be `nullptr`. Set `read_index` to 0 and skip waiting.
        make_default_batch_read_index_result(false);
        return;
    }

    /// Blocking learner read. Note that learner read must be performed ahead of data read,
    /// otherwise the desired index will be blocked by the lock of data read.
    auto res = kvstore->batchReadIndex(batch_read_index_req, timeout_ms);
    for (auto && [resp, region_id] : res)
    {
        batch_read_index_result.emplace(region_id, std::move(resp));
    }
}

void LearnerReadWorker::recordReadIndexError(
    const LearnerReadSnapshot & regions_snapshot,
    RegionsReadIndexResult & read_index_result)
{
    // if size of batch_read_index_result is not equal with batch_read_index_req, there must be region_error/lock, find and return directly.
    for (auto & [region_id, resp] : read_index_result)
    {
        std::string extra_msg;
        if (resp.has_region_error())
        {
            const auto & region_error = resp.region_error();
            auto region_status = RegionException::RegionReadStatus::OTHER;
            if (region_error.has_epoch_not_match())
            {
                // 1. From TiKV
                // 2. Find a TiKV mem lock of start_ts, and retry all other ts in the batch
                auto snapshot_region_iter = regions_snapshot.find(region_id);
                if (snapshot_region_iter != regions_snapshot.end())
                {
                    extra_msg = fmt::format(
                        "read_index_resp error, region_id={} version={} conf_version={}",
                        region_id,
                        snapshot_region_iter->second.create_time_version,
                        snapshot_region_iter->second.create_time_conf_ver);
                }
                else
                {
                    extra_msg = fmt::format("read_index_resp error, region_id={} not found in snapshot", region_id);
                }
                GET_METRIC(tiflash_raft_learner_read_failures_count, type_epoch_not_match).Increment();
                region_status = RegionException::RegionReadStatus::EPOCH_NOT_MATCH;
            }
            else if (region_error.has_not_leader())
            {
                GET_METRIC(tiflash_raft_learner_read_failures_count, type_not_leader).Increment();
                region_status = RegionException::RegionReadStatus::NOT_LEADER;
            }
            else if (region_error.has_region_not_found())
            {
                // 1. From TiKV
                // 2. Can't send read index request
                // 3. Read index timeout
                GET_METRIC(tiflash_raft_learner_read_failures_count, type_not_found_tikv).Increment();
                region_status = RegionException::RegionReadStatus::NOT_FOUND_TIKV;
            }
            // Below errors seldomly happens in raftstore-v1, however, we are not sure if they will happen in v2.
            else if (region_error.has_flashbackinprogress() || region_error.has_flashbacknotprepared())
            {
                GET_METRIC(tiflash_raft_learner_read_failures_count, type_flashback).Increment();
                region_status = RegionException::RegionReadStatus::FLASHBACK;
            }
            else if (region_error.has_bucket_version_not_match())
            {
                GET_METRIC(tiflash_raft_learner_read_failures_count, type_bucket_epoch_not_match).Increment();
                LOG_DEBUG(
                    log,
                    "meet abnormal region error {}, [region_id={}]",
                    resp.region_error().DebugString(),
                    region_id);
                region_status = RegionException::RegionReadStatus::BUCKET_EPOCH_NOT_MATCH;
            }
            else if (region_error.has_key_not_in_region())
            {
                GET_METRIC(tiflash_raft_learner_read_failures_count, type_key_not_in_region).Increment();
                LOG_DEBUG(
                    log,
                    "meet abnormal region error {}, [region_id={}]",
                    resp.region_error().DebugString(),
                    region_id);
                region_status = RegionException::RegionReadStatus::KEY_NOT_IN_REGION;
            }
            else if (region_error.has_server_is_busy())
            {
                // 1. From TiKV
                // 2. Read index request timeout
                GET_METRIC(tiflash_raft_learner_read_failures_count, type_read_index_timeout).Increment();
                LOG_DEBUG(log, "meet abnormal region error {}, [region_id={}]", resp.ShortDebugString(), region_id);
                region_status = RegionException::RegionReadStatus::READ_INDEX_TIMEOUT;
            }
            else if (
                region_error.has_raft_entry_too_large() || region_error.has_region_not_initialized()
                || region_error.has_disk_full() || region_error.has_read_index_not_ready()
                || region_error.has_proposal_in_merging_mode())
            {
                GET_METRIC(tiflash_raft_learner_read_failures_count, type_tikv_server_issue).Increment();
                LOG_DEBUG(log, "meet abnormal region error {}, [region_id={}]", resp.ShortDebugString(), region_id);
                region_status = RegionException::RegionReadStatus::TIKV_SERVER_ISSUE;
            }
            else
            {
                GET_METRIC(tiflash_raft_learner_read_failures_count, type_other).Increment();
                LOG_DEBUG(log, "meet abnormal region error {}, [region_id={}]", resp.ShortDebugString(), region_id);
            }
            unavailable_regions.addStatus(region_id, region_status, std::move(extra_msg));
        }
        else if (resp.has_locked())
        {
            GET_METRIC(tiflash_raft_learner_read_failures_count, type_tikv_lock).Increment();
            unavailable_regions.addRegionLock(region_id, LockInfoPtr(resp.release_locked()));
        }
        else
        {
            // cache read-index to avoid useless overhead about retry.
            // resp.read_index() is 0 when stale read, skip it to avoid overwriting read_index res from the last retry.
            if (resp.read_index() != 0)
            {
                mvcc_query_info.addReadIndexResToCache(region_id, resp.read_index());
            }
        }
    }
}

RegionsReadIndexResult LearnerReadWorker::readIndex(
    const LearnerReadSnapshot & regions_snapshot,
    UInt64 timeout_ms,
    Stopwatch & watch)
{
    RegionsReadIndexResult batch_read_index_result;
    const auto batch_read_index_req
        = buildBatchReadIndexReq(tmt.getRegionTable(), regions_snapshot, batch_read_index_result);

    GET_METRIC(tiflash_stale_read_count).Increment(stats.num_stale_read);
    GET_METRIC(tiflash_raft_read_index_count).Increment(batch_read_index_req.size());

    doBatchReadIndex(batch_read_index_req, timeout_ms, batch_read_index_result);

    stats.read_index_elapsed_ms = watch.elapsedMilliseconds();
    GET_METRIC(tiflash_raft_read_index_duration_seconds).Observe(stats.read_index_elapsed_ms / 1000.0);
    recordReadIndexError(regions_snapshot, batch_read_index_result);

    const auto log_lvl = unavailable_regions.empty() ? Poco::Message::PRIO_DEBUG : Poco::Message::PRIO_INFORMATION;
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

    return batch_read_index_result;
}

void LearnerReadWorker::waitIndex(
    const LearnerReadSnapshot & regions_snapshot,
    RegionsReadIndexResult & batch_read_index_result,
    const UInt64 timeout_ms,
    Stopwatch & watch)
{
    const auto & regions_info = mvcc_query_info.regions_query_info;
    for (const auto & region_to_query : regions_info)
    {
        // if region is unavailable, skip wait index.
        if (unavailable_regions.contains(region_to_query.region_id))
            continue;

        const auto & region = regions_snapshot.find(region_to_query.region_id)->second;

        const auto total_wait_index_elapsed_ms = watch.elapsedMilliseconds();
        const auto index_to_wait = batch_read_index_result.find(region_to_query.region_id)->second.read_index();
        if (timeout_ms != 0 && total_wait_index_elapsed_ms > timeout_ms)
        {
            // Wait index timeout is enabled && timeout happens, simply check all Regions' applied index
            // instead of wait index for Regions one by one.
            if (!region->checkIndex(index_to_wait))
            {
                auto current = region->appliedIndex();
                unavailable_regions.addRegionWaitIndexTimeout(region_to_query.region_id, index_to_wait, current);
            }
            continue; // timeout happens, check next region quickly
        }

        // Wait index timeout is disabled; or timeout is enabled but not happen yet, wait index for
        // a specify Region.
        const auto [wait_res, time_cost] = region->waitIndex(
            index_to_wait,
            timeout_ms,
            [this]() { return tmt.checkRunning(); },
            log);
        if (wait_res != WaitIndexStatus::Finished)
        {
            auto current = region->appliedIndex();
            unavailable_regions.addRegionWaitIndexTimeout(region_to_query.region_id, index_to_wait, current);
            continue; // error or timeout happens, check next region quickly
        }

        if (time_cost > 0)
        {
            // Only record information if wait-index does happen
            GET_METRIC(tiflash_raft_wait_index_duration_seconds).Observe(time_cost);
        }

        if (unlikely(!mvcc_query_info.resolve_locks))
        {
            continue;
        }

        // Try to resolve locks and flush data into storage layer
        const auto & physical_table_id = region_to_query.physical_table_id;
        auto res = RegionTable::resolveLocksAndWriteRegion(
            tmt,
            physical_table_id,
            region,
            mvcc_query_info.read_tso,
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
                        unavailable_regions.addStatus(region->id(), status, "resolveLock");
                    }
                },
            },
            res);
    } // wait index for next region

    stats.wait_index_elapsed_ms = watch.elapsedMilliseconds();
    const auto log_lvl = unavailable_regions.empty() ? Poco::Message::PRIO_DEBUG : Poco::Message::PRIO_INFORMATION;
    LOG_IMPL(
        log,
        log_lvl,
        "[Learner Read] Finish wait index and resolve locks, wait_cost={}ms n_regions={} n_unavailable={}",
        stats.wait_index_elapsed_ms,
        stats.num_regions,
        unavailable_regions.size());

    auto bypass_formatter = [&](const RegionQueryInfo & query_info) -> String {
        if (query_info.bypass_lock_ts == nullptr)
            return "";
        FmtBuffer buffer;
        buffer.joinStr(
            query_info.bypass_lock_ts->begin(),
            query_info.bypass_lock_ts->end(),
            [](const auto & v, FmtBuffer & f) { f.fmtAppend("{}", v); },
            "|");
        return buffer.toString();
    };
    auto region_info_formatter = [&]() -> String {
        FmtBuffer buffer;
        buffer.joinStr(
            regions_info.begin(),
            regions_info.end(),
            [&](const auto & region_to_query, FmtBuffer & f) {
                const auto & region = regions_snapshot.find(region_to_query.region_id)->second;
                f.fmtAppend(
                    "id:{} applied_index:{} bypass_locks:{});",
                    region_to_query.region_id,
                    region->appliedIndex(),
                    bypass_formatter(region_to_query));
            },
            ";");
        return buffer.toString();
    };

    LOG_DEBUG(
        log,
        "[Learner Read] Learner Read Summary, regions_info={}, unavailable_regions_info={}",
        region_info_formatter(),
        unavailable_regions.toDebugString());
}

std::tuple<Clock::time_point, Clock::time_point> //
LearnerReadWorker::waitUntilDataAvailable(
    const LearnerReadSnapshot & regions_snapshot,
    UInt64 read_index_timeout_ms,
    UInt64 wait_index_timeout_ms)
{
    const auto start_time = Clock::now();

    Stopwatch watch;
    RegionsReadIndexResult batch_read_index_result = readIndex(regions_snapshot, read_index_timeout_ms, watch);
    watch.restart(); // restart to count the elapsed of wait index
    waitIndex(regions_snapshot, batch_read_index_result, wait_index_timeout_ms, watch);

    const auto end_time = Clock::now();
    const auto time_elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    GET_METRIC(tiflash_syncing_data_freshness).Observe(time_elapsed_ms / 1000.0); // For DBaaS SLI

    // TODO should we try throw immediately after readIndex?
    // Throw Region exception if there are any unavailable regions, the exception will be handled in the
    // following methods
    // - `CoprocessorHandler::execute`
    // - `FlashService::EstablishDisaggTask`
    // - `DAGDriver::execute`
    // - `DAGStorageInterpreter::doBatchCopLearnerRead`
    // - `DAGStorageInterpreter::buildLocalStreamsForPhysicalTable`
    // - `DAGStorageInterpreter::buildLocalExecForPhysicalTable`
    unavailable_regions.tryThrowRegionException();

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
    return {start_time, end_time};
}

} // namespace DB
