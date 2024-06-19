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

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/TiFlashMetrics.h>
#include <Interpreters/Context.h>
#include <Poco/Message.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Read/ReadIndexWorkerImpl.h>
#include <Storages/KVStore/Region.h>
#include <Storages/KVStore/TMTContext.h>
#include <common/logger_useful.h>
#include <fiu.h>

namespace ProfileEvents
{
extern const Event RaftWaitIndexTimeout;
} // namespace ProfileEvents

namespace DB
{
namespace FailPoints
{
extern const char force_wait_index_timeout[];
} // namespace FailPoints

kvrpcpb::ReadIndexRequest GenRegionReadIndexReq(const Region & region, UInt64 start_ts)
{
    auto meta_snap = region.dumpRegionMetaSnapshot();
    kvrpcpb::ReadIndexRequest request;
    {
        auto * context = request.mutable_context();
        context->set_region_id(region.id());
        *context->mutable_peer() = meta_snap.peer;
        context->mutable_region_epoch()->set_version(meta_snap.ver);
        context->mutable_region_epoch()->set_conf_ver(meta_snap.conf_ver);
        // if start_ts is 0, only send read index request to proxy
        if (start_ts)
        {
            request.set_start_ts(start_ts);
            auto * key_range = request.add_ranges();
            // use original tikv key
            key_range->set_start_key(meta_snap.range->comparableKeys().first.key);
            key_range->set_end_key(meta_snap.range->comparableKeys().second.key);
        }
    }
    return request;
}

bool Region::checkIndex(UInt64 index) const
{
    return meta.checkIndex(index);
}

std::tuple<WaitIndexStatus, double> Region::waitIndex(
    UInt64 index,
    const UInt64 timeout_ms,
    std::function<bool(void)> && check_running,
    const LoggerPtr & log)
{
    fiu_return_on(FailPoints::force_wait_index_timeout, std::make_tuple(WaitIndexStatus::Timeout, 1.0));
    if unlikely (proxy_helper == nullptr) // just for debug
        return {WaitIndexStatus::Finished, 0};

    if (meta.checkIndex(index))
    {
        // already satisfied
        return {WaitIndexStatus::Finished, 0};
    }

    Stopwatch wait_index_watch;
    const auto wait_idx_res = meta.waitIndex(index, timeout_ms, std::move(check_running));
    const auto elapsed_secs = wait_index_watch.elapsedSeconds();
    const auto & status = wait_idx_res.status;
    switch (status)
    {
    case WaitIndexStatus::Finished:
    {
        const auto log_lvl = elapsed_secs < 1.0 ? Poco::Message::PRIO_DEBUG : Poco::Message::PRIO_INFORMATION;
        LOG_IMPL(
            log,
            log_lvl,
            "{} wait learner index done, prev_index={} curr_index={} to_wait={} elapsed_s={:.3f} timeout_s={:.3f}",
            toString(false),
            wait_idx_res.prev_index,
            wait_idx_res.current_index,
            index,
            elapsed_secs,
            timeout_ms / 1000.0);
        return {status, elapsed_secs};
    }
    case WaitIndexStatus::Terminated:
    {
        return {status, elapsed_secs};
    }
    case WaitIndexStatus::Timeout:
    {
        ProfileEvents::increment(ProfileEvents::RaftWaitIndexTimeout);
        LOG_WARNING(
            log,
            "{} wait learner index timeout, prev_index={} curr_index={} to_wait={} state={}"
            " elapsed_s={:.3f} timeout_s={:.3f}",
            toString(false),
            wait_idx_res.prev_index,
            wait_idx_res.current_index,
            index,
            fmt::underlying(peerState()),
            elapsed_secs,
            timeout_ms / 1000.0);
        return {status, elapsed_secs};
    }
    }
}

void WaitCheckRegionReadyImpl(
    const TMTContext & tmt,
    KVStore & kvstore,
    const std::atomic_size_t & terminate_signals_counter,
    double wait_tick_time,
    double max_wait_tick_time,
    double get_wait_region_ready_timeout_sec)
{
    // part of time for waiting shall be assigned to batch-read-index
    static constexpr double BATCH_READ_INDEX_TIME_RATE = 0.2;
    auto log = Logger::get(__FUNCTION__);

    LOG_INFO(
        log,
        "start to check regions ready, min-wait-tick {}s, max-wait-tick {}s, wait-region-ready-timeout {:.3f}s",
        wait_tick_time,
        max_wait_tick_time,
        get_wait_region_ready_timeout_sec);

    std::unordered_set<RegionID> remain_regions;
    std::unordered_map<RegionID, uint64_t> regions_to_check;
    Stopwatch region_check_watch;
    size_t total_regions_cnt = 0;
    {
        kvstore.traverseRegions(
            [&remain_regions](RegionID region_id, const RegionPtr &) { remain_regions.emplace(region_id); });
        total_regions_cnt = remain_regions.size();
    }
    while (region_check_watch.elapsedSeconds() < get_wait_region_ready_timeout_sec * BATCH_READ_INDEX_TIME_RATE
           && terminate_signals_counter.load(std::memory_order_relaxed) == 0)
    {
        // Generate the read index requests
        std::vector<kvrpcpb::ReadIndexRequest> batch_read_index_req;
        for (auto it = remain_regions.begin(); it != remain_regions.end(); /**/)
        {
            auto region_id = *it;
            if (auto region = kvstore.getRegion(region_id); region)
            {
                batch_read_index_req.emplace_back(GenRegionReadIndexReq(*region));
                it++;
            }
            else
            {
                // Remove the region that is not exist now
                it = remain_regions.erase(it);
            }
        }

        // Record the latest commit index in TiKV
        auto read_index_res = kvstore.batchReadIndex(batch_read_index_req, tmt.batchReadIndexTimeout());
        for (auto && [resp, region_id] : read_index_res)
        {
            bool need_retry = resp.read_index() == 0;
            if (resp.has_region_error())
            {
                const auto & region_error = resp.region_error();
                if (region_error.has_region_not_found() || region_error.has_epoch_not_match())
                    need_retry = false;
                LOG_DEBUG(
                    log,
                    "neglect error, region_id={} not_found={} epoch_not_match={}",
                    region_id,
                    region_error.has_region_not_found(),
                    region_error.has_epoch_not_match());
            }
            if (!need_retry)
            {
                // `read_index` can be zero if region error happens.
                // It is not worthy waiting applying and reading index again.
                // if region is able to get latest commit-index from TiKV, we should make it available only after it has caught up.
                regions_to_check.emplace(region_id, resp.read_index());
                remain_regions.erase(region_id);
            }
            else
            {
                // retry in next round
            }
        }
        if (remain_regions.empty())
            break;

        LOG_INFO(
            log,
            "{} regions need to fetch latest commit-index in next round, sleep for {:.3f}s",
            remain_regions.size(),
            wait_tick_time);
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<Int64>(wait_tick_time * 1000)));
        wait_tick_time = std::min(max_wait_tick_time, wait_tick_time * 2);
    }

    if (!remain_regions.empty())
    {
        // timeout for fetching latest commit index from TiKV happen
        FmtBuffer buffer;
        buffer.joinStr(
            remain_regions.begin(),
            remain_regions.end(),
            [&](const auto & e, FmtBuffer & b) { b.fmtAppend("{}", e); },
            " ");
        LOG_WARNING(
            log,
            "{} regions CANNOT fetch latest commit-index from TiKV, (region-id): {}",
            remain_regions.size(),
            buffer.toString());
    }

    // Wait untill all region has catch up with TiKV or timeout happen
    do
    {
        for (auto it = regions_to_check.begin(); it != regions_to_check.end(); /**/)
        {
            auto [region_id, latest_index] = *it;
            if (auto region = kvstore.getRegion(region_id); region)
            {
                if (region->appliedIndex() >= latest_index)
                {
                    // The region has already catch up
                    it = regions_to_check.erase(it);
                }
                else
                {
                    ++it;
                }
            }
            else
            {
                // The region is removed from this instance
                it = regions_to_check.erase(it);
            }
        }

        if (regions_to_check.empty())
            break;

        LOG_INFO(
            log,
            "{} regions need to apply to latest index, sleep for {:.3f}s",
            regions_to_check.size(),
            wait_tick_time);
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<Int64>(wait_tick_time * 1000)));
        wait_tick_time = std::min(max_wait_tick_time, wait_tick_time * 2);
    } while (region_check_watch.elapsedSeconds() < get_wait_region_ready_timeout_sec
             && terminate_signals_counter.load(std::memory_order_relaxed) == 0);

    if (!regions_to_check.empty())
    {
        FmtBuffer buffer;
        buffer.joinStr(
            regions_to_check.begin(),
            regions_to_check.end(),
            [&](const auto & e, FmtBuffer & b) {
                if (auto r = kvstore.getRegion(e.first); r)
                {
                    b.fmtAppend("{},{},{}", e.first, e.second, r->appliedIndex());
                }
                else
                {
                    // The region is removed from this instance during waiting latest index
                    b.fmtAppend("{},{},none", e.first, e.second);
                }
            },
            " ");
        LOG_WARNING(
            log,
            "{} regions CANNOT catch up with latest index, (region-id,latest-index,apply-index): {}",
            regions_to_check.size(),
            buffer.toString());
    }

    const auto total_elapse = region_check_watch.elapsedSeconds();
    const auto log_level = total_elapse > 60.0 ? Poco::Message::PRIO_WARNING : Poco::Message::PRIO_INFORMATION;
    LOG_IMPL(log, log_level, "finish to check {} regions, time cost {:.3f}s", total_regions_cnt, total_elapse);
}

void WaitCheckRegionReady(
    const TMTContext & tmt,
    KVStore & kvstore,
    const std::atomic_size_t & terminate_signals_counter)
{
    // wait interval to check region ready, not recommended to modify only if for tesing
    auto wait_region_ready_tick = tmt.getContext().getConfigRef().getUInt64("flash.wait_region_ready_tick", 0);
    auto wait_region_ready_timeout_sec = static_cast<double>(tmt.waitRegionReadyTimeout());
    const double max_wait_tick_time = 0 == wait_region_ready_tick ? 20.0 : wait_region_ready_timeout_sec;
    double min_wait_tick_time = 0 == wait_region_ready_tick
        ? 2.5
        : static_cast<double>(wait_region_ready_tick); // default tick in TiKV is about 2s (without hibernate-region)
    return WaitCheckRegionReadyImpl(
        tmt,
        kvstore,
        terminate_signals_counter,
        min_wait_tick_time,
        max_wait_tick_time,
        wait_region_ready_timeout_sec);
}


BatchReadIndexRes KVStore::batchReadIndex(const std::vector<kvrpcpb::ReadIndexRequest> & reqs, uint64_t timeout_ms)
    const
{
    assert(this->proxy_helper);
    if (read_index_worker_manager)
    {
        return this->read_index_worker_manager->batchReadIndex(reqs, timeout_ms);
    }
    else
    {
        return proxy_helper->batchReadIndex_v1(reqs, timeout_ms);
    }
}

void KVStore::initReadIndexWorkers(
    ReadIndexWorkerManager::FnGetTickTime && fn_min_dur_handle_region,
    size_t runner_cnt,
    size_t worker_coefficient)
{
    if (!runner_cnt)
    {
        LOG_WARNING(log, "Run without read-index workers");
        return;
    }
    auto worker_cnt = worker_coefficient * runner_cnt;
    LOG_INFO(log, "Start to initialize read-index workers: worker count {}, runner count {}", worker_cnt, runner_cnt);
    auto * ptr = ReadIndexWorkerManager::newReadIndexWorkerManager(
                     *proxy_helper,
                     *this,
                     worker_cnt,
                     std::move(fn_min_dur_handle_region),
                     runner_cnt)
                     .release();
    std::atomic_thread_fence(std::memory_order_seq_cst);
    read_index_worker_manager = ptr;
}

void KVStore::asyncRunReadIndexWorkers() const
{
    if (!read_index_worker_manager)
        return;

    assert(this->proxy_helper);
    read_index_worker_manager->asyncRun();
}

void KVStore::stopReadIndexWorkers() const
{
    if (!read_index_worker_manager)
        return;

    assert(this->proxy_helper);
    read_index_worker_manager->stop();
}

void KVStore::releaseReadIndexWorkers()
{
    LOG_INFO(log, "KVStore shutdown, deleting read index worker");
    if (read_index_worker_manager)
    {
        delete read_index_worker_manager;
        read_index_worker_manager = nullptr;
    }
}

} // namespace DB
