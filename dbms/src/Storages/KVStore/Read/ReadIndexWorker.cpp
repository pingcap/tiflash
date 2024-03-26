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

#include <Storages/KVStore/Read/ReadIndexWorkerImpl.h>

namespace DB
{

BatchReadIndexRes TiFlashRaftProxyHelper::batchReadIndex(
    const std::vector<kvrpcpb::ReadIndexRequest> & req,
    uint64_t timeout_ms) const
{
    return batchReadIndex_v2(req, timeout_ms);
}

BatchReadIndexRes TiFlashRaftProxyHelper::batchReadIndex_v2(
    const std::vector<kvrpcpb::ReadIndexRequest> & req,
    uint64_t timeout_ms) const
{
    AsyncWaker waker(*this);
    BlockedReadIndexHelper helper{timeout_ms, waker};

    std::queue<std::pair<RegionID, ReadIndexTask>> tasks;
    BatchReadIndexRes resps;
    resps.reserve(req.size());

    for (const auto & r : req)
    {
        if (auto task = makeReadIndexTask(r); !task)
        {
            // The read index request is not sent successfully.
            GET_METRIC(tiflash_raft_learner_read_failures_count, type_request_error).Increment();
            kvrpcpb::ReadIndexResponse res;
            res.mutable_region_error();
            resps.emplace_back(std::move(res), r.context().region_id());
        }
        else
        {
            tasks.emplace(r.context().region_id(), std::move(*task));
        }
    }

    {
        // Block wait for all tasks are ready or timeout.
        kvrpcpb::ReadIndexResponse tmp;
        while (!tasks.empty())
        {
            auto & it = tasks.front();
            if (pollReadIndexTask(it.second, tmp, helper.getWaker().getRaw()))
            {
                resps.emplace_back(std::move(tmp), it.first);
                tmp.Clear();
                tasks.pop();
            }
            else
            {
                if (helper.blockedWait() == AsyncNotifier::Status::Timeout)
                    break;
            }
        }
    }
    {
        // If meets timeout, which means some of the regions can not get response from leader, try to poll rest tasks
        while (!tasks.empty())
        {
            auto & it = tasks.front();
            kvrpcpb::ReadIndexResponse tmp;
            if (pollReadIndexTask(it.second, tmp))
            {
                resps.emplace_back(std::move(tmp), it.first);
            }
            else
            {
                tmp.mutable_region_error()->mutable_region_not_found();
                resps.emplace_back(std::move(tmp), it.first);
            }
            tasks.pop();
        }
    }

    return resps;
}

RawRustPtr TiFlashRaftProxyHelper::makeAsyncWaker(void (*wake_fn)(RawVoidPtr), RawCppPtr data) const
{
    return fn_make_async_waker(wake_fn, data);
}

std::optional<ReadIndexTask> TiFlashRaftProxyHelper::makeReadIndexTask(const kvrpcpb::ReadIndexRequest & req) const
{
    thread_local std::string buff_cache;
    req.SerializeToString(&buff_cache);
    auto req_view = strIntoView(&buff_cache);
    if (RawRustPtr ptr = fn_make_read_index_task(proxy_ptr, req_view); ptr.ptr)
    {
        return ReadIndexTask{ptr};
    }
    else
    {
        return {};
    }
}

bool TiFlashRaftProxyHelper::pollReadIndexTask(
    ReadIndexTask & task,
    kvrpcpb::ReadIndexResponse & resp,
    RawVoidPtr waker) const
{
    return fn_poll_read_index_task(proxy_ptr, task.ptr, &resp, waker);
}

TimerTask TiFlashRaftProxyHelper::makeTimerTask(uint64_t time_ms) const
{
    return TimerTask{fn_make_timer_task(time_ms)};
}

bool TiFlashRaftProxyHelper::pollTimerTask(TimerTask & task, RawVoidPtr waker) const
{
    return fn_poll_timer_task(task.ptr, waker);
}


std::atomic<std::chrono::milliseconds> ReadIndexWorker::max_read_index_task_timeout
    = std::chrono::milliseconds{8 * 1000};
//std::atomic<size_t> ReadIndexWorker::max_read_index_history{8};

bool RegionNotifyMap::empty() const NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return data.empty();
}
void RegionNotifyMap::add(RegionID id) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    data.emplace(id);
}
RegionNotifyMap::Data RegionNotifyMap::popAll() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return std::move(data);
}

void ReadIndexFuture::update(kvrpcpb::ReadIndexResponse resp) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    if (finished)
        return;

    TEST_LOG_FMT("set ReadIndexFuture resp for req {}, resp {}", req.ShortDebugString(), resp.ShortDebugString());

    finished = true;
    this->resp = std::move(resp);

    if (notifier)
    {
        TEST_LOG_FMT("wake notifier for region_id={}", req.context().region_id());
        notifier->wake();
    }
}

std::optional<kvrpcpb::ReadIndexResponse> ReadIndexFuture::poll(const std::shared_ptr<AsyncNotifier> & notifier_) const
    NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    if (!finished)
    {
        if (notifier_ != notifier)
        {
            TEST_LOG_FMT("set notifier for region_id={}", req.context().region_id());
            notifier = notifier_;
        }
        return {};
    }
    return resp;
}


ReadIndexDataNodePtr ReadIndexWorker::DataMap::upsertDataNode(RegionID region_id) const
{
    auto _ = genUniqueLock();

    TEST_LOG_FMT("upsertDataNode for region_id={}", region_id);

    auto [it, ok] = region_map.try_emplace(region_id);
    if (ok)
        it->second = std::make_shared<ReadIndexDataNode>(region_id);
    return it->second;
}

ReadIndexDataNodePtr ReadIndexWorker::DataMap::tryGetDataNode(RegionID region_id) const
{
    auto _ = genSharedLock();
    if (auto it = region_map.find(region_id); it != region_map.end())
    {
        return it->second;
    }
    return nullptr;
}

ReadIndexDataNodePtr ReadIndexWorker::DataMap::getDataNode(RegionID region_id) const
{
    if (auto ptr = tryGetDataNode(region_id); ptr != nullptr)
        return ptr;
    return upsertDataNode(region_id);
}

void ReadIndexWorker::DataMap::invoke(std::function<void(std::unordered_map<RegionID, ReadIndexDataNodePtr> &)> && cb)
{
    auto _ = genUniqueLock();
    cb(region_map);
}

void ReadIndexWorker::DataMap::removeRegion(RegionID region_id)
{
    auto _ = genUniqueLock();
    region_map.erase(region_id);
}

void ReadIndexWorker::consumeReadIndexNotifyCtrl()
{
    for (auto && [region_id, ts] : read_index_notify_ctrl->popAll())
    {
        auto node = data_map.getDataNode(region_id);
        TEST_LOG_FMT("consume region_id={}, ts {}", region_id, ts);
        node->consume(proxy_helper, ts);
    }
}

void ReadIndexWorker::consumeRegionNotifies(SteadyClock::duration min_dur)
{
    if (!lastRunTimeout(min_dur))
    {
        TEST_LOG_FMT("worker {} failed to check last run timeout {}", getID(), min_dur);
        return;
    }

    for (auto && region_id : region_notify_map.popAll())
    {
        auto node = data_map.getDataNode(region_id);
        node->runOneRound(proxy_helper, read_index_notify_ctrl);
    }

    TEST_LOG_FMT("worker {} set last run time {}", getID(), Clock::now());
    last_run_time.store(SteadyClock::now(), std::memory_order_release);
}

ReadIndexFuturePtr ReadIndexWorker::genReadIndexFuture(const kvrpcpb::ReadIndexRequest & req)
{
    auto data = data_map.getDataNode(req.context().region_id());
    auto res = data->insertTask(req);
    region_notify_map.add(req.context().region_id());
    return res;
}


void ReadIndexWorker::runOneRound(SteadyClock::duration min_dur)
{
    if (!read_index_notify_ctrl->empty())
    {
        consumeReadIndexNotifyCtrl();
    }
    if (!region_notify_map.empty())
    {
        consumeRegionNotifies(min_dur);
    }
}

ReadIndexWorker::ReadIndexWorker(
    const TiFlashRaftProxyHelper & proxy_helper_,
    KVStore & kvstore_,
    size_t id_,
    AsyncWaker::NotifierPtr notifier_)
    : proxy_helper(proxy_helper_)
    , kvstore(kvstore_)
    , id(id_)
    , read_index_notify_ctrl(std::make_shared<ReadIndexNotifyCtrl>(notifier_))
{}

bool ReadIndexWorker::lastRunTimeout(SteadyClock::duration timeout) const
{
    TEST_LOG_FMT(
        "worker {}, last run time {}, timeout {}",
        getID(),
        last_run_time.load(std::memory_order_relaxed).time_since_epoch().count(),
        std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count());
    return last_run_time.load(std::memory_order_relaxed) + timeout < SteadyClock::now();
}

void ReadIndexWorker::removeRegion(uint64_t region_id)
{
    TEST_LOG_FMT("remove region_id={}", region_id);
    data_map.removeRegion(region_id);
}


const uint64_t MockStressTestCfg::RegionIdPrefix = 1ull << 40;
bool MockStressTestCfg::enable = false;

} // namespace DB
