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

#include <Common/MemoryAllocTrace.h>
#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>
#include <Storages/KVStore/FFI/ProxyFFI.h>
#include <Storages/KVStore/KVStore.h>
#include <Storages/KVStore/Read/ReadIndexWorker.h>
#include <fmt/chrono.h>

#include <queue>


namespace DB
{
// #define ADD_TEST_DEBUG_LOG_FMT

#ifdef ADD_TEST_DEBUG_LOG_FMT

static Poco::Logger * global_logger_for_test = nullptr;
static std::mutex global_logger_mutex;

#define TEST_LOG_FMT(...) LOG_ERROR(global_logger_for_test, __VA_ARGS__)

void F_TEST_LOG_FMT(const std::string & s)
{
    auto _ = std::lock_guard(global_logger_mutex);
    std::cout << s << std::endl;
}
#else
#define TEST_LOG_FMT(...)
void F_TEST_LOG_FMT(const std::string &) {}
#endif
} // namespace DB

namespace DB
{
AsyncNotifier::Status AsyncWaker::Notifier::blockedWaitUtil(const SteadyClock::time_point & time_point)
{
    // if flag from false to false, wait for notification.
    // if flag from true to false, do nothing.
    auto res = AsyncNotifier::Status::Normal;
    if (!is_awake->exchange(false, std::memory_order_acq_rel))
    {
        {
            auto lock = genUniqueLock();
            if (!is_awake->load(std::memory_order_acquire))
            {
                if (condVar().wait_until(lock, time_point) == std::cv_status::timeout)
                    res = AsyncNotifier::Status::Timeout;
            }
        }
        is_awake->store(false, std::memory_order_release);
    }
    return res;
}

void AsyncWaker::Notifier::wake()
{
    // if flag from false -> true, then wake up.
    // if flag from true -> true, do nothing.
    if (is_awake->load(std::memory_order_acquire))
        return;
    if (!is_awake->exchange(true, std::memory_order_acq_rel))
    {
        // wake up notifier
        auto _ = genLockGuard();
        condVar().notify_one();
    }
}

void AsyncWaker::wake(RawVoidPtr notifier_)
{
    auto & notifier = *reinterpret_cast<AsyncNotifier *>(notifier_);
    notifier.wake();
}

AsyncWaker::AsyncWaker(const TiFlashRaftProxyHelper & helper_)
    : AsyncWaker(helper_, new AsyncWaker::Notifier{})
{}

AsyncWaker::AsyncWaker(const TiFlashRaftProxyHelper & helper_, AsyncNotifier * notifier_)
    : inner(helper_.makeAsyncWaker(AsyncWaker::wake, GenRawCppPtr(notifier_, RawCppPtrTypeImpl::WakerNotifier)))
    , notifier(*notifier_)
{}

AsyncNotifier::Status AsyncWaker::waitUtil(SteadyClock::time_point time_point)
{
    return notifier.blockedWaitUtil(time_point);
}

RawVoidPtr AsyncWaker::getRaw() const
{
    return inner.ptr;
}

struct BlockedReadIndexHelperTrait
{
    explicit BlockedReadIndexHelperTrait(uint64_t timeout_ms_)
        : time_point(SteadyClock::now() + std::chrono::milliseconds{timeout_ms_})
    {}
    virtual AsyncNotifier::Status blockedWaitUtil(SteadyClock::time_point) = 0;

    // block current runtime and wait.
    virtual AsyncNotifier::Status blockedWait()
    {
        // TODO: use async process if supported by framework
        return blockedWaitUtil(time_point);
    }
    virtual ~BlockedReadIndexHelperTrait() = default;

protected:
    SteadyClock::time_point time_point;
};

struct BlockedReadIndexHelper final : BlockedReadIndexHelperTrait
{
public:
    BlockedReadIndexHelper(uint64_t timeout_ms_, AsyncWaker & waker_)
        : BlockedReadIndexHelperTrait(timeout_ms_)
        , waker(waker_)
    {}

    const AsyncWaker & getWaker() const { return waker; }

    AsyncNotifier::Status blockedWaitUtil(SteadyClock::time_point time_point) override
    {
        return waker.waitUtil(time_point);
    }

    ~BlockedReadIndexHelper() override = default;

private:
    AsyncWaker & waker;
};

struct BlockedReadIndexHelperV3 final : BlockedReadIndexHelperTrait
{
    BlockedReadIndexHelperV3(uint64_t timeout_ms_, AsyncWaker::Notifier & notifier_)
        : BlockedReadIndexHelperTrait(timeout_ms_)
        , notifier(notifier_)
    {}

    AsyncNotifier::Status blockedWaitUtil(SteadyClock::time_point time_point) override
    {
        return notifier.blockedWaitUtil(time_point);
    }

    ~BlockedReadIndexHelperV3() override = default;

private:
    AsyncWaker::Notifier & notifier;
};

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
            kvrpcpb::ReadIndexResponse res;
            res.mutable_region_error();
            resps.emplace_back(std::move(res), r.context().region_id());
        }
        else
        {
            tasks.emplace(r.context().region_id(), std::move(*task));
        }
    }

    { // block wait for all tasks are ready or timeout.
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
    { // if meet timeout, which means part of regions can not get response from leader, try to poll rest tasks
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

struct ReadIndexNotifyCtrl : MutexLockWrap
{
    using Data = std::deque<std::pair<RegionID, Timestamp>>;

    bool empty() const
    {
        auto _ = genLockGuard();
        return data.empty();
    }
    void add(RegionID id, Timestamp ts)
    {
        auto _ = genLockGuard();
        data.emplace_back(id, ts);
    }
    Data popAll()
    {
        auto _ = genLockGuard();
        return std::move(data);
    }

    void wake() const { notifier->wake(); }

    explicit ReadIndexNotifyCtrl(AsyncWaker::NotifierPtr notifier_)
        : notifier(notifier_)
    {}

    Data data;
    AsyncWaker::NotifierPtr notifier;
};

struct RegionReadIndexNotifier final : AsyncNotifier
{
    void wake() override
    {
        notify->add(region_id, ts);
        notify->wake();
    }
    Status blockedWaitUtil(const SteadyClock::time_point &) override { return Status::Timeout; }

    ~RegionReadIndexNotifier() override = default;

    RegionReadIndexNotifier(RegionID region_id_, Timestamp ts_, const ReadIndexNotifyCtrlPtr & notify_)
        : region_id(region_id_)
        , ts(ts_)
        , notify(notify_)
    {}

    RegionID region_id;
    Timestamp ts;
    ReadIndexNotifyCtrlPtr notify;
};

std::atomic<std::chrono::milliseconds> ReadIndexWorker::max_read_index_task_timeout
    = std::chrono::milliseconds{8 * 1000};
//std::atomic<size_t> ReadIndexWorker::max_read_index_history{8};

void ReadIndexFuture::update(kvrpcpb::ReadIndexResponse resp)
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

void ReadIndexDataNode::ReadIndexElement::doTriggerCallbacks()
{
    TEST_LOG_FMT("start to triggerCallbacks for region_id={} callbacks size {}", region_id, callbacks.size());

    if (resp.has_locked())
    {
        for (auto && cb : callbacks)
        {
            kvrpcpb::ReadIndexResponse res{};
            if (cb->req.start_ts() == start_ts)
                res = resp;
            else
                res.mutable_region_error()->mutable_epoch_not_match();
            cb->update(std::move(res));
        }
    }
    else
    {
        for (auto && cb : callbacks)
        {
            cb->update(resp);
        }
    }
    callbacks.clear();
}

void ReadIndexDataNode::ReadIndexElement::doForceSet(const kvrpcpb::ReadIndexResponse & f)
{
    TEST_LOG_FMT("force set response {}, start-ts {}", f.ShortDebugString(), start_ts);

    task_pair.reset();
    resp = f;
    doTriggerCallbacks();
}

void ReadIndexDataNode::ReadIndexElement::doPoll(
    const TiFlashRaftProxyHelper & helper,
    std::chrono::milliseconds timeout)
{
    bool can_trigger = false;
    {
        if (task_pair)
        {
            auto && [task, waker] = *task_pair;

            auto * raw_waker = waker.getRaw();
            bool clean_task = false;
            if (helper.pollReadIndexTask(task, resp, raw_waker))
            {
                TEST_LOG_FMT(
                    "poll ReadIndexElement success for region_id={}, resp {}",
                    region_id,
                    resp.ShortDebugString());

                clean_task = true;
            }
            else if (SteadyClock::now() > timeout + start_time)
            {
                TEST_LOG_FMT("poll ReadIndexElement timeout for region_id={}", region_id);

                clean_task = true;
                resp.mutable_region_error()
                    ->mutable_server_is_busy(); // set region_error `server_is_busy` for task timeout
            }
            else
            {
                TEST_LOG_FMT(
                    "poll ReadIndexElement failed, region_id={} time_cost={} timeout={}",
                    region_id,
                    std::chrono::duration<double, std::milli>(SteadyClock::now() - start_time).count(),
                    timeout);
            }

            if (clean_task)
            {
                can_trigger = true;
                task_pair.reset();
            }
        }
        else
            can_trigger = true;
    }
    if (can_trigger)
    {
        doTriggerCallbacks();
    }
}

void ReadIndexDataNode::doAddHistoryTasks(Timestamp ts, kvrpcpb::ReadIndexResponse && resp)
{
    for (auto it = running_tasks.begin(); it != running_tasks.end();)
    {
        if (it->first <= ts)
        {
            it->second.doForceSet(resp); // copy resp
            it = running_tasks.erase(it);
        }
        else
        {
            break;
        }
    }
    {
        history_success_tasks.emplace(ts, std::move(resp)); // move resp
    }
}

void ReadIndexDataNode::doConsume(const TiFlashRaftProxyHelper & helper, RunningTasks::iterator it)
{
    it->second.doPoll(helper, ReadIndexWorker::getMaxReadIndexTaskTimeout());
    if (!it->second.task_pair)
    {
        auto start_ts = it->first;
        auto resp = std::move(it->second.resp);

        running_tasks.erase(it);

        // If `start ts` is NOT initialized or response has `region error` | `lock`, just ignore
        if (start_ts && !resp.has_locked() && !resp.has_region_error())
        {
            doAddHistoryTasks(start_ts, std::move(resp));
        }
    }
}

void ReadIndexDataNode::consume(const TiFlashRaftProxyHelper & helper, Timestamp ts)
{
    auto _ = genLockGuard();

    if (auto it = running_tasks.find(ts); it != running_tasks.end())
    {
        doConsume(helper, it);
    }
}

const uint64_t MockStressTestCfg::RegionIdPrefix = 1ull << 40;
bool MockStressTestCfg::enable = false;

std::optional<ReadIndexTask> makeReadIndexTask(const TiFlashRaftProxyHelper & helper, kvrpcpb::ReadIndexRequest & req)
{
    if (likely(!MockStressTestCfg::enable))
        return helper.makeReadIndexTask(req);
    else
    {
        auto ori_id = req.context().region_id();
        req.mutable_context()->set_region_id(
            ori_id % MockStressTestCfg::RegionIdPrefix); // set region id to original one.
        TEST_LOG_FMT("hacked ReadIndexTask to req {}", req.ShortDebugString());
        auto res = helper.makeReadIndexTask(req);
        req.mutable_context()->set_region_id(ori_id);
        return res;
    }
}

void ReadIndexDataNode::runOneRound(const TiFlashRaftProxyHelper & helper, const ReadIndexNotifyCtrlPtr & notify)
{
    auto opt_waiting_tasks = this->waiting_tasks.popAll();
    if (!opt_waiting_tasks)
        return;
    auto & waiting_tasks = *opt_waiting_tasks;

    auto _ = genLockGuard();

    {
        // Find the task with the maximum ts in all `waiting_tasks`.
        Timestamp max_ts = 0;
        ReadIndexFuturePtr max_ts_task = nullptr;
        {
            const ReadIndexFuturePtr * x = nullptr;
            for (auto & e : waiting_tasks)
            {
                if (e.first >= max_ts)
                {
                    max_ts = e.first;
                    x = &e.second;
                }
            }
            max_ts_task = *x; // NOLINT
        }

        auto region_id = max_ts_task->req.context().region_id();

        TEST_LOG_FMT(
            "try to use max_ts {}, from request for region_id={}, waiting_tasks size {}, running_tasks {}",
            max_ts,
            region_id,
            waiting_tasks.size(),
            running_tasks.size());

        // start-ts `0` will be used to only get the latest index, do not use history
        if (history_success_tasks && history_success_tasks->first >= max_ts && max_ts)
        {
            TEST_LOG_FMT("find history_tasks resp {}", history_success_tasks->second.ShortDebugString());

            for (const auto & e : waiting_tasks)
            {
                e.second->update(history_success_tasks->second);
            }

            cnt_use_history_tasks += waiting_tasks.size();
        }
        else
        {
            auto run_it = running_tasks.lower_bound(max_ts);
            if (run_it == running_tasks.end())
            {
                TEST_LOG_FMT("no exist running_tasks for ts {}", max_ts);

                if (auto t = makeReadIndexTask(helper, max_ts_task->req); t)
                {
                    TEST_LOG_FMT("successfully make ReadIndexTask for region_id={} ts {}", region_id, max_ts);
                    AsyncWaker waker{helper, new RegionReadIndexNotifier(region_id, max_ts, notify)};
                    run_it = running_tasks.try_emplace(max_ts, region_id, max_ts).first;
                    run_it->second.task_pair.emplace(std::move(*t), std::move(waker));
                }
                else
                {
                    TEST_LOG_FMT("failed to make ReadIndexTask for region_id={} ts {}", region_id, max_ts);
                    run_it = running_tasks.try_emplace(max_ts, region_id, max_ts).first;
                    run_it->second.resp.mutable_region_error();
                }
            }

            for (auto && e : waiting_tasks)
            {
                run_it->second.callbacks.emplace_back(std::move(e.second));
            }

            doConsume(helper, run_it);
        }
    }
}

ReadIndexDataNode::~ReadIndexDataNode()
{
    auto _ = genLockGuard();

    kvrpcpb::ReadIndexResponse resp;
    resp.mutable_region_error()->mutable_region_not_found();

    TEST_LOG_FMT(
        "~ReadIndexDataNode region_id={}: waiting_tasks {}, running_tasks {} ",
        region_id,
        waiting_tasks.size(),
        running_tasks.size());

    if (auto waiting_tasks = this->waiting_tasks.popAll(); waiting_tasks)
    {
        for (const auto & e : *waiting_tasks)
        {
            e.second->update(resp);
        }
    }

    for (auto & e : running_tasks)
    {
        e.second.doForceSet(resp);
    }
    running_tasks.clear();
}

ReadIndexFuturePtr ReadIndexDataNode::insertTask(const kvrpcpb::ReadIndexRequest & req)
{
    auto task = std::make_shared<ReadIndexFuture>();
    task->req = req;

    waiting_tasks.add(req.start_ts(), task);

    return task;
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

ReadIndexFuturePtr ReadIndexWorkerManager::genReadIndexFuture(const kvrpcpb::ReadIndexRequest & req)
{
    return getWorkerByRegion(req.context().region_id()).genReadIndexFuture(req);
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

ReadIndexWorker & ReadIndexWorkerManager::getWorkerByRegion(RegionID region_id)
{
    return *workers[region_id % workers.size()];
}

ReadIndexWorkerManager::ReadIndexWorkerManager(
    const TiFlashRaftProxyHelper & proxy_helper_,
    KVStore & kvstore_,
    size_t workers_cnt,
    ReadIndexWorkerManager::FnGetTickTime && fn_min_dur_handle_region,
    size_t runner_cnt)
    : proxy_helper(proxy_helper_)
    , kvstore(kvstore_)
    , logger(Logger::get("ReadIndexWorkers"))
{
    for (size_t i = 0; i < runner_cnt; ++i)
        runners.emplace_back(std::make_unique<ReadIndexRunner>(
            i,
            runner_cnt,
            kvstore,
            workers,
            logger,
            fn_min_dur_handle_region,
            std::make_shared<AsyncWaker::Notifier>()));

    {
        workers.reserve(workers_cnt);
        for (size_t i = 0; i < workers_cnt; ++i)
            workers.emplace_back(nullptr);

        for (size_t rid = 0; rid < runner_cnt; ++rid)
        {
            for (size_t wid = rid; wid < workers_cnt; wid += runner_cnt)
            {
                workers[wid]
                    = std::make_unique<ReadIndexWorker>(proxy_helper, kvstore, wid, runners[rid]->global_notifier);
            }
        }
    }
}

void ReadIndexWorkerManager::wakeAll()
{
    for (auto & runner : runners)
        runner->wake();
}

void ReadIndexWorkerManager::asyncRun()
{
    for (auto & runner : runners)
        runner->asyncRun();
}

void ReadIndexWorkerManager::stop()
{
    for (auto & runner : runners)
        runner->stop();
}

ReadIndexWorkerManager::~ReadIndexWorkerManager()
{
    stop();
}

void ReadIndexWorkerManager::runOneRoundAll(SteadyClock::duration min_dur)
{
    for (size_t id = 0; id < runners.size(); ++id)
        runOneRound(min_dur, id);
}

void ReadIndexWorkerManager::runOneRound(SteadyClock::duration min_dur, size_t id)
{
    runners[id]->runOneRound(min_dur);
}

BatchReadIndexRes ReadIndexWorkerManager::batchReadIndex(
    const std::vector<kvrpcpb::ReadIndexRequest> & reqs,
    uint64_t timeout_ms)
{
    TEST_LOG_FMT("reqs size {}, timeout {}ms", reqs.size(), timeout_ms);

    auto notifier = std::make_shared<AsyncWaker::Notifier>();
    BlockedReadIndexHelperV3 helper{timeout_ms, *notifier};
    std::queue<std::pair<RegionID, ReadIndexFuturePtr>> tasks;
    BatchReadIndexRes resps;
    resps.reserve(reqs.size());

    for (const auto & req : reqs)
    {
        auto region_id = req.context().region_id();
        auto & wk = this->getWorkerByRegion(region_id);
        auto future = wk.genReadIndexFuture(req);
        tasks.emplace(region_id, future);
    }
    this->wakeAll();

    TEST_LOG_FMT("wake read_index_worker");

    while (!tasks.empty())
    {
        auto & it = tasks.front();
        if (auto res = it.second->poll(notifier); res)
        {
            resps.emplace_back(std::move(*res), it.first);
            tasks.pop();
        }
        else
        {
            TEST_LOG_FMT("got resp {}, remain {}", resps.size(), tasks.size());
            if (helper.blockedWait() == AsyncNotifier::Status::Timeout)
            {
                break;
            }
        }
    }
    { // if meet timeout, which means part of regions can not get response from leader, try to poll rest tasks
        TEST_LOG_FMT("rest {}, poll rest tasks once", tasks.size());

        while (!tasks.empty())
        {
            auto & it = tasks.front();
            if (auto res = it.second->poll(); res)
            {
                resps.emplace_back(std::move(*res), it.first);
            }
            else
            {
                kvrpcpb::ReadIndexResponse tmp;
                tmp.mutable_region_error()->mutable_region_not_found();
                resps.emplace_back(std::move(tmp), it.first);
            }
            tasks.pop();
        }
    }
    return resps;
}

void ReadIndexWorker::removeRegion(uint64_t region_id)
{
    TEST_LOG_FMT("remove region_id={}", region_id);
    data_map.removeRegion(region_id);
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

std::unique_ptr<ReadIndexWorkerManager> ReadIndexWorkerManager::newReadIndexWorkerManager(
    const TiFlashRaftProxyHelper & proxy_helper,
    KVStore & kvstore,
    size_t cap,
    ReadIndexWorkerManager::FnGetTickTime && fn_min_dur_handle_region,
    size_t runner_cnt)
{
#ifdef ADD_TEST_DEBUG_LOG_FMT
    global_logger_for_test = &Poco::Logger::get("TestReadIndexWork");
#endif
    return std::make_unique<ReadIndexWorkerManager>(
        proxy_helper,
        kvstore,
        cap,
        std::move(fn_min_dur_handle_region),
        runner_cnt);
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
    if (read_index_worker_manager)
    {
        delete read_index_worker_manager;
        read_index_worker_manager = nullptr;
    }
}

void ReadIndexWorkerManager::ReadIndexRunner::wake() const
{
    global_notifier->wake();
}

void ReadIndexWorkerManager::ReadIndexRunner::stop()
{
    auto tmp = State::Running;
    state.compare_exchange_strong(tmp, State::Stopping, std::memory_order_acq_rel);
    global_notifier->wake();
    if (work_thread)
    {
        work_thread->join();
        work_thread.reset();
        LOG_INFO(logger, "Thread of read-index runner {} has joined", id);
    }
    state.store(State::Terminated);
}

void ReadIndexWorkerManager::ReadIndexRunner::blockedWaitFor(std::chrono::milliseconds timeout) const
{
    global_notifier->blockedWaitFor(timeout);
}

void ReadIndexWorkerManager::ReadIndexRunner::runOneRound(SteadyClock::duration min_dur)
{
    for (size_t i = id; i < workers.size(); i += runner_cnt)
        workers[i]->runOneRound(min_dur);
}

void ReadIndexWorkerManager::ReadIndexRunner::asyncRun()
{
    state = State::Running;
    work_thread = std::make_unique<std::thread>([this]() {
        std::string name = fmt::format("ReadIndexWkr-{}", id);
        setThreadName(name.data());
        auto [ptr_a, ptr_d] = getAllocDeallocPtr();
        kvstore.reportThreadAllocInfo(name, ReportThreadAllocateInfoType::Reset, 0);
        kvstore.reportThreadAllocInfo(name, ReportThreadAllocateInfoType::AllocPtr, reinterpret_cast<uint64_t>(ptr_a));
        kvstore.reportThreadAllocInfo(
            name,
            ReportThreadAllocateInfoType::DeallocPtr,
            reinterpret_cast<uint64_t>(ptr_d));
        LOG_INFO(logger, "Start read-index runner {}", id);
        while (true)
        {
            auto base_tick_timeout = fn_min_dur_handle_region();
            blockedWaitFor(base_tick_timeout);
            runOneRound(base_tick_timeout);
            if (state.load(std::memory_order_acquire) != State::Running)
                break;
        }
        kvstore.reportThreadAllocInfo(name, ReportThreadAllocateInfoType::Remove, 0);
        LOG_INFO(logger, "Start to stop read-index runner {}", id);
    });
}

ReadIndexWorkerManager::ReadIndexRunner::ReadIndexRunner(
    size_t id_,
    size_t runner_cnt_,
    KVStore & kvstore_,
    ReadIndexWorkers & workers_,
    LoggerPtr logger_,
    FnGetTickTime fn_min_dur_handle_region_,
    AsyncWaker::NotifierPtr global_notifier_)
    : id(id_)
    , runner_cnt(runner_cnt_)
    , kvstore(kvstore_)
    , workers(workers_)
    , logger(std::move(logger_))
    , fn_min_dur_handle_region(std::move(fn_min_dur_handle_region_))
    , global_notifier(std::move(global_notifier_))
{}

} // namespace DB
