// Copyright 2024 PingCAP, Inc.
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

ReadIndexFuturePtr ReadIndexWorkerManager::genReadIndexFuture(const kvrpcpb::ReadIndexRequest & req)
{
    return getWorkerByRegion(req.context().region_id()).genReadIndexFuture(req);
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
        // Will insert the read index task into data node
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
    {
        // If meet timeout, which means some of the regions can not get response from leader, try to poll rest tasks
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
} // namespace DB