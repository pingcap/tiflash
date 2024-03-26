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
void ReadIndexDataNode::WaitingTasks::add(Timestamp ts, ReadIndexFuturePtr f) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    waiting_tasks.emplace_back(ts, std::move(f));
}

std::optional<ReadIndexDataNode::WaitingTasks::Data> ReadIndexDataNode::WaitingTasks::popAll() NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    if (waiting_tasks.empty())
        return {};
    return std::move(waiting_tasks);
}

size_t ReadIndexDataNode::WaitingTasks::size() const NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();
    return waiting_tasks.size();
}

void ReadIndexDataNode::runOneRound(const TiFlashRaftProxyHelper & helper, const ReadIndexNotifyCtrlPtr & notify)
    NO_THREAD_SAFETY_ANALYSIS
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

void ReadIndexDataNode::consume(const TiFlashRaftProxyHelper & helper, Timestamp ts) NO_THREAD_SAFETY_ANALYSIS
{
    auto _ = genLockGuard();

    if (auto it = running_tasks.find(ts); it != running_tasks.end())
    {
        doConsume(helper, it);
    }
}

ReadIndexDataNode::~ReadIndexDataNode() NO_THREAD_SAFETY_ANALYSIS
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

} // namespace DB