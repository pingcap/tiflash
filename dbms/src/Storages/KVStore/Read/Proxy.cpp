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
} // namespace DB