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