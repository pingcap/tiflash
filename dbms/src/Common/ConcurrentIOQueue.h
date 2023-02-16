// Copyright 2022 PingCAP, Ltd.
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

#pragma once

#include <Common/MPMCQueue.h>

#include <deque>

namespace DB
{
template <typename T>
class ConcurrentIOQueue
{
public:
    explicit ConcurrentIOQueue(size_t capacity_)
        : mpmc_queue(capacity_)
        , capacity(capacity_)
    {}

    MPMCQueueResult pop(T & data)
    {
        auto res = mpmc_queue.pop(data);
        if (res == MPMCQueueResult::OK)
            kickRemaingsOnce();
        else if (res == MPMCQueueResult::FINISHED)
        {
            // when finished, we still should pop the remaings.
            return kickRemaingsOnce(data) ? MPMCQueueResult::OK : MPMCQueueResult::FINISHED;
        }
        return res;
    }

    MPMCQueueResult tryPop(T & data)
    {
        auto res = mpmc_queue.tryPop(data);
        if (res == MPMCQueueResult::OK)
            kickRemaingsOnce();
        else if (res == MPMCQueueResult::FINISHED)
        {
            // when finished, we still should pop the remaings.
            return kickRemaingsOnce(data) ? MPMCQueueResult::OK : MPMCQueueResult::FINISHED;
        }
        return res;
    }

    MPMCQueueResult push(T && data)
    {
        return mpmc_queue.push(std::move(data));
    }

    MPMCQueueResult nonBlockingPush(T && data)
    {
        auto res = mpmc_queue.tryPush(std::move(data));
        if (res == MPMCQueueResult::FULL)
        {
            std::lock_guard lock(mu);
            remaings.push_front(std::move(data));
            return MPMCQueueResult::OK;
        }
        return res;
    }

    bool isFull()
    {
        {
            std::lock_guard lock(mu);
            if (!remaings.empty())
                return true;
        }
        return mpmc_queue.size() > capacity;
    }

    MPMCQueueStatus getStatus() const
    {
        return mpmc_queue.getStatus();
    }

    /// Cancel a NORMAL queue will wake up all blocking readers and writers.
    /// After `cancel()` the queue can't be pushed or popped any more.
    /// That means some objects may leave at the queue without poped.
    bool cancel()
    {
        return cancelWith("");
    }
    bool cancelWith(String reason)
    {
        if (mpmc_queue.cancelWith(std::move(reason)))
        {
            std::lock_guard lock(mu);
            remaings.clear();
            return true;
        }
        return false;
    }

    const String & getCancelReason() const
    {
        return mpmc_queue.getCancelReason();
    }

    /// Finish a NORMAL queue will wake up all blocking readers and writers.
    /// After `finish()` the queue can't be pushed any more while `pop` is allowed
    /// the queue is empty.
    /// Return true if the previous status is NORMAL.
    bool finish()
    {
        return mpmc_queue.finish();
    }

private:
    void kickRemaingsOnce()
    {
        std::lock_guard lock(mu);
        if (!remaings.empty())
        {
            auto res = mpmc_queue.tryPush(std::move(remaings.back()));
            if (res == MPMCQueueResult::OK)
                remaings.pop_back();
        }
    }

    bool kickRemaingsOnce(T & data)
    {
        std::lock_guard lock(mu);
        if (!remaings.empty())
        {
            data = std::move(remaings.back());
            return true;
        }
        return false;
    }

private:
    mutable std::mutex mu;
    std::deque<T> remaings;
    MPMCQueue<T> mpmc_queue;
    size_t capacity;
};
} // namespace DB
