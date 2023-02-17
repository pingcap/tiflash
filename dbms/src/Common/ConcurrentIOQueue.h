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
/** A simple thread-safe concurrent queue for io.
  * Provide functions `nonBlockingPush` and `isFull` to support asynchronous writes.
  * ```
  * while (queue.isFull()) {}
  * queue.nonBlockingPush(std::move(obj));
  * ```
  * There is additional overhead compared to using mpmcqueue directly
  * - two locks
  * - condition double check.
  * Since io queues are not performance sensitive, they are acceptable.
  */
template <typename T>
class ConcurrentIOQueue
{
public:
    explicit ConcurrentIOQueue(size_t capacity_)
        : mpmc_queue(capacity_)
    {}

    ConcurrentIOQueue(
        size_t capacity_,
        Int64 max_auxiliary_memory_usage_,
        typename MPMCQueue<T>::ElementAuxiliaryMemoryUsageFunc && get_auxiliary_memory_usage_)
        : mpmc_queue(capacity_, max_auxiliary_memory_usage_, std::move(get_auxiliary_memory_usage_))
    {}

    /// Non-blocking function.
    /// Besides all conditions mentioned at `push`, `nonBlockingPush` will still return OK if queue is `NORMAL` and full.
    /// The obj that exceeds its capacity will be stored in remaings and wait for the next pop/tryPop to trigger kickRemaings.
    MPMCQueueResult nonBlockingPush(T && data)
    {
        auto res = mpmc_queue.tryPush(std::move(data));
        if (res == MPMCQueueResult::FULL)
        {
            // Double check if this queue is full.
            std::lock_guard lock(mu);
            res = mpmc_queue.tryPush(std::move(data));
            if (res == MPMCQueueResult::FULL)
            {
                // obj that exceeds its capacity will be stored in remaings.
                // And then waiting for the next pop/tryPop to trigger kickRemaings.
                remaings.push_front(std::move(data));
                return MPMCQueueResult::OK;
            }
        }
        return res;
    }

    MPMCQueueResult pop(T & data)
    {
        auto res = mpmc_queue.pop(data);
        switch (res)
        {
        case MPMCQueueResult::FINISHED:
            // when finished, we still should pop the remaings.
            return kickRemaingsOnce(data) ? MPMCQueueResult::OK : MPMCQueueResult::FINISHED;
        case MPMCQueueResult::OK:
            kickRemaings();
        default:
            return res;
        }
    }

    MPMCQueueResult tryPop(T & data)
    {
        auto res = mpmc_queue.tryPop(data);
        switch (res)
        {
        case MPMCQueueResult::FINISHED:
            // when finished, we still should pop the remaings.
            return kickRemaingsOnce(data) ? MPMCQueueResult::OK : MPMCQueueResult::FINISHED;
        case MPMCQueueResult::EMPTY:
        {
            // Double check if this queue is empty.
            std::lock_guard lock(mu);
            res = mpmc_queue.tryPop(data);
            switch (res)
            {
            case MPMCQueueResult::FINISHED:
            case MPMCQueueResult::EMPTY:
                // when finished, we still should pop the remaings.
                // when mpmc_queue empty, we can try pop from remaings.
                return kickRemaingsOnceWithoutLock(data) ? MPMCQueueResult::OK : res;
            case MPMCQueueResult::OK:
                kickRemaingsWithoutLock();
            default:
                return res;
            }
        }
        case MPMCQueueResult::OK:
            kickRemaings();
        default:
            return res;
        }
    }

    MPMCQueueResult push(T && data)
    {
        return mpmc_queue.push(std::move(data));
    }

    size_t size() const
    {
        std::lock_guard lock(mu);
        return remaings.size() + mpmc_queue.size();
    }

    // When the queue is finished, it may appear that the mpmc_queue is empty but the remaings are not, causing isFull to return true.
    // But this is expected, and the return value of isFull is meaningless after finished/cancelled.
    bool isFull() const
    {
        {
            std::lock_guard lock(mu);
            if (!remaings.empty())
                return true;
        }
        return mpmc_queue.isFull();
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
    void kickRemaingsWithoutLock()
    {
        while (!remaings.empty())
        {
            auto res = mpmc_queue.tryPush(std::move(remaings.back()));
            if (res == MPMCQueueResult::OK)
                remaings.pop_back();
            else
                return;
        }
    }

    void kickRemaings()
    {
        std::lock_guard lock(mu);
        kickRemaingsWithoutLock();
    }

    bool kickRemaingsOnceWithoutLock(T & data)
    {
        if (!remaings.empty())
        {
            data = std::move(remaings.back());
            remaings.pop_back();
            return true;
        }
        return false;
    }

    bool kickRemaingsOnce(T & data)
    {
        std::lock_guard lock(mu);
        return kickRemaingsOnceWithoutLock(data);
    }

private:
    mutable std::mutex mu;
    // Used to hold objs pushed by `NonBlockingPush` that exceed the capacity.
    // remaings will try to push into the mpmc_queue when pop/tryPop.
    std::deque<T> remaings;
    MPMCQueue<T> mpmc_queue;
};
} // namespace DB
