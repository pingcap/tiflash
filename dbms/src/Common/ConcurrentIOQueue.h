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
  * There are additional overheads compared to using mpmcqueue directly
  * - two locks
  * - condition double check
  * Since io queues are not performance sensitive, they are acceptable.
  */
template <typename T>
class ConcurrentIOQueue
{
public:
    explicit ConcurrentIOQueue(size_t capacity)
        : mpmc_queue(std::max(1, capacity))
    {}

    ConcurrentIOQueue(
        size_t capacity,
        Int64 max_auxiliary_memory_usage_,
        typename MPMCQueue<T>::ElementAuxiliaryMemoryUsageFunc && get_auxiliary_memory_usage_)
        : mpmc_queue(std::max(1, capacity), max_auxiliary_memory_usage_, std::move(get_auxiliary_memory_usage_))
    {}

    /// blocking function.
    /// Just like MPMCQueue::push.
    MPMCQueueResult push(T && data)
    {
        return mpmc_queue.push(std::move(data));
    }

    /// Non-blocking function.
    /// Besides all conditions mentioned at `push`, `nonBlockingPush` will still return OK if queue is `NORMAL` and full.
    /// The obj that exceeds its capacity will be stored in remaining_objs and wait for the next pop/tryPop to trigger pushRemains.
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
                // obj that exceeds its capacity will be stored in remaining_objs.
                // And then waiting for the next pop/tryPop to trigger pushRemains.
                remaining_objs.push_front(std::move(data));
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
            // when finished, we still should pop the remaining_objs.
            return popRemains(data) ? MPMCQueueResult::OK : MPMCQueueResult::FINISHED;
        case MPMCQueueResult::OK:
            pushRemains();
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
            return popRemains(data) ? MPMCQueueResult::OK : MPMCQueueResult::FINISHED;
        case MPMCQueueResult::EMPTY:
        {
            // Double check if this queue is empty.
            std::lock_guard lock(mu);
            res = mpmc_queue.tryPop(data);
            switch (res)
            {
            case MPMCQueueResult::FINISHED:
            case MPMCQueueResult::EMPTY:
                // when finished, we still should pop the remaining_objs.
                // when mpmc_queue empty, we can try pop from remaining_objs.
                return popRemainsWithoutLock(data) ? MPMCQueueResult::OK : res;
            case MPMCQueueResult::OK:
                pushRemainsWithoutLock();
            default:
                return res;
            }
        }
        case MPMCQueueResult::OK:
            pushRemains();
        default:
            return res;
        }
    }

    size_t size() const
    {
        size_t mpmc_queue_size = mpmc_queue.size();
        std::lock_guard lock(mu);
        return mpmc_queue_size + remaining_objs.size();
    }

    // When the queue is finished, it may appear that the mpmc_queue is empty but the remaining_objs are not, causing isFull to return true.
    // But this is expected, and the return value of isFull is meaningless after finished/cancelled.
    bool isFull() const
    {
        {
            std::lock_guard lock(mu);
            if (!remaining_objs.empty())
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
            remaining_objs.clear();
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
    void pushRemainsWithoutLock()
    {
        while (!remaining_objs.empty() && mpmc_queue.tryPush(std::move(remaining_objs.back())) == MPMCQueueResult::OK)
            remaining_objs.pop_back();
    }

    void pushRemains()
    {
        std::lock_guard lock(mu);
        pushRemainsWithoutLock();
    }

    bool popRemainsWithoutLock(T & data)
    {
        if (!remaining_objs.empty())
        {
            data = std::move(remaining_objs.back());
            remaining_objs.pop_back();
            return true;
        }
        return false;
    }

    bool popRemains(T & data)
    {
        std::lock_guard lock(mu);
        return popRemainsWithoutLock(data);
    }

private:
    mutable std::mutex mu;
    // Used to hold objs pushed by `NonBlockingPush` that exceed the capacity.
    // pop/tryPop will try to push remaining_objs into the mpmc_queue when pop/tryPop.
    // The size of the deque is less than or equal to the number of writing threads,
    // because remaining_objs is only pushed when isFull return false.
    std::deque<T> remaining_objs;
    MPMCQueue<T> mpmc_queue;
};
} // namespace DB
