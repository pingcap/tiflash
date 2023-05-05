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
  */
template <typename T>
class ConcurrentIOQueue
{
public:
    explicit ConcurrentIOQueue(size_t capacity_)
        : capacity(std::max(1, capacity_))
    {}

    /// blocking function.
    /// Just like MPMCQueue::push.
    MPMCQueueResult push(T && data)
    {
        std::unique_lock lock(mu);

        push_cv.wait(lock, [&] { return queue.size() < capacity || unlikely (status != MPMCQueueStatus::NORMAL); });

        if ((likely(status == MPMCQueueStatus::NORMAL)) && queue.size() < capacity)
        {
            queue.emplace_front(std::move(data));
            pop_cv.notify_one();
            return MPMCQueueResult::OK;
        }

        switch (status)
        {
        case MPMCQueueStatus::NORMAL:
            return MPMCQueueResult::FULL;
        case MPMCQueueStatus::CANCELLED:
            return MPMCQueueResult::CANCELLED;
        case MPMCQueueStatus::FINISHED:
            return MPMCQueueResult::FINISHED;
        }
    }

    /// Just like MPMCQueue::tryPush.
    template <typename U>
    MPMCQueueResult tryPush(U && data)
    {
        std::lock_guard lock(mu);

        if unlikely (status == MPMCQueueStatus::CANCELLED)
           return MPMCQueueResult::CANCELLED;
        if unlikely (status == MPMCQueueStatus::FINISHED)
           return MPMCQueueResult::FINISHED;

        if (queue.size() >= capacity)
            return MPMCQueueResult::FULL;

        queue.emplace_front(std::forward<U>(data));
        pop_cv.notify_one();
        return MPMCQueueResult::OK;
    }

    /// Non-blocking function.
    /// Besides all conditions mentioned at `push`, `nonBlockingPush` will still return OK if queue is `NORMAL` and full.
    /// The obj that exceeds its capacity will still be stored in queue.
    MPMCQueueResult nonBlockingPush(T && data)
    {
        std::lock_guard lock(mu);

        if unlikely (status == MPMCQueueStatus::CANCELLED)
           return MPMCQueueResult::CANCELLED;
        if unlikely (status == MPMCQueueStatus::FINISHED)
           return MPMCQueueResult::FINISHED;

        queue.push_front(std::move(data));
        pop_cv.notify_one();
        return MPMCQueueResult::OK;
    }

    MPMCQueueResult pop(T & data)
    {
        std::unique_lock lock(mu);

        pop_cv.wait(lock, [&] { return !queue.empty() || unlikely (status != MPMCQueueStatus::NORMAL); });

        if ((likely(status != MPMCQueueStatus::CANCELLED)) && !queue.empty())
        {
            data = std::move(queue.back());
            queue.pop_back();
            push_cv.notify_one();
            return MPMCQueueResult::OK;
        }

        switch (status)
        {
        case MPMCQueueStatus::NORMAL:
            return MPMCQueueResult::EMPTY;
        case MPMCQueueStatus::CANCELLED:
            return MPMCQueueResult::CANCELLED;
        case MPMCQueueStatus::FINISHED:
            return MPMCQueueResult::FINISHED;
        }
    }

    MPMCQueueResult tryPop(T & data)
    {
        std::lock_guard lock(mu);

        if unlikely (status == MPMCQueueStatus::CANCELLED)
            return MPMCQueueResult::CANCELLED;

        if (queue.empty())
            return status == MPMCQueueStatus::NORMAL
                ? MPMCQueueResult::EMPTY
                : MPMCQueueResult::FINISHED;

        data = std::move(queue.back());
        queue.pop_back();
        push_cv.notify_one();
        return MPMCQueueResult::OK;
    }

    size_t size() const
    {
        std::lock_guard lock(mu);
        return queue.size();
    }

    bool isFull() const
    {
        std::lock_guard lock(mu);
        return queue.size() >= capacity;
    }

    MPMCQueueStatus getStatus() const
    {
        std::lock_guard lock(mu);
        return status;
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
        std::lock_guard lock(mu);
        if likely (status == MPMCQueueStatus::NORMAL)
        {
            status = MPMCQueueStatus::CANCELLED;
            cancel_reason = std::move(reason);
            pop_cv.notify_all();
            push_cv.notify_all();
            return true;
        }
        return false;
    }

    const String & getCancelReason() const
    {
        std::unique_lock lock(mu);
        RUNTIME_ASSERT(status == MPMCQueueStatus::CANCELLED);
        return cancel_reason;
    }

    /// Finish a NORMAL queue will wake up all blocking readers and writers.
    /// After `finish()` the queue can't be pushed any more while `pop` is allowed
    /// the queue is empty.
    /// Return true if the previous status is NORMAL.
    bool finish()
    {
        std::lock_guard lock(mu);
        if likely (status == MPMCQueueStatus::NORMAL)
        {
            status = MPMCQueueStatus::FINISHED;
            pop_cv.notify_all();
            push_cv.notify_all();
            return true;
        }
        return false;
    }

private:
    mutable std::mutex mu;
    std::deque<T> queue;
    size_t capacity;
    std::condition_variable pop_cv;
    std::condition_variable push_cv;

    MPMCQueueStatus status = MPMCQueueStatus::NORMAL;
    String cancel_reason;
};
} // namespace DB
