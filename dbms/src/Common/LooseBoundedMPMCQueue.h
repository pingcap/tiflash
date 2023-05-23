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
/** A simple thread-safe loose-bounded concurrent queue and basically compatible with MPMCQueue.
  * Provide functions `forcePush` and `isFull` to support asynchronous writes.
  * ```
  * while (queue.isFull()) {}
  * queue.forcePush(std::move(obj));
  * ```
  */
template <typename T>
class LooseBoundedMPMCQueue
{
public:
    using ElementAuxiliaryMemoryUsageFunc = std::function<Int64(const T & element)>;

    explicit LooseBoundedMPMCQueue(size_t capacity_)
        : capacity(std::max(1, capacity_))
        , max_auxiliary_memory_usage(std::numeric_limits<Int64>::max())
        , get_auxiliary_memory_usage([](const T &) { return 0; })
    {}
    LooseBoundedMPMCQueue(size_t capacity_, Int64 max_auxiliary_memory_usage_, ElementAuxiliaryMemoryUsageFunc && get_auxiliary_memory_usage_)
        : capacity(std::max(1, capacity_))
        , max_auxiliary_memory_usage(max_auxiliary_memory_usage_ <= 0 ? std::numeric_limits<Int64>::max() : max_auxiliary_memory_usage_)
        , get_auxiliary_memory_usage(max_auxiliary_memory_usage == std::numeric_limits<Int64>::max() ? [](const T &) {
            return 0;
        }
                                                                                                     : std::move(get_auxiliary_memory_usage_))
    {}

    /// blocking function.
    /// Just like MPMCQueue::push.
    template <typename U>
    MPMCQueueResult push(U && data)
    {
        std::unique_lock lock(mu);
        writer_head.wait(lock, [&] { return !isFullWithoutLock() || (unlikely(status != MPMCQueueStatus::NORMAL)); });

        if ((likely(status == MPMCQueueStatus::NORMAL)) && !isFullWithoutLock())
        {
            pushFront(std::forward<U>(data));
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

        if (isFullWithoutLock())
            return MPMCQueueResult::FULL;

        pushFront(std::forward<U>(data));
        return MPMCQueueResult::OK;
    }

    /// Non-blocking function.
    /// Besides all conditions mentioned at `push`, `forcePush` will still return OK if queue is `NORMAL` and full.
    /// The obj that exceeds its capacity will still be stored in queue.
    template <typename U>
    MPMCQueueResult forcePush(U && data)
    {
        std::lock_guard lock(mu);

        if unlikely (status == MPMCQueueStatus::CANCELLED)
            return MPMCQueueResult::CANCELLED;
        if unlikely (status == MPMCQueueStatus::FINISHED)
            return MPMCQueueResult::FINISHED;

        pushFront(std::forward<U>(data));
        return MPMCQueueResult::OK;
    }

    MPMCQueueResult pop(T & data)
    {
        std::unique_lock lock(mu);
        reader_head.wait(lock, [&] { return !queue.empty() || (unlikely(status != MPMCQueueStatus::NORMAL)); });

        if ((likely(status != MPMCQueueStatus::CANCELLED)) && !queue.empty())
        {
            data = popBack();
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

        data = popBack();
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
        return isFullWithoutLock();
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
        return changeStatus([&] {
            status = MPMCQueueStatus::CANCELLED;
            cancel_reason = std::move(reason);
        });
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
        return changeStatus([&] {
            status = MPMCQueueStatus::FINISHED;
        });
    }

private:
    bool isFullWithoutLock() const
    {
        return queue.size() >= capacity || current_auxiliary_memory_usage >= max_auxiliary_memory_usage;
    }

    template <typename FF>
    ALWAYS_INLINE bool changeStatus(FF && ff)
    {
        std::lock_guard lock(mu);
        if likely (status == MPMCQueueStatus::NORMAL)
        {
            ff();
            reader_head.notifyAll();
            writer_head.notifyAll();
            return true;
        }
        return false;
    }

    ALWAYS_INLINE T popBack()
    {
        auto element = std::move(queue.back());
        queue.pop_back();
        current_auxiliary_memory_usage -= element.memory_usage;
        assert(!queue.empty() || current_auxiliary_memory_usage == 0);
        writer_head.notifyNext();
        return element.data;
    }

    template <typename U>
    ALWAYS_INLINE void pushFront(U && data)
    {
        Int64 memory_usage = get_auxiliary_memory_usage(data);
        queue.emplace_front(std::forward<U>(data), memory_usage);
        current_auxiliary_memory_usage += memory_usage;
        reader_head.notifyNext();
    }

private:
    mutable std::mutex mu;
    struct DataWithMemoryUsage
    {
        T data;
        Int64 memory_usage;
        DataWithMemoryUsage(T && data_, Int64 memory_usage_)
            : data(std::move(data_))
            , memory_usage(memory_usage_)
        {}
        DataWithMemoryUsage(T & data_, Int64 memory_usage_)
            : data(data_)
            , memory_usage(memory_usage_)
        {}
    };

    std::deque<DataWithMemoryUsage> queue;
    size_t capacity;
    const Int64 max_auxiliary_memory_usage;
    const ElementAuxiliaryMemoryUsageFunc get_auxiliary_memory_usage;
    Int64 current_auxiliary_memory_usage = 0;

    MPMCQueueDetail::WaitingNode reader_head;
    MPMCQueueDetail::WaitingNode writer_head;

    MPMCQueueStatus status = MPMCQueueStatus::NORMAL;
    String cancel_reason;
};
} // namespace DB
