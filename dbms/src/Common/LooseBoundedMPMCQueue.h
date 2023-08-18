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

#pragma once

#include <Common/MPMCQueue.h>

#include <deque>

namespace DB
{
/** A simple thread-safe loose-bounded concurrent queue and basically compatible with MPMCQueue.
  * Provide functions `forcePush` and `isWritable` to support non-blocking writes.
  * ```
  * while (!queue.isWritable()) {}
  * queue.forcePush(std::move(obj));
  * ```
  */
template <typename T>
class LooseBoundedMPMCQueue
{
public:
    using ElementAuxiliaryMemoryUsageFunc = std::function<Int64(const T & element)>;
    using PushCallback = std::function<void(const T & element)>;

    explicit LooseBoundedMPMCQueue(
        const CapacityLimits & capacity_limits_,
        ElementAuxiliaryMemoryUsageFunc && get_auxiliary_memory_usage_ = [](const T &) { return 0; },
        PushCallback && push_callback_ = {})
        : capacity_limits(capacity_limits_)
        , get_auxiliary_memory_usage(
              capacity_limits.max_bytes == std::numeric_limits<Int64>::max()
                  ? [](const T &) {
                        return 0;
                    }
                  : std::move(get_auxiliary_memory_usage_))
        , push_callback(std::move(push_callback_))
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
            return status == MPMCQueueStatus::NORMAL ? MPMCQueueResult::EMPTY : MPMCQueueResult::FINISHED;

        data = popBack();
        return MPMCQueueResult::OK;
    }

    MPMCQueueResult tryDequeue()
    {
        std::lock_guard lock(mu);

        if unlikely (status == MPMCQueueStatus::CANCELLED)
            return MPMCQueueResult::CANCELLED;

        if (queue.empty())
            return status == MPMCQueueStatus::NORMAL ? MPMCQueueResult::EMPTY : MPMCQueueResult::FINISHED;

        popBack();
        return MPMCQueueResult::OK;
    }

    size_t size() const
    {
        std::lock_guard lock(mu);
        return queue.size();
    }

    bool isWritable() const
    {
        std::lock_guard lock(mu);
        // When the queue is not in normal status, isWritable returns true to ensure that forcePush can be called.
        if unlikely (status != MPMCQueueStatus::NORMAL)
            return true;
        return !isFullWithoutLock();
    }

    MPMCQueueStatus getStatus() const
    {
        std::lock_guard lock(mu);
        return status;
    }

    /// Cancel a NORMAL queue will wake up all blocking readers and writers.
    /// After `cancel()` the queue can't be pushed or popped any more.
    /// That means some objects may leave at the queue without poped.
    bool cancel() { return cancelWith(""); }
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
        return changeStatus([&] { status = MPMCQueueStatus::FINISHED; });
    }

private:
    bool isFullWithoutLock() const
    {
        assert(current_auxiliary_memory_usage >= 0);
        return static_cast<Int64>(queue.size()) >= capacity_limits.max_size
            || current_auxiliary_memory_usage >= capacity_limits.max_bytes;
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
        if (push_callback)
        {
            push_callback(queue.front().data);
        }
        reader_head.notifyNext();
        /// consider a case that the queue capacity is 2, the max_auxiliary_memory_usage is 100,
        /// T1: a writer write an object with size 100
        /// T2: two writers(w2, w3) try to write, but all blocked because of the max_auxiliary_memory_usage
        /// T3: a reader reads the object, and it will notify one of the waiting writers
        /// T4: assuming w2 is notified, then it writes an object of size 50, and there is no reader at that time
        /// then the queue's size is 1 and current_auxiliary_memory_usage is 50, which means the
        /// queue is not full, but w3 is still blocked, the queue's status is not changed until
        /// 1. there is another reader
        /// 2. there is another writer
        /// if we notify the writer if the queue is not full here, w3 can write immediately
        if (capacity_limits.max_bytes != std::numeric_limits<Int64>::max() && !isFullWithoutLock())
            writer_head.notifyNext();
    }

private:
    mutable std::mutex mu;
    struct DataWithMemoryUsage
    {
        T data;
        Int64 memory_usage;
        DataWithMemoryUsage(const T && data_, Int64 memory_usage_)
            : data(std::move(data_))
            , memory_usage(memory_usage_)
        {}
        DataWithMemoryUsage(const T & data_, Int64 memory_usage_)
            : data(data_)
            , memory_usage(memory_usage_)
        {}
    };

    std::deque<DataWithMemoryUsage> queue;
    CapacityLimits capacity_limits;
    const ElementAuxiliaryMemoryUsageFunc get_auxiliary_memory_usage;
    const PushCallback push_callback;
    Int64 current_auxiliary_memory_usage = 0;

    MPMCQueueDetail::WaitingNode reader_head;
    MPMCQueueDetail::WaitingNode writer_head;

    MPMCQueueStatus status = MPMCQueueStatus::NORMAL;
    String cancel_reason;
};
} // namespace DB
