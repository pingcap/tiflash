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
#ifdef __APPLE__
        MPMCQueueDetail::WaitingNode node;
#else
        thread_local MPMCQueueDetail::WaitingNode node;
#endif
        std::unique_lock lock(mu);
        wait(lock, writer_head, node, [&] { return queue.size() < capacity || (unlikely(status != MPMCQueueStatus::NORMAL)); });

        if ((likely(status == MPMCQueueStatus::NORMAL)) && queue.size() < capacity)
        {
            pushFront(std::move(data));
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

        pushFront(std::forward<U>(data));
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

        pushFront(std::move(data));
        return MPMCQueueResult::OK;
    }

    MPMCQueueResult pop(T & data)
    {
#ifdef __APPLE__
        MPMCQueueDetail::WaitingNode node;
#else
        thread_local MPMCQueueDetail::WaitingNode node;
#endif
        std::unique_lock lock(mu);
        wait(lock, reader_head, node, [&] { return !queue.empty() || (unlikely(status != MPMCQueueStatus::NORMAL)); });

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
    template <typename Pred>
    ALWAYS_INLINE void wait(
        std::unique_lock<std::mutex> & lock,
        MPMCQueueDetail::WaitingNode & head,
        MPMCQueueDetail::WaitingNode & node,
        Pred pred)
    {
        while (!pred())
        {
            node.prependTo(&head);
            node.cv.wait(lock);
            node.detach();
        }
    }

    ALWAYS_INLINE void notifyNext(MPMCQueueDetail::WaitingNode & head)
    {
        auto * next = head.next;
        if (next != &head)
        {
            next->cv.notify_one();
            next->detach(); // avoid being notified more than once
        }
    }

    ALWAYS_INLINE void notifyAll()
    {
        for (auto * p = &reader_head; p->next != &reader_head; p = p->next)
            p->next->cv.notify_one();
        for (auto * p = &writer_head; p->next != &writer_head; p = p->next)
            p->next->cv.notify_one();
    }

    template <typename FF>
    ALWAYS_INLINE bool changeStatus(FF && ff)
    {
        std::lock_guard lock(mu);
        if likely (status == MPMCQueueStatus::NORMAL)
        {
            ff();
            notifyAll();
            return true;
        }
        return false;
    }

    ALWAYS_INLINE T popBack()
    {
        auto data = std::move(queue.back());
        queue.pop_back();
        notifyNext(writer_head);
        return data;
    }

    template <typename U>
    ALWAYS_INLINE void pushFront(U && data)
    {
        queue.emplace_front(std::forward<U>(data));
        notifyNext(reader_head);
    }

private:
    mutable std::mutex mu;

    std::deque<T> queue;
    size_t capacity;

    MPMCQueueDetail::WaitingNode reader_head;
    MPMCQueueDetail::WaitingNode writer_head;

    MPMCQueueStatus status = MPMCQueueStatus::NORMAL;
    String cancel_reason;
};
} // namespace DB
