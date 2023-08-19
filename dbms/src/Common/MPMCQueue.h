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

#include <Common/Exception.h>
#include <Common/SimpleIntrusiveNode.h>
#include <Common/nocopyable.h>
#include <common/defines.h>
#include <common/types.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <type_traits>

namespace DB
{
namespace MPMCQueueDetail
{
/// WaitingNode is used to construct a double-linked waiting list so that
/// every time a push/pop succeeds, it can notify next reader/writer in fifo.
///
/// Double link is to support remove self from the mid of the list when timeout.
struct WaitingNode : public SimpleIntrusiveNode<WaitingNode>
{
    std::condition_variable cv;
};
} // namespace MPMCQueueDetail

enum class MPMCQueueStatus
{
    NORMAL,
    CANCELLED,
    FINISHED,
};

enum class MPMCQueueResult
{
    OK,
    CANCELLED,
    FINISHED,
    TIMEOUT,
    EMPTY,
    FULL,
};

/// MPMCQueue is a FIFO queue which supports concurrent operations from
/// multiple producers and consumers.
///
/// MPMCQueue is thread-safe and exception-safe.
///
/// It is inspired by MCSLock that all blocking readers/writers construct a
/// waiting list and everyone only wait on its own condition_variable.
///
/// This can significantly reduce contentions and avoid "thundering herd" problem.
template <typename T>
class MPMCQueue
{
public:
    using Status = MPMCQueueStatus;
    using Result = MPMCQueueResult;

    explicit MPMCQueue(size_t capacity_)
        : capacity(capacity_)
        , data(capacity * sizeof(T))
    {
    }

    ~MPMCQueue()
    {
        drain();
    }

    // Cannot to use copy/move constructor,
    // because MPMCQueue maybe used by different threads.
    // Copy and move it is dangerous.
    DISALLOW_COPY_AND_MOVE(MPMCQueue);

    /*
    * | Queue Status     | Empty      | Behavior                 |
    * |------------------|------------|--------------------------|
    * | Normal           | Yes        | Block                    |
    * | Normal           | No         | Pop and return OK        |
    * | Finished         | Yes        | return FINISHED          |
    * | Finished         | No         | Pop and return OK        |
    * | Cancelled        | Yes/No     | return CANCELLED         |
    * */
    ALWAYS_INLINE Result pop(T & obj)
    {
        return popObj<true>(obj);
    }

    /// Besides all conditions mentioned at `pop`, `popTimeout` will return TIMEOUT if `timeout` is exceeded.
    template <typename Duration>
    ALWAYS_INLINE Result popTimeout(T & obj, const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        return popObj<true>(obj, &deadline);
    }

    /// Non-blocking function.
    /// Besides all conditions mentioned at `pop`, `tryPop` will immediately return EMPTY if queue is `NORMAL` but empty.
    ALWAYS_INLINE Result tryPop(T & obj)
    {
        return popObj<false>(obj);
    }

    /*
    * | Queue Status     | Empty      | Behavior                 |
    * |------------------|------------|--------------------------|
    * | Normal           | Yes        | Block                    |
    * | Normal           | No         | Pop and return OK        |
    * | Finished         | Yes/No     | return FINISHED          |
    * | Cancelled        | Yes/No     | return CANCELLED         |
    * */
    template <typename U>
    ALWAYS_INLINE Result push(U && u)
    {
        return pushObj<true>(std::forward<U>(u));
    }

    /// Besides all conditions mentioned at `push`, `pushTimeout` will return TIMEOUT if `timeout` is exceeded.
    template <typename U, typename Duration>
    ALWAYS_INLINE Result pushTimeout(U && u, const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        return pushObj<true>(std::forward<U>(u), &deadline);
    }

    /// Non-blocking function.
    /// Besides all conditions mentioned at `push`, `tryPush` will immediately return FULL if queue is `NORMAL` and full.
    template <typename U>
    ALWAYS_INLINE Result tryPush(U && u)
    {
        return pushObj<false>(std::forward<U>(u));
    }

    /// The same as `push` except it will construct the object in place.
    template <typename... Args>
    ALWAYS_INLINE Result emplace(Args &&... args)
    {
        return emplaceObj<true>(nullptr, std::forward<Args>(args)...);
    }

    /// The same as `pushTimeout` except it will construct the object in place.
    template <typename... Args, typename Duration>
    ALWAYS_INLINE Result emplaceTimeout(Args &&... args, const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        return emplaceObj<true>(&deadline, std::forward<Args>(args)...);
    }

    /// The same as `tryPush` except it will construct the object in place.
    template <typename... Args>
    ALWAYS_INLINE Result tryEmplace(Args &&... args)
    {
        return emplaceObj<false>(nullptr, std::forward<Args>(args)...);
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
            status = Status::CANCELLED;
            cancel_reason = std::move(reason);
        });
    }

    /// Finish a NORMAL queue will wake up all blocking readers and writers.
    /// After `finish()` the queue can't be pushed any more while `pop` is allowed
    /// the queue is empty.
    /// Return true if the previous status is NORMAL.
    bool finish()
    {
        return changeStatus([&] {
            status = Status::FINISHED;
        });
    }

    Status getStatus() const
    {
        std::unique_lock lock(mu);
        return status;
    }

    size_t size() const
    {
        std::unique_lock lock(mu);
        assert(write_pos >= read_pos);
        return static_cast<size_t>(write_pos - read_pos);
    }

    const String & getCancelReason() const
    {
        std::unique_lock lock(mu);
        RUNTIME_ASSERT(isCancelled());
        return cancel_reason;
    }

private:
    using TimePoint = std::chrono::time_point<std::chrono::system_clock>;
    using WaitingNode = MPMCQueueDetail::WaitingNode;

    void notifyAll()
    {
        for (auto * p = &reader_head; p->next != &reader_head; p = p->next)
            p->next->cv.notify_one();
        for (auto * p = &writer_head; p->next != &writer_head; p = p->next)
            p->next->cv.notify_one();
    }

    template <typename Pred>
    ALWAYS_INLINE bool wait(
        std::unique_lock<std::mutex> & lock,
        WaitingNode & head,
        WaitingNode & node,
        Pred pred,
        const TimePoint * deadline)
    {
        if (deadline)
        {
            while (!pred())
            {
                node.prependTo(&head);
                auto res = node.cv.wait_until(lock, *deadline);
                node.detach();
                if (res == std::cv_status::timeout)
                    return false;
            }
        }
        else
        {
            while (!pred())
            {
                node.prependTo(&head);
                node.cv.wait(lock);
                node.detach();
            }
        }
        return true;
    }

    ALWAYS_INLINE void notifyNext(WaitingNode & head)
    {
        auto * next = head.next;
        if (next != &head)
        {
            next->cv.notify_one();
            next->detach(); // avoid being notified more than once
        }
    }

    template <bool need_wait>
    Result popObj(T & res, [[maybe_unused]] const TimePoint * deadline = nullptr)
    {
#ifdef __APPLE__
        WaitingNode node;
#else
        thread_local WaitingNode node;
#endif
        std::unique_lock lock(mu);

        if constexpr (need_wait)
        {
            /// read_pos < write_pos means the queue isn't empty
            auto pred = [&] {
                return read_pos < write_pos || !isNormal();
            };
            if (!wait(lock, reader_head, node, pred, deadline))
                return Result::TIMEOUT;
        }
        if (!isCancelled() && read_pos < write_pos)
        {
            auto & obj = getObj(read_pos);
            res = std::move(obj);
            destruct(obj);

            /// update pos only after all operations that may throw an exception.
            ++read_pos;

            /// Notify next writer within the critical area because:
            /// 1. If we remove the next writer node and notify it later,
            ///    it may find itself can't obtain the lock while not being in the list.
            ///    This need carefully procesing in `assignObj`.
            /// 2. If we do not remove the next writer, only obtain its pointer and notify it later,
            ///    deadlock can be possible because different readers may notify one writer.
            notifyNext(writer_head);
            return Result::OK;
        }
        switch (status)
        {
        case Status::NORMAL:
            return Result::EMPTY;
        case Status::CANCELLED:
            return Result::CANCELLED;
        case Status::FINISHED:
            return Result::FINISHED;
        }
    }

    template <bool need_wait, typename F>
    Result assignObj([[maybe_unused]] const TimePoint * deadline, F && assigner)
    {
#ifdef __APPLE__
        WaitingNode node;
#else
        thread_local WaitingNode node;
#endif
        std::unique_lock lock(mu);

        if constexpr (need_wait)
        {
            auto pred = [&] {
                return write_pos - read_pos < capacity || !isNormal();
            };
            if (!wait(lock, writer_head, node, pred, deadline))
                return Result::TIMEOUT;
        }

        /// double check status after potential wait
        /// check write_pos because timeouted will also reach here.
        if (isNormal() && write_pos - read_pos < capacity)
        {
            void * addr = getObjAddr(write_pos);
            assigner(addr);

            /// update pos only after all operations that may throw an exception.
            ++write_pos;

            /// See comments in `popObj`.
            notifyNext(reader_head);
            return Result::OK;
        }
        switch (status)
        {
        case Status::NORMAL:
            return Result::FULL;
        case Status::CANCELLED:
            return Result::CANCELLED;
        case Status::FINISHED:
            return Result::FINISHED;
        }
    }

    template <bool need_wait, typename U>
    ALWAYS_INLINE Result pushObj(U && u, const TimePoint * deadline = nullptr)
    {
        return assignObj<need_wait>(deadline, [&](void * addr) { new (addr) T(std::forward<U>(u)); });
    }

    template <bool need_wait, typename... Args>
    ALWAYS_INLINE Result emplaceObj(const TimePoint * deadline, Args &&... args)
    {
        return assignObj<need_wait>(deadline, [&](void * addr) { new (addr) T(std::forward<Args>(args)...); });
    }

    ALWAYS_INLINE bool isNormal() const
    {
        return likely(status == Status::NORMAL);
    }

    ALWAYS_INLINE bool isCancelled() const
    {
        return unlikely(status == Status::CANCELLED);
    }

    ALWAYS_INLINE void * getObjAddr(Int64 pos)
    {
        pos = (pos % capacity) * sizeof(T);
        return &data[pos];
    }

    ALWAYS_INLINE T & getObj(Int64 pos)
    {
        return *reinterpret_cast<T *>(getObjAddr(pos));
    }

    ALWAYS_INLINE void destruct(T & obj)
    {
        if constexpr (!std::is_trivially_destructible_v<T>)
            obj.~T();
    }

    void drain()
    {
        std::unique_lock lock(mu);
        for (; read_pos < write_pos; ++read_pos)
            destruct(getObj(read_pos));

        read_pos = 0;
        write_pos = 0;
    }

    template <typename F>
    ALWAYS_INLINE bool changeStatus(F && action)
    {
        std::unique_lock lock(mu);
        if (isNormal())
        {
            action();
            notifyAll();
            return true;
        }
        return false;
    }

private:
    const Int64 capacity;

    mutable std::mutex mu;
    WaitingNode reader_head;
    WaitingNode writer_head;
    Int64 read_pos = 0;
    Int64 write_pos = 0;
    Status status = Status::NORMAL;
    String cancel_reason;

    std::vector<UInt8> data;
};

} // namespace DB
