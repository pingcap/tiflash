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

#include <Common/SimpleIntrusiveNode.h>
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

    explicit MPMCQueue(Int64 capacity_)
        : capacity(capacity_)
        , data(capacity * sizeof(T))
    {
    }

    ~MPMCQueue()
    {
        std::unique_lock lock(mu);
        for (; read_pos < write_pos; ++read_pos)
            destruct(getObj(read_pos));
    }

    /// Block util:
    /// 1. Pop succeeds with a valid T: return true.
    /// 2. The queue is cancelled or finished: return false.
    bool pop(T & obj)
    {
        return popObj(obj);
    }

    /// Besides all conditions mentioned at `pop`, `tryPop` will return false if `timeout` is exceeded.
    template <typename Duration>
    bool tryPop(T & obj, const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        return popObj(obj, &deadline);
    }

    /// Block util:
    /// 1. Push succeeds and return true.
    /// 2. The queue is cancelled and return false.
    /// 3. The queue has finished and return false.
    template <typename U>
    ALWAYS_INLINE bool push(U && u)
    {
        return pushObj(std::forward<U>(u));
    }

    /// Besides all conditions mentioned at `push`, `tryPush` will return false if `timeout` is exceeded.
    template <typename U, typename Duration>
    ALWAYS_INLINE bool tryPush(U && u, const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        return pushObj(std::forward<U>(u), &deadline);
    }

    /// The same as `push` except it will construct the object in place.
    template <typename... Args>
    ALWAYS_INLINE bool emplace(Args &&... args)
    {
        return emplaceObj(nullptr, std::forward<Args>(args)...);
    }

    /// The same as `tryPush` except it will construct the object in place.
    template <typename... Args, typename Duration>
    ALWAYS_INLINE bool tryEmplace(Args &&... args, const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        return emplaceObj(&deadline, std::forward<Args>(args)...);
    }

    /// Cancel a NORMAL queue will wake up all blocking readers and writers.
    /// After `cancel()` the queue can't be pushed or popped any more.
    /// That means some objects may leave at the queue without poped.
    void cancel()
    {
        std::unique_lock lock(mu);
        if (isNormal())
        {
            status = Status::CANCELLED;
            notifyAll();
        }
    }

    /// Finish a NORMAL queue will wake up all blocking readers and writers.
    /// After `finish()` the queue can't be pushed any more while `pop` is allowed
    /// the queue is empty.
    /// Return true if the previous status is NORMAL.
    bool finish()
    {
        std::unique_lock lock(mu);
        if (isNormal())
        {
            status = Status::FINISHED;
            notifyAll();
            return true;
        }
        else
            return false;
    }

    bool isNextPopNonBlocking() const
    {
        std::unique_lock lock(mu);
        return read_pos < write_pos || !isNormal();
    }

    bool isNextPushNonBlocking() const
    {
        std::unique_lock lock(mu);
        return write_pos - read_pos < capacity || !isNormal();
    }

    MPMCQueueStatus getStatus() const
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
    ALWAYS_INLINE void wait(
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
                    break;
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

    bool popObj(T & res, const TimePoint * deadline = nullptr)
    {
#ifdef __APPLE__
        WaitingNode node;
#else
        thread_local WaitingNode node;
#endif
        {
            /// read_pos < write_pos means the queue isn't empty
            auto pred = [&] {
                return read_pos < write_pos || !isNormal();
            };

            std::unique_lock lock(mu);

            wait(lock, reader_head, node, pred, deadline);

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
                return true;
            }
        }
        return false;
    }

    template <typename F>
    bool assignObj(const TimePoint * deadline, F && assigner)
    {
#ifdef __APPLE__
        WaitingNode node;
#else
        thread_local WaitingNode node;
#endif
        auto pred = [&] {
            return write_pos - read_pos < capacity || !isNormal();
        };

        std::unique_lock lock(mu);

        wait(lock, writer_head, node, pred, deadline);

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
            return true;
        }
        return false;
    }

    template <typename U>
    ALWAYS_INLINE bool pushObj(U && u, const TimePoint * deadline = nullptr)
    {
        return assignObj(deadline, [&](void * addr) { new (addr) T(std::forward<U>(u)); });
    }

    template <typename... Args>
    ALWAYS_INLINE bool emplaceObj(const TimePoint * deadline, Args &&... args)
    {
        return assignObj(deadline, [&](void * addr) { new (addr) T(std::forward<Args>(args)...); });
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

private:
    const Int64 capacity;

    mutable std::mutex mu;
    WaitingNode reader_head;
    WaitingNode writer_head;
    Int64 read_pos = 0;
    Int64 write_pos = 0;
    Status status = Status::NORMAL;

    std::vector<UInt8> data;
};

} // namespace DB
