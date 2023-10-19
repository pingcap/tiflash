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

#include <Common/CapacityLimits.h>
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
class WaitingNode : public SimpleIntrusiveNode<WaitingNode>
{
public:
    ALWAYS_INLINE void notifyNext()
    {
        if (next != this)
        {
            next->cv.notify_one();
            next->detach(); // avoid being notified more than once
        }
    }

    ALWAYS_INLINE void notifyAll()
    {
        for (auto * p = this; p->next != this; p = p->next)
            p->next->cv.notify_one();
    }

    template <typename Pred>
    ALWAYS_INLINE void wait(std::unique_lock<std::mutex> & lock, Pred pred)
    {
#ifdef __APPLE__
        WaitingNode node;
#else
        thread_local WaitingNode node;
#endif
        while (!pred())
        {
            node.prependTo(this);
            node.cv.wait(lock);
            node.detach();
        }
    }

    template <typename Pred>
    ALWAYS_INLINE bool waitFor(
        std::unique_lock<std::mutex> & lock,
        Pred pred,
        const std::chrono::steady_clock::time_point & deadline)
    {
#ifdef __APPLE__
        WaitingNode node;
#else
        thread_local WaitingNode node;
#endif
        while (!pred())
        {
            node.prependTo(this);
            auto res = node.cv.wait_until(lock, deadline);
            node.detach();
            if (res == std::cv_status::timeout)
                return false;
        }
        return true;
    }

private:
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
    using ElementAuxiliaryMemoryUsageFunc = std::function<Int64(const T & element)>;

    explicit MPMCQueue(
        const CapacityLimits & capacity_limits_,
        ElementAuxiliaryMemoryUsageFunc && get_auxiliary_memory_usage_ = [](const T &) { return 0; })
        : capacity_limits(capacity_limits_)
        , get_auxiliary_memory_usage(
              capacity_limits.max_bytes == std::numeric_limits<Int64>::max()
                  ? [](const T &) {
                        return 0;
                    }
                  : std::move(get_auxiliary_memory_usage_))
        , element_auxiliary_memory(capacity_limits.max_size, 0)
        , data(capacity_limits.max_size * sizeof(T))
    {}

    ~MPMCQueue() { drain(); }

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
    ALWAYS_INLINE Result pop(T & obj) { return popObj<true>(obj); }

    /// Besides all conditions mentioned at `pop`, `popTimeout` will return TIMEOUT if `timeout` is exceeded.
    template <typename Duration>
    ALWAYS_INLINE Result popTimeout(T & obj, const Duration & timeout)
    {
        auto deadline = SteadyClock::now() + timeout;
        return popObj<true>(obj, &deadline);
    }

    /// Non-blocking function.
    /// Besides all conditions mentioned at `pop`, `tryPop` will immediately return EMPTY if queue is `NORMAL` but empty.
    ALWAYS_INLINE Result tryPop(T & obj) { return popObj<false>(obj); }

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
        auto deadline = SteadyClock::now() + timeout;
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
        auto deadline = SteadyClock::now() + timeout;
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
    bool cancel() { return cancelWith(""); }

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
        return changeStatus([&] { status = Status::FINISHED; });
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
    using SteadyClock = std::chrono::steady_clock;
    using TimePoint = SteadyClock::time_point;
    using WaitingNode = MPMCQueueDetail::WaitingNode;

    void notifyAll()
    {
        reader_head.notifyAll();
        writer_head.notifyAll();
    }

    template <typename Pred>
    ALWAYS_INLINE bool wait(
        std::unique_lock<std::mutex> & lock,
        WaitingNode & head,
        Pred pred,
        const TimePoint * deadline)
    {
        if (deadline)
        {
            return head.waitFor(lock, pred, *deadline);
        }
        else
        {
            head.wait(lock, pred);
            return true;
        }
    }

    template <bool need_wait>
    Result popObj(T & res, [[maybe_unused]] const TimePoint * deadline = nullptr)
    {
        std::unique_lock lock(mu);
        bool is_timeout = false;

        if constexpr (need_wait)
        {
            /// read_pos < write_pos means the queue isn't empty
            auto pred = [&] {
                return read_pos < write_pos || !isNormal();
            };
            if (!wait(lock, reader_head, pred, deadline))
                is_timeout = true;
        }
        /// double check status after potential wait
        if (!isCancelled() && read_pos < write_pos)
        {
            auto & obj = getObj(read_pos);
            res = std::move(obj);
            destruct(obj);
            updateElementAuxiliaryMemory<true>(read_pos);

            /// update pos only after all operations that may throw an exception.
            ++read_pos;
            /// assert so in debug mode, we can get notified if some bugs happens when updating current_auxiliary_memory_usage
            assert(read_pos != write_pos || current_auxiliary_memory_usage == 0);
            if (read_pos == write_pos)
                current_auxiliary_memory_usage = 0;

            /// Notify next writer within the critical area because:
            /// 1. If we remove the next writer node and notify it later,
            ///    it may find itself can't obtain the lock while not being in the list.
            ///    This need carefully procesing in `assignObj`.
            /// 2. If we do not remove the next writer, only obtain its pointer and notify it later,
            ///    deadlock can be possible because different readers may notify one writer.
            if (current_auxiliary_memory_usage < capacity_limits.max_bytes)
                writer_head.notifyNext();
            return Result::OK;
        }
        if constexpr (need_wait)
        {
            if (is_timeout)
                return Result::TIMEOUT;
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
        std::unique_lock lock(mu);
        bool is_timeout = false;

        if constexpr (need_wait)
        {
            auto pred = [&] {
                return (write_pos - read_pos < capacity_limits.max_size
                        && current_auxiliary_memory_usage < capacity_limits.max_bytes)
                    || !isNormal();
            };
            if (!wait(lock, writer_head, pred, deadline))
                is_timeout = true;
        }

        /// double check status after potential wait
        /// check write_pos because timeouted will also reach here.
        if (isNormal()
            && (write_pos - read_pos < capacity_limits.max_size
                && current_auxiliary_memory_usage < capacity_limits.max_bytes))
        {
            void * addr = getObjAddr(write_pos);
            assigner(addr);
            updateElementAuxiliaryMemory<false>(write_pos);

            /// update pos only after all operations that may throw an exception.
            ++write_pos;

            /// See comments in `popObj`.
            reader_head.notifyNext();
            if (capacity_limits.max_bytes != std::numeric_limits<Int64>::max()
                && current_auxiliary_memory_usage < capacity_limits.max_bytes
                && write_pos - read_pos < capacity_limits.max_size)
                writer_head.notifyNext();
            return Result::OK;
        }
        if constexpr (need_wait)
        {
            if (is_timeout)
                return Result::TIMEOUT;
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

    ALWAYS_INLINE bool isNormal() const { return likely(status == Status::NORMAL); }

    ALWAYS_INLINE bool isCancelled() const { return unlikely(status == Status::CANCELLED); }

    ALWAYS_INLINE void * getObjAddr(Int64 pos)
    {
        pos = (pos % capacity_limits.max_size) * sizeof(T);
        return &data[pos];
    }

    ALWAYS_INLINE T & getObj(Int64 pos) { return *reinterpret_cast<T *>(getObjAddr(pos)); }

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
        current_auxiliary_memory_usage = 0;
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

    template <bool read>
    ALWAYS_INLINE void updateElementAuxiliaryMemory(size_t pos)
    {
        if constexpr (read)
        {
            auto & elem_value = element_auxiliary_memory[pos % capacity_limits.max_size];
            current_auxiliary_memory_usage -= elem_value;
            elem_value = 0;
        }
        else
        {
            auto auxiliary_memory = get_auxiliary_memory_usage(getObj(pos));
            current_auxiliary_memory_usage += auxiliary_memory;
            element_auxiliary_memory[pos % capacity_limits.max_size] = auxiliary_memory;
        }
    }

private:
    /// capacity_limits.max_bytes is the bound of all the element's auxiliary memory
    /// for an element stored in the queue, it will take two kinds of memory
    /// 1. the memory took by the element itself: sizeof(T) bytes, it is a constant value and already reserved in `data`
    /// 2. the auxiliary memory of the element, for example, if the element type is std::vector<Int64>,
    ///    then the auxiliary memory for each element is sizeof(Int64) * element.capacity()
    /// If the element stored in the queue has auxiliary memory usage, and the user wants to set a bound for the total
    /// auxiliary memory usage, then the user should provide the function to calculate the auxiliary memory usage
    /// Note: unlike capacity, capacity_limits.max_bytes is actually a soft-limit because we need to make at least
    /// one element can be pushed to the queue even if its auxiliary memory exceeds capacity_limits.max_bytes
    const CapacityLimits capacity_limits;
    const ElementAuxiliaryMemoryUsageFunc get_auxiliary_memory_usage;

    mutable std::mutex mu;
    WaitingNode reader_head;
    WaitingNode writer_head;
    Int64 read_pos = 0;
    Int64 write_pos = 0;
    Status status = Status::NORMAL;
    String cancel_reason;
    Int64 current_auxiliary_memory_usage = 0;

    std::vector<Int64> element_auxiliary_memory;
    std::vector<UInt8> data;
};

} // namespace DB
