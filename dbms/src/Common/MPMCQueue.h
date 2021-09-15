#pragma once

#include <common/defines.h>
#include <common/types.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>

namespace DB
{
namespace MPMCQueueDetail
{
template <typename T>
struct SingleElementQueue
{
    std::optional<T> obj;
    std::mutex mu;
    std::condition_variable cv;
    bool last_pop_timeouted = false;

    std::atomic<bool> cancelled = false; /// allow to cancel from outside without acquire lock

    bool isCancelled() const
    {
        return cancelled.load(std::memory_order_relaxed);
    }

    void cancel()
    {
        cancelled.store(true, std::memory_order_relaxed);
    }
};

enum class OPStatus
{
    SUCCEEDED,
    FAILED,
    NEED_RETRY
};
} // namespace MPMCQueueDetail

enum class MPMCQueueStatus
{
    NORMAL,
    CANCELLED,
    FINISHED,
};

template <typename T>
class MPMCQueue
{
public:
    explicit MPMCQueue(Int64 capacity_)
        : capacity(capacity_)
        , objs(capacity)
        , read_finished_cvs(capacity)
        , write_finished_cvs(capacity)
    {}

    std::optional<T> pop()
    {
        Int64 ticket = getReadTicket();
        if (ticket < 0)
            return {};

        auto res = popObj(ticket);
        finishRead(ticket);
        return std::move(res);
    }

    template <typename Duration>
    std::optional<T> tryPop(const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        Int64 ticket = getReadTicket(&deadline);
        if (ticket < 0)
            return {};

        auto res = popObj(ticket, &deadline);
        finishRead(ticket);
        return std::move(res);
    }

    template <typename U>
    bool push(U && u)
    {
        while (true)
        {
            Int64 ticket = getWriteTicket();
            if (ticket < 0)
                return {};

            auto res = pushObj(ticket, std::forward<U>(u));
            finishWrite(ticket);
            if (res != OPStatus::NEED_RETRY)
                return res == OPStatus::SUCCEEDED;
        }
        __builtin_unreachable();
    }

    template <typename U, typename Duration>
    bool tryPush(U && u, const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        while (true)
        {
            Int64 ticket = getWriteTicket(&deadline);
            if (ticket < 0)
                return {};

            auto res = pushObj(ticket, std::forward<U>(u), &deadline);
            finishWrite(ticket);
            if (res != OPStatus::NEED_RETRY)
                return res == OPStatus::SUCCEEDED;
        }
        __builtin_unreachable();
    }

    template <typename... Args>
    bool emplace(Args &&... args)
    {
        while (true)
        {
            Int64 ticket = getWriteTicket();
            if (ticket < 0)
                return {};

            auto res = emplaceObj(ticket, nullptr, std::forward<Args>(args)...);
            finishWrite(ticket);
            if (res != OPStatus::NEED_RETRY)
                return res == OPStatus::SUCCEEDED;
        }
        __builtin_unreachable();
    }

    template <typename... Args, typename Duration>
    bool tryEmplace(Args &&... args, const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        while (true)
        {
            Int64 ticket = getWriteTicket(&deadline);
            if (ticket < 0)
                return {};

            auto res = emplaceObj(ticket, &deadline, std::forward<Args>(args)...);
            finishWrite(ticket);
            if (res != OPStatus::NEED_RETRY)
                return res == OPStatus::SUCCEEDED;
        }
        __builtin_unreachable();
    }

    void cancel()
    {
        bool changed = false;
        {
            std::unique_lock read_lock(read_mu);
            std::unique_lock write_lock(write_mu);
            if (!finished && !cancelled)
            {
                cancelled = true;
                changed = true;
            }
        }
        if (changed)
        {
            read_cv.notify_all();
            write_cv.notify_all();
            for (auto & obj : objs)
            {
                obj.cancel();
                obj.cv.notify_all();
            }
            for (auto & cv : read_finished_cvs)
                cv.notify_all();
            for (auto & cv : write_finished_cvs)
                cv.notify_all();
        }
    }

    void finish()
    {
        bool changed = false;
        Int64 local_read_allocated = -1;
        Int64 local_write_allocated = -1;
        {
            std::unique_lock read_lock(read_mu);
            std::unique_lock write_lock(write_mu);
            if (!finished && !cancelled)
            {
                finished = true;
                changed = true;
                local_read_allocated = read_allocated;
                local_write_allocated = write_allocated;
            }
        }
        if (changed)
        {
            /// cancel all readers waiting for tickets that won't check forever
            for (Int64 i = local_write_allocated; i < local_read_allocated; ++i)
                objs[i % capacity].cancel();

            read_cv.notify_all();
            write_cv.notify_all();
            for (auto & obj : objs)
                obj.cv.notify_all();
            for (auto & cv : read_finished_cvs)
                cv.notify_all();
            for (auto & cv : write_finished_cvs)
                cv.notify_all();
        }
    }

    MPMCQueueStatus getStatus() const
    {
        {
            /// both write_mu and read_mu are ok
            std::unique_lock write_lock(write_mu);
            if (unlikely(cancelled))
                return MPMCQueueStatus::CANCELLED;
            if (unlikely(finished))
                return MPMCQueueStatus::FINISHED;
        }
        return MPMCQueueStatus::NORMAL;
    }
private:
    using TimePoint = std::chrono::time_point<std::chrono::system_clock>;
    using OPStatus = MPMCQueueDetail::OPStatus;

    Int64 getReadTicket(const TimePoint * deadline = nullptr)
    {
        Int64 ticket = -1;
        bool timeouted = false;
        auto pred = [&] { return read_allocated - read_finished < capacity || cancelled || readFinished(); };
        {
            std::unique_lock lock(read_mu);
            if (deadline)
                timeouted = !read_cv.wait_until(lock, *deadline, pred);
            else
                read_cv.wait(lock, pred);
            if (!timeouted && !cancelled && !readFinished() && read_allocated - read_finished < capacity)
                ticket = read_allocated++;
        }
        return ticket;
    }

    Int64 getWriteTicket(const TimePoint * deadline = nullptr)
    {
        Int64 ticket = -1;
        bool timeouted = false;
        auto pred = [&] { return write_allocated - write_finished < capacity || finished || cancelled; };
        {
            std::unique_lock lock(write_mu);
            if (deadline)
                timeouted = !write_cv.wait_until(lock, *deadline, pred);
            else
                write_cv.wait(lock, pred);
            if (!timeouted && !finished && !cancelled && write_allocated - write_finished < capacity)
                ticket = write_allocated++;
        }
        return ticket;
    }

    std::optional<T> popObj(Int64 ticket, const TimePoint * deadline = nullptr)
    {
        auto & queue = objs[ticket % capacity];
        std::optional<T> res;
        auto pred = [&] { return queue.obj.has_value() || queue.isCancelled(); };
        {
            bool timeouted = false;
            std::unique_lock lock(queue.mu);
            if (deadline)
                timeouted = !queue.cv.wait_until(lock, *deadline, pred);
            else
                queue.cv.wait(lock, pred);
            if (timeouted)
            {
                queue.last_pop_timeouted = true;
            }
            else if (!queue.obj.has_value()) /// cancelled
            {
                /// do nothing
            }
            else
            {
                queue.last_pop_timeouted = false;
                res = std::move(queue.obj);
                queue.obj.reset();
            }
        }
        queue.cv.notify_one();
        return std::move(res);
    }

    template <typename F>
    OPStatus assignObj(Int64 ticket, const TimePoint * deadline, F && assigner)
    {
        auto & queue = objs[ticket % capacity];
        OPStatus res = OPStatus::FAILED;
        auto pred = [&] { return queue.last_pop_timeouted || !queue.obj.has_value() || queue.isCancelled(); };
        {
            bool timeouted = false;
            std::unique_lock lock(queue.mu);
            if (deadline)
                timeouted = !queue.cv.wait_until(lock, *deadline, pred);
            else
                queue.cv.wait(lock, pred);
            if (timeouted)
            {
                res = OPStatus::FAILED;
            }
            else if (queue.obj.has_value()) /// cancelled
            {
                res = OPStatus::FAILED;
            }
            else if (queue.last_pop_timeouted)
            {
                res = OPStatus::NEED_RETRY;
            }
            else
            {
                assigner(queue.obj);
                res = OPStatus::SUCCEEDED;
            }
        }
        queue.cv.notify_one();
        return res;
    }

    template <typename U>
    OPStatus pushObj(Int64 ticket, U && u, const TimePoint * deadline = nullptr)
    {
        return assignObj(ticket, deadline, [&](auto & obj) { obj = std::forward<U>(u); });
    }

    template <typename... Args>
    OPStatus emplaceObj(Int64 ticket, const TimePoint * deadline, Args &&... args)
    {
        return assignObj(ticket, deadline, [&](auto & obj) { obj.emplace(std::forward<Args>(args)...); });
    }

    void bumpFinished(Int64 ticket, std::mutex & mu, std::vector<std::condition_variable> & cvs, Int64 & finished_ticket)
    {
        Int64 pos = ticket % capacity;
        auto & cv = cvs[pos];
        {
            std::unique_lock lock(mu);
            cv.wait(lock, [&] { return finished_ticket == ticket || cancelled; });
            ++finished_ticket; // when cancelled it's ok to bump finished.
        }
        Int64 next_pos = (pos + 1) % capacity;
        cvs[next_pos].notify_one();
    }

    void finishRead(Int64 ticket)
    {
        bumpFinished(ticket, read_mu, read_finished_cvs, read_finished);
        read_cv.notify_one();
    }

    void finishWrite(Int64 ticket)
    {
        bumpFinished(ticket, write_mu, write_finished_cvs, write_finished);
        write_cv.notify_one();
    }

    /// must call under protection of `read_mu`.
    bool readFinished() const
    {
        /// it's safe to visit write_allocated here
        /// since after `finished` is set to true `write_allocated` won't change
        /// and the change of `finished` is always under `read_mu`.
        return finished && read_allocated >= write_allocated;
    }
private:
    const Int64 capacity;

    mutable std::mutex read_mu;
    mutable std::mutex write_mu;
    std::condition_variable read_cv;
    std::condition_variable write_cv;
    Int64 read_finished = 0;
    Int64 read_allocated = 0;
    Int64 write_finished = 0;
    Int64 write_allocated = 0;
    bool cancelled = false;
    bool finished = false;

    std::vector<MPMCQueueDetail::SingleElementQueue<T>> objs;
    std::vector<std::condition_variable> read_finished_cvs;
    std::vector<std::condition_variable> write_finished_cvs;
};

} // namespace DB

