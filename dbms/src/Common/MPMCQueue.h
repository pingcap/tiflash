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
struct WaitingNode
{
    WaitingNode * next = nullptr;
    WaitingNode * prev = nullptr;
    std::condition_variable cv;

    WaitingNode()
    {
        next = this;
        prev = this;
    }

    void pushBack(WaitingNode & node)
    {
        node.prev = prev;
        node.next = this;
        prev->next = &node;
        prev = &node;
    }

    void removeSelfFromList()
    {
        if (next != this)
        {
            next->prev = prev;
            prev->next = next;
            next = this;
            prev = this;
        }
    }
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
    {
    }

    std::optional<T> pop()
    {
        return popObj();
    }

    template <typename Duration>
    std::optional<T> tryPop(const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        return popObj(&deadline);
    }

    template <typename U>
    ALWAYS_INLINE bool push(U && u)
    {
        return pushObj(std::forward<U>(u));
    }

    template <typename U, typename Duration>
    ALWAYS_INLINE bool tryPush(U && u, const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        return pushObj(std::forward<U>(u), &deadline);
    }

    template <typename... Args>
    ALWAYS_INLINE bool emplace(Args &&... args)
    {
        return emplaceObj(nullptr, std::forward<Args>(args)...);
    }

    template <typename... Args, typename Duration>
    ALWAYS_INLINE bool tryEmplace(Args &&... args, const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        return emplaceObj(&deadline, std::forward<Args>(args)...);
    }

    void cancel()
    {
        std::unique_lock read_lock(mu);
        if (!finished && !cancelled)
        {
            cancelled = true;
            notifyAll();
        }
    }

    void finish()
    {
        std::unique_lock read_lock(mu);
        if (!finished && !cancelled)
        {
            finished = true;
            notifyAll();
        }
    }

    MPMCQueueStatus getStatus() const
    {
        {
            /// both write_mu and read_mu are ok
            std::unique_lock write_lock(mu);
            if (unlikely(cancelled))
                return MPMCQueueStatus::CANCELLED;
            if (unlikely(finished))
                return MPMCQueueStatus::FINISHED;
        }
        return MPMCQueueStatus::NORMAL;
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
        std::unique_lock<std::mutex> & lock, WaitingNode & head, WaitingNode & node, Pred pred, const TimePoint * deadline)
    {
        head.pushBack(node);
        if (deadline)
            !node.cv.wait_until(lock, *deadline, pred);
        else
            node.cv.wait(lock, pred);
        node.removeSelfFromList();
    }

    ALWAYS_INLINE void notifyNext(WaitingNode & head)
    {
        auto * next = head.next;
        if (next != &head)
            next->cv.notify_one();
    }

    std::optional<T> popObj(const TimePoint * deadline = nullptr)
    {
        thread_local WaitingNode node;
        std::optional<T> res;
        {
            auto pred = [&] { return read_pos < write_pos || unlikely(cancelled || finished); };

            std::unique_lock lock(mu);
            if (unlikely(cancelled))
                return res;

            if (read_pos >= write_pos)
                wait(lock, reader_head, node, pred, deadline);

            if (likely(!cancelled && read_pos < write_pos))
            {
                auto & obj = objs[read_pos % capacity];
                ++read_pos;

                res = std::move(obj);
                obj.reset();

                notifyNext(writer_head);
            }
        }
        return res;
    }

    template <typename F>
    bool assignObj(const TimePoint * deadline, F && assigner)
    {
        thread_local WaitingNode node;
        {
            auto pred = [&] { return write_pos - read_pos < capacity || unlikely(cancelled || finished); };

            std::unique_lock lock(mu);
            if (unlikely(cancelled || finished))
                return false;

            if (write_pos - read_pos >= capacity)
                wait(lock, writer_head, node, pred, deadline);

            if (likely(!cancelled && write_pos - read_pos < capacity))
            {
                assigner(objs[write_pos % capacity]);
                ++write_pos;

                notifyNext(reader_head);
                return true;
            }
        }
        return false;
    }

    template <typename U>
    ALWAYS_INLINE bool pushObj(U && u, const TimePoint * deadline = nullptr)
    {
        return assignObj(deadline, [&](auto & obj) { obj = std::forward<U>(u); });
    }

    template <typename... Args>
    ALWAYS_INLINE bool emplaceObj(const TimePoint * deadline, Args &&... args)
    {
        return assignObj(deadline, [&](auto & obj) { obj.emplace(std::forward<Args>(args)...); });
    }

private:
    const Int64 capacity;

    mutable std::mutex mu;
    WaitingNode reader_head;
    WaitingNode writer_head;
    Int64 read_pos = 0;
    Int64 write_pos = 0;
    bool cancelled = false;
    bool finished = false;

    std::vector<std::optional<T>> objs;
};

} // namespace DB

