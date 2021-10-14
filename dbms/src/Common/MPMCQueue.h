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
/// WaitingNode is used to construct a double-linked waiting list so that
/// every time a push/pop succeeds, it can notify next reader/writer in fifo.
///
/// Double link is to support remove self from the mid of the list when timeout.
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
        if (node.next == &node)
        {
            node.prev = prev;
            node.next = this;
            prev->next = &node;
            prev = &node;
        }
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
        , objs(capacity)
    {
    }

    /// Block util:
    /// 1. Pop succeeds with a valid T.
    /// 2. The queue is cancelled. Then it will return a std::nullopt_t.
    /// 3. The queue has finished, all pushed values have been poped out. Then it will return a std::nullopt_t.
    std::optional<T> pop()
    {
        return popObj();
    }

    /// Besides all conditions mentioned at `pop`, `tryPop` will return a std::nullopt_t if `timeout` is exceeded.
    template <typename Duration>
    std::optional<T> tryPop(const Duration & timeout)
    {
        /// std::condition_variable::wait_until will always use system_clock.
        auto deadline = std::chrono::system_clock::now() + timeout;
        return popObj(&deadline);
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
    void finish()
    {
        std::unique_lock lock(mu);
        if (isNormal())
        {
            status = Status::FINISHED;
            notifyAll();
        }
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
        Pred & pred,
        const TimePoint * deadline)
    {
        do {
            head.pushBack(node);
            if (deadline)
                node.cv.wait_until(lock, *deadline);
            else
                node.cv.wait(lock);
            node.removeSelfFromList();
        } while (!pred());
    }

    ALWAYS_INLINE void notifyNext(WaitingNode & head)
    {
        auto * next = head.next;
        if (next != &head)
        {
            next->removeSelfFromList();
            next->cv.notify_one();
        }
    }

    std::optional<T> popObj(const TimePoint * deadline = nullptr)
    {
        thread_local WaitingNode node;
        std::optional<T> res;
        {
            /// read_pos < write_pos means the queue isn't empty
            auto pred = [&] {
                return read_pos < write_pos || !isNormal();
            };

            std::unique_lock lock(mu);
            if (!pred())
                wait(lock, reader_head, node, pred, deadline);

            /// double check status after potential wait
            /// check read_pos because timeouted will also reach here.
            if (!isCancelled() && read_pos < write_pos)
            {
                auto & obj = objs[read_pos % capacity];
                assert(obj.has_value());
                res = std::move(obj);
                /// Should manually reset a `std::optional` since `std::move` may not reset it.
                obj.reset();

                /// update pos only after all operations that may throw an exception.
                ++read_pos;

                /// Notify next writer within the critical area because:
                /// 1. If we remove the next writer node and notify it later,
                ///    it may find itself can't obtain the lock while not being in the list.
                ///    This need carefully procesing in `assignObj`.
                /// 2. If we do not remove the next writer, only obtain its pointer and notify it later,
                ///    deadlock can be possible because different readers may notify one writer.
                notifyNext(writer_head);
            }
        }
        return res;
    }

    template <typename F>
    bool assignObj(const TimePoint * deadline, F && assigner)
    {
        thread_local WaitingNode node;
        auto pred = [&] {
            return write_pos - read_pos < capacity || !isNormal();
        };

        std::unique_lock lock(mu);
        if (!pred())
            wait(lock, writer_head, node, pred, deadline);

        /// double check status after potential wait
        /// check write_pos because timeouted will also reach here.
        if (isNormal() && write_pos - read_pos < capacity)
        {
            auto & obj = objs[write_pos % capacity];
            assert(!obj.has_value());
            assigner(obj);

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
        return assignObj(deadline, [&](auto & obj) { obj = std::forward<U>(u); });
    }

    template <typename... Args>
    ALWAYS_INLINE bool emplaceObj(const TimePoint * deadline, Args &&... args)
    {
        return assignObj(deadline, [&](auto & obj) { obj.emplace(std::forward<Args>(args)...); });
    }

    ALWAYS_INLINE bool isNormal() const
    {
        return likely(status == Status::NORMAL);
    }

    ALWAYS_INLINE bool isCancelled() const
    {
        return unlikely(status == Status::CANCELLED);
    }

private:
    const Int64 capacity;

    mutable std::mutex mu;
    WaitingNode reader_head;
    WaitingNode writer_head;
    Int64 read_pos = 0;
    Int64 write_pos = 0;
    Status status = Status::NORMAL;

    std::vector<std::optional<T>> objs;
};

} // namespace DB
