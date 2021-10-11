#pragma once

#include <boost/fiber/all.hpp>

namespace DB
{
class FiberRWLock
{
public:
    FiberRWLock() = default;
    FiberRWLock(const FiberRWLock &) = delete;
    ~FiberRWLock() = default;

    FiberRWLock & operator=(const FiberRWLock &) = delete;

    void lock()
    {
        std::unique_lock lock(mu);
        ++writer_wait_cnt;
        writer_cv.wait(lock, [&] { return writer_cnt == 0 && reader_cnt == 0; });
        --writer_wait_cnt;
        writer_cnt = 1;
    }

    bool try_lock()
    {
        std::unique_lock lock(mu);
        if (writer_cnt == 0 && reader_cnt == 0)
        {
            writer_cnt = 1;
            return true;
        }
        return false;
    }

    void unlock()
    {
        std::unique_lock lock(mu);
        writer_cnt = 0;
        if (writer_wait_cnt != 0)
            writer_cv.notify_one();
        else
            reader_cv.notify_all();
    }

    void lock_shared()
    {
        std::unique_lock lock(mu);
        reader_cv.wait(lock, [&] { return writer_cnt == 0 && writer_wait_cnt == 0; });
        ++reader_cnt;
    }

    bool try_lock_shared()
    {
        std::unique_lock lock(mu);
        if (writer_cnt == 0 && writer_wait_cnt == 0)
        {
            ++reader_cnt;
            return true;
        }
        return false;
    }

    void unlock_shared()
    {
        std::unique_lock lock(mu);
        if (--reader_cnt == 0 && writer_wait_cnt != 0)
            writer_cv.notify_one();
    }
private:
    boost::fibers::mutex mu;
    boost::fibers::condition_variable reader_cv;
    boost::fibers::condition_variable writer_cv;
    int reader_cnt = 0;
    int writer_cnt = 0;
    int writer_wait_cnt = 0;
};
} // namespace DB

