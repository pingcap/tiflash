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

#include <boost/fiber/all.hpp>

namespace DB
{
/// FiberRWLock has the same interface with `std::shared_mutex` and is intended to
/// work with `std::shared_lock` or `std::unique_lock`.
/// FiberRWLock is designed to work well with both OS thread and boost::fiber, though the
/// latter performs better.
///
/// It is designed as writer-first in that waiting writer will block following readers.
/// TODO: if there's a new coming writer after some blocking readers, current implementation
/// will let this writer go before readers. This simplifies the code but hurts the fairness.
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
