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

#include <stdint.h>

#include <condition_variable>
#include <mutex>
#include <queue>

namespace DB::DM
{
template <typename T>
class WorkQueue
{
    // Protects all member variable access
    std::mutex mu;
    std::condition_variable reader_cv;
    std::condition_variable writer_cv;
    std::condition_variable finish_cv;
    std::queue<T> queue;
    bool done;
    std::size_t max_size;

    std::size_t peak_queue_size;
    int64_t pop_times;
    int64_t pop_empty_times;
    // Must have lock to call this function
    bool full() const
    {
        if (max_size == 0)
        {
            return false;
        }
        return queue.size() >= max_size;
    }

public:
    /**
   * Constructs an empty work queue with an optional max size.
   * If `maxSize == 0` the queue size is unbounded.
   *
   * @param maxSize The maximum allowed size of the work queue.
   */
    explicit WorkQueue(std::size_t maxSize = 0)
        : done(false)
        , max_size(maxSize)
        , peak_queue_size(0)
        , pop_times(0)
        , pop_empty_times(0)
    {}
    /**
   * Push an item onto the work queue.  Notify a single thread that work is
   * available.  If `finish()` has been called, do nothing and return false.
   * If `push()` returns false, then `item` has not been copied from.
   *
   * @param item  Item to push onto the queue.
   * @returns     True upon success, false if `finish()` has been called.  An
   *               item was pushed iff `push()` returns true.
   */
    template <typename U>
    bool push(U && item, size_t * size)
    {
        {
            std::unique_lock<std::mutex> lock(mu);
            while (full() && !done)
            {
                writer_cv.wait(lock);
            }
            if (done)
            {
                return false;
            }
            queue.push(std::forward<U>(item));
            peak_queue_size = std::max(queue.size(), peak_queue_size);
            if (size != nullptr)
            {
                *size = queue.size();
            }
        }
        reader_cv.notify_one();
        return true;
    }
    /**
   * Attempts to pop an item off the work queue.  It will block until data is
   * available or `finish()` has been called.
   *
   * @param[out] item  If `pop` returns `true`, it contains the popped item.
   *                    If `pop` returns `false`, it is unmodified.
   * @returns          True upon success.  False if the queue is empty and
   *                    `finish()` has been called.
   */
    bool pop(T & item)
    {
        {
            std::unique_lock<std::mutex> lock(mu);
            pop_times++;
            while (queue.empty() && !done)
            {
                pop_empty_times++;
                reader_cv.wait(lock);
            }
            if (queue.empty())
            {
                assert(done);
                return false;
            }
            item = std::move(queue.front());
            queue.pop();
        }
        writer_cv.notify_one();
        return true;
    }
    /**
   * Sets the maximum queue size.  If `maxSize == 0` then it is unbounded.
   *
   * @param maxSize The new maximum queue size.
   */
    void setMaxSize(std::size_t maxSize)
    {
        {
            std::lock_guard lock(mu);
            max_size = maxSize;
        }
        writer_cv.notify_all();
    }
    /**
   * Promise that `push()` won't be called again, so once the queue is empty
   * there will never any more work.
   */
    void finish()
    {
        {
            std::lock_guard lock(mu);
            assert(!done);
            done = true;
        }
        reader_cv.notify_all();
        writer_cv.notify_all();
        finish_cv.notify_all();
    }
    /// Blocks until `finish()` has been called (but the queue may not be empty).
    void waitUntilFinished()
    {
        std::unique_lock<std::mutex> lock(mu);
        while (!done)
        {
            finish_cv.wait(lock);
        }
    }

    size_t size()
    {
        std::lock_guard lock(mu);
        return queue.size();
    }

    std::tuple<int64_t, int64_t, size_t> getStat() const
    {
        return std::tuple<int64_t, int64_t, size_t>{pop_times, pop_empty_times, peak_queue_size};
    }
};
} // namespace DB::DM
