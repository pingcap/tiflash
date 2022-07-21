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
    std::mutex mutex_;
    std::condition_variable readerCv_;
    std::condition_variable writerCv_;
    std::condition_variable finishCv_;
    std::queue<T> queue_;
    bool done_;
    std::size_t maxSize_;

    std::size_t peak_queue_size;
    int64_t pop_times;
    int64_t pop_empty_times;
    // Must have lock to call this function
    bool full() const
    {
        if (maxSize_ == 0)
        {
            return false;
        }
        return queue_.size() >= maxSize_;
    }

public:
    /**
   * Constructs an empty work queue with an optional max size.
   * If `maxSize == 0` the queue size is unbounded.
   *
   * @param maxSize The maximum allowed size of the work queue.
   */
    WorkQueue(std::size_t maxSize = 0)
        : done_(false)
        , maxSize_(maxSize)
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
            std::unique_lock<std::mutex> lock(mutex_);
            while (full() && !done_)
            {
                writerCv_.wait(lock);
            }
            if (done_)
            {
                return false;
            }
            queue_.push(std::forward<U>(item));
            peak_queue_size = std::max(queue_.size(), peak_queue_size);
            if (size != nullptr)
            {
                *size = queue_.size();
            }
        }
        readerCv_.notify_one();
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
            std::unique_lock<std::mutex> lock(mutex_);
            pop_times++;
            while (queue_.empty() && !done_)
            {
                pop_empty_times++;
                readerCv_.wait(lock);
            }
            if (queue_.empty())
            {
                assert(done_);
                return false;
            }
            item = std::move(queue_.front());
            queue_.pop();
        }
        writerCv_.notify_one();
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
            std::lock_guard<std::mutex> lock(mutex_);
            maxSize_ = maxSize;
        }
        writerCv_.notify_all();
    }
    /**
   * Promise that `push()` won't be called again, so once the queue is empty
   * there will never any more work.
   */
    void finish()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            assert(!done_);
            done_ = true;
        }
        readerCv_.notify_all();
        writerCv_.notify_all();
        finishCv_.notify_all();
    }
    /// Blocks until `finish()` has been called (but the queue may not be empty).
    void waitUntilFinished()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!done_)
        {
            finishCv_.wait(lock);
        }
    }

    size_t size()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    std::tuple<int64_t, int64_t, size_t> getStat() const
    {
        return std::tuple<int64_t, int64_t, size_t>{pop_times, pop_empty_times, peak_queue_size};
    }
};
} // namespace DB::DM