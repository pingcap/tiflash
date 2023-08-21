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

#include <Poco/Mutex.h>
#include <Poco/Semaphore.h>
#include <common/types.h>

#include <queue>
#include <type_traits>


namespace detail
{
template <typename T, bool is_nothrow_move_assignable = std::is_nothrow_move_assignable_v<T>>
struct MoveOrCopyIfThrow;

template <typename T>
struct MoveOrCopyIfThrow<T, true>
{
    void operator()(T && src, T & dst) const { dst = std::forward<T>(src); }
};

template <typename T>
struct MoveOrCopyIfThrow<T, false>
{
    void operator()(T && src, T & dst) const { dst = src; }
};

template <typename T>
void moveOrCopyIfThrow(T && src, T & dst)
{
    MoveOrCopyIfThrow<T>()(std::forward<T>(src), dst);
}
}; // namespace detail

/** A very simple thread-safe queue of limited size.
  * If you try to pop an item from an empty queue, the thread is blocked until the queue becomes nonempty.
  * If you try to push an element into an overflowed queue, the thread is blocked until space appears in the queue.
  */
template <typename T>
class ConcurrentBoundedQueue
{
private:
    std::queue<T> queue;
    Poco::FastMutex mutex;
    Poco::Semaphore fill_count;
    Poco::Semaphore empty_count;

public:
    explicit ConcurrentBoundedQueue(size_t max_fill)
        : fill_count(0, max_fill)
        , empty_count(max_fill, max_fill)
    {}

    void push(const T & x)
    {
        empty_count.wait();
        {
            Poco::ScopedLock<Poco::FastMutex> lock(mutex);
            queue.push(x);
        }
        fill_count.set();
    }

    template <typename... Args>
    void emplace(Args &&... args)
    {
        empty_count.wait();
        {
            Poco::ScopedLock<Poco::FastMutex> lock(mutex);
            queue.emplace(std::forward<Args>(args)...);
        }
        fill_count.set();
    }

    void pop(T & x)
    {
        fill_count.wait();
        {
            Poco::ScopedLock<Poco::FastMutex> lock(mutex);
            detail::moveOrCopyIfThrow(std::move(queue.front()), x);
            queue.pop();
        }
        empty_count.set();
    }

    bool tryPush(const T & x, UInt64 milliseconds = 0)
    {
        if (empty_count.tryWait(milliseconds))
        {
            {
                Poco::ScopedLock<Poco::FastMutex> lock(mutex);
                queue.push(x);
            }
            fill_count.set();
            return true;
        }
        return false;
    }

    template <typename... Args>
    bool tryEmplace(UInt64 milliseconds, Args &&... args)
    {
        if (empty_count.tryWait(milliseconds))
        {
            {
                Poco::ScopedLock<Poco::FastMutex> lock(mutex);
                queue.emplace(std::forward<Args>(args)...);
            }
            fill_count.set();
            return true;
        }
        return false;
    }

    bool tryPop(T & x, UInt64 milliseconds = 0)
    {
        if (fill_count.tryWait(milliseconds))
        {
            {
                Poco::ScopedLock<Poco::FastMutex> lock(mutex);
                detail::moveOrCopyIfThrow(std::move(queue.front()), x);
                queue.pop();
            }
            empty_count.set();
            return true;
        }
        return false;
    }

    size_t size()
    {
        Poco::ScopedLock<Poco::FastMutex> lock(mutex);
        return queue.size();
    }

    void clear()
    {
        while (fill_count.tryWait(0))
        {
            {
                Poco::ScopedLock<Poco::FastMutex> lock(mutex);
                queue.pop();
            }
            empty_count.set();
        }
    }
};
