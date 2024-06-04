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

#include <Common/nocopyable.h>
#include <common/defines.h>
#include <common/types.h>

#include <shared_mutex>

namespace DB
{

template <typename T>
class SharedMutexProtected
{
    DISALLOW_COPY_AND_MOVE(SharedMutexProtected);

private:
    template <typename U, typename Lock>
    class Locked
    {
        DISALLOW_COPY_AND_MOVE(Locked);

    public:
        Locked(U & value, std::shared_mutex & mutex)
            : m_value(value)
            , m_locker(mutex)
        {}

        ALWAYS_INLINE inline U const * operator->() const { return &m_value; }
        ALWAYS_INLINE inline U const & operator*() const { return m_value; }

        ALWAYS_INLINE inline U * operator->()
            requires(!std::is_const_v<U>)
        {
            return &m_value;
        }
        ALWAYS_INLINE inline U & operator*()
            requires(!std::is_const_v<U>)
        {
            return m_value;
        }

        ALWAYS_INLINE inline U const & get() const { return &m_value; }
        ALWAYS_INLINE inline U & get()
            requires(!std::is_const_v<U>)
        {
            return &m_value;
        }

    private:
        U & m_value;
        Lock m_locker;
    };

public:
    // Return a locked object that can be used to shared access the protected value.
    // Please destroy the object ASAP to release the lock.
    auto lockShared() const { return Locked<T const, std::shared_lock<std::shared_mutex>>(m_value, m_mutex); }
    // Return a locked object that can be used to exclusive access the protected value.
    // Please destroy the object ASAP to release the lock.
    auto lockExclusive() { return Locked<T, std::unique_lock<std::shared_mutex>>(m_value, m_mutex); }

    SharedMutexProtected() = default;

    template <typename Callback>
    decltype(auto) withShared(Callback callback) const
    {
        auto lock = lockShared();
        return callback(*lock);
    }

    template <typename Callback>
    decltype(auto) withExclusive(Callback callback)
    {
        auto lock = lockExclusive();
        return callback(*lock);
    }

    template <typename Callback>
    void forEachShared(Callback callback) const
    {
        withShared([&](auto const & value) {
            for (auto & item : value)
                callback(item);
        });
    }

    template <typename Callback>
    void forEachExclusive(Callback callback)
    {
        withExclusive([&](auto & value) {
            for (auto & item : value)
                callback(item);
        });
    }

private:
    T m_value;
    mutable std::shared_mutex m_mutex;
};

} // namespace DB
