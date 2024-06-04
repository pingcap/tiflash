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

namespace DB
{

template <typename T>
class MutexProtected
{
    DISALLOW_COPY_AND_MOVE(MutexProtected);

private:
    template <typename U>
    class Locked
    {
        DISALLOW_COPY_AND_MOVE(Locked);

    public:
        Locked(U & value, std::mutex & mutex)
            : m_value(value)
            , m_locker(mutex)
        {}

        ALWAYS_INLINE inline U const * operator->() const { return &m_value; }
        ALWAYS_INLINE inline U const & operator*() const { return m_value; }

        ALWAYS_INLINE inline U * operator->() { return &m_value; }
        ALWAYS_INLINE inline U & operator*() { return m_value; }

        ALWAYS_INLINE inline U const & get() const { return m_value; }
        ALWAYS_INLINE inline U & get() { return m_value; }

    private:
        U & m_value;
        std::scoped_lock<std::mutex> m_locker;
    };

    auto lockConst() const { return Locked<T const>(m_value, m_mutex); }
    auto lockMutable() { return Locked<T>(m_value, m_mutex); }

public:
    template <typename... Args>
    explicit MutexProtected(Args &&... args)
        : m_value(forward<Args>(args)...)
    {}

    template <typename Callback>
    decltype(auto) with(Callback callback) const
    {
        auto lock = lockConst();
        return callback(*lock);
    }

    template <typename Callback>
    decltype(auto) with(Callback callback)
    {
        auto lock = lockMutable();
        return callback(*lock);
    }

    template <typename Callback>
    void forEachConst(Callback callback) const
    {
        with([&](auto const & value) {
            for (auto & item : value)
                callback(item);
        });
    }

    template <typename Callback>
    void forEach(Callback callback)
    {
        with([&](auto & value) {
            for (auto & item : value)
                callback(item);
        });
    }

private:
    T m_value;
    mutable std::mutex m_mutex;
};

} // namespace DB
