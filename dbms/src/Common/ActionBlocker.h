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

#include <atomic>

namespace DB
{
/// An atomic variable that is used to block and interrupt certain actions
/// If it is not zero then actions related with it should be considered as interrupted
class ActionBlocker
{
private:
    mutable std::atomic<int> counter{0};

public:
    bool isCancelled() const { return counter > 0; }

    /// Temporarily blocks corresponding actions (while the returned object is alive)
    struct LockHolder;
    LockHolder cancel() const { return LockHolder(this); }

    /// Cancel the actions forever.
    void cancelForever() const { ++counter; }

    /// Returns reference to counter to allow to watch on it directly.
    auto & getCounter() { return counter; }

    /// Blocks related action while a BlockerHolder instance exists
    struct LockHolder
    {
        explicit LockHolder(const ActionBlocker * var_ = nullptr)
            : var(var_)
        {
            if (var)
                ++var->counter;
        }

        LockHolder(LockHolder && other) noexcept { *this = std::move(other); }

        LockHolder & operator=(LockHolder && other) noexcept
        {
            var = other.var;
            other.var = nullptr;
            return *this;
        }

        DISALLOW_COPY(LockHolder);

        ~LockHolder()
        {
            if (var)
                --var->counter;
        }

    private:
        const ActionBlocker * var = nullptr;
    };
};

} // namespace DB
