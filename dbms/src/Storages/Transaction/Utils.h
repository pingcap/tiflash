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

#include <Common/VariantOp.h>

#include <mutex>
#include <shared_mutex>

namespace DB
{
using SteadyClock = std::chrono::steady_clock;
static constexpr size_t CPU_CACHE_LINE_SIZE = 64;

class MutexLockWrap
{
public:
    using Mutex = std::mutex;

protected:
    std::lock_guard<Mutex> genLockGuard() const
    {
        return std::lock_guard(mutex());
    }

    std::unique_lock<Mutex> tryToLock() const
    {
        return std::unique_lock(mutex(), std::try_to_lock);
    }

    std::unique_lock<Mutex> genUniqueLock() const
    {
        return std::unique_lock(mutex());
    }

private:
    Mutex & mutex() const
    {
        return impl;
    }
    struct alignas(CPU_CACHE_LINE_SIZE) Impl : Mutex
    {
    };
    mutable Impl impl;
};

class SharedMutexLockWrap
{
public:
    using Mutex = std::shared_mutex;

protected:
    std::shared_lock<Mutex> genSharedLock() const
    {
        return std::shared_lock(mutex());
    }

    std::unique_lock<Mutex> genUniqueLock() const
    {
        return std::unique_lock(mutex());
    }

private:
    Mutex & mutex() const
    {
        return impl;
    }
    struct alignas(CPU_CACHE_LINE_SIZE) Impl : Mutex
    {
    };
    mutable Impl impl;
};

class MutexCondVarWrap : public MutexLockWrap
{
public:
    using CondVar = std::condition_variable;

protected:
    CondVar & condVar() const { return cv(); }

private:
    CondVar & cv() const
    {
        return impl;
    }
    struct alignas(CPU_CACHE_LINE_SIZE) Impl : CondVar
    {
    };
    mutable Impl impl;
};


struct AsyncNotifier
{
    enum class Status
    {
        Timeout,
        Normal,
    };
    virtual Status blockedWaitFor(const std::chrono::milliseconds & duration)
    {
        return blockedWaitUtil(SteadyClock::now() + duration);
    }
    virtual Status blockedWaitUtil(const SteadyClock::time_point &) = 0;
    virtual void wake() = 0;
    virtual ~AsyncNotifier() = default;
};
} // namespace DB
