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

#include <Common/VariantOp.h>

#include <mutex>
#include <shared_mutex>

namespace DB
{
using SteadyClock = std::chrono::steady_clock;
static constexpr size_t CPU_CACHE_LINE_SIZE = 64;

template <typename Base, size_t alignment>
struct AlignedStruct
{
    template <typename... Args>
    explicit AlignedStruct(Args &&... args)
        : inner{std::forward<Args>(args)...}
    {}

    Base & base() { return inner; }
    const Base & base() const { return inner; }
    Base * operator->() { return &inner; }
    const Base * operator->() const { return &inner; }
    Base & operator*() { return inner; }
    const Base & operator*() const { return inner; }

private:
    // Wrapped with struct to guarantee that it is aligned to `alignment`
    // DO NOT need padding byte
    alignas(alignment) Base inner;
};

class MutexLockWrap
{
public:
    using Mutex = std::mutex;

    std::lock_guard<Mutex> genLockGuard() const;

    std::unique_lock<Mutex> tryToLock() const;

    std::unique_lock<Mutex> genUniqueLock() const;

private:
    mutable AlignedStruct<Mutex, CPU_CACHE_LINE_SIZE> mutex;
};

class SharedMutexLockWrap
{
public:
    using Mutex = std::shared_mutex;

    std::shared_lock<Mutex> genSharedLock() const { return std::shared_lock(*mutex); }

    std::unique_lock<Mutex> genUniqueLock() const { return std::unique_lock(*mutex); }

private:
    mutable AlignedStruct<Mutex, CPU_CACHE_LINE_SIZE> mutex;
};

class MutexCondVarWrap : public MutexLockWrap
{
public:
    using CondVar = std::condition_variable;

    CondVar & condVar() const { return *cv; }

private:
    mutable AlignedStruct<CondVar, CPU_CACHE_LINE_SIZE> cv;
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
