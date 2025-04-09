// Copyright 2025 PingCAP, Inc.
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

#include <common/defines.h>

#ifdef __linux__ /// Because of futex

#include <common/types.h>

#include <atomic>

namespace DB
{

// Faster implementation of `SharedMutex` based on a pair of futexes
class TSA_CAPABILITY("SharedMutex") SharedMutex
{
public:
    SharedMutex();
    ~SharedMutex() = default;
    SharedMutex(const SharedMutex &) = delete;
    SharedMutex & operator=(const SharedMutex &) = delete;
    SharedMutex(SharedMutex &&) = delete;
    SharedMutex & operator=(SharedMutex &&) = delete;

    // Exclusive ownership
    void lock() TSA_ACQUIRE();
    bool try_lock() TSA_TRY_ACQUIRE(true); // NOLINT
    void unlock() TSA_RELEASE();

    // Shared ownership
    void lock_shared() TSA_ACQUIRE_SHARED(); // NOLINT
    bool try_lock_shared() TSA_TRY_ACQUIRE_SHARED(true); // NOLINT
    void unlock_shared() TSA_RELEASE_SHARED(); // NOLINT

private:
    static constexpr UInt64 readers = (1ull << 32ull) - 1ull; // Lower 32 bits of state
    static constexpr UInt64 writers = ~readers; // Upper 32 bits of state

    alignas(64) std::atomic<UInt64> state;
    std::atomic<UInt32> waiters;
    /// Is set while the lock is held (or is in the process of being acquired) in exclusive mode only to facilitate debugging
    std::atomic<UInt64> writer_thread_id;
};

} // namespace DB

#else

#include <absl/synchronization/mutex.h>

namespace DB
{

class TSA_CAPABILITY("SharedMutex") SharedMutex : public absl::Mutex
{
    using absl::Mutex::Mutex;

public:
    SharedMutex(const SharedMutex &) = delete;
    SharedMutex & operator=(const SharedMutex &) = delete;
    SharedMutex(SharedMutex &&) = delete;
    SharedMutex & operator=(SharedMutex &&) = delete;

    // Exclusive ownership
    void lock() TSA_ACQUIRE() { WriterLock(); }

    bool try_lock() TSA_TRY_ACQUIRE(true) { return WriterTryLock(); } // NOLINT

    void unlock() TSA_RELEASE() { WriterUnlock(); }

    // Shared ownership
    void lock_shared() TSA_ACQUIRE_SHARED() { ReaderLock(); } // NOLINT

    bool try_lock_shared() TSA_TRY_ACQUIRE_SHARED(true) { return ReaderTryLock(); } // NOLINT

    void unlock_shared() TSA_RELEASE_SHARED() { ReaderUnlock(); } // NOLINT
};

} // namespace DB

#endif
