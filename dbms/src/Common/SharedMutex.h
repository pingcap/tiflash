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

#include <absl/synchronization/mutex.h>
#include <common/defines.h>

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
