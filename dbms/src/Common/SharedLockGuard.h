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

namespace DB
{

/** SharedLockGuard provides RAII-style locking mechanism for acquiring shared ownership of the implementation
  * of the SharedLockable concept (for example SharedMutex) supplied as the
  * constructor argument. Think of it as std::lock_guard which locks shared.
  *
  * On construction it acquires shared ownership using `lock_shared` method.
  * On destruction shared ownership is released using `unlock_shared` method.
  */
template <typename Mutex>
class TSA_SCOPED_LOCKABLE SharedLockGuard
{
public:
    explicit SharedLockGuard(Mutex & mutex_) TSA_ACQUIRE_SHARED(mutex_)
        : mutex(mutex_)
    {
        mutex_.lock_shared();
    }

    ~SharedLockGuard() TSA_RELEASE() { mutex.unlock_shared(); }

private:
    Mutex & mutex;
};

} // namespace DB
