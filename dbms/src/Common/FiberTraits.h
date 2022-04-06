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

#include <Common/FiberRWLock.h>

#include <condition_variable>
#include <mutex>
#include <shared_mutex>

namespace DB
{
struct FiberTraits
{
#ifdef TIFLASH_USE_FIBER
    using Mutex = boost::fibers::mutex;
    using ConditionVariable = boost::fibers::condition_variable;
    using SharedMutex = FiberRWLock;
    template <typename T>
    using PackagedTask = boost::fibers::packaged_task<T>;
    template <typename T>
    using Promise = boost::fibers::promise<T>;
    template <typename T>
    using SharedFuture = boost::fibers::shared_future<T>;
#else
    using Mutex = std::mutex;
    using ConditionVariable = std::condition_variable;
    using SharedMutex = std::shared_mutex;
    template <typename T>
    using PackagedTask = std::packaged_task<T>;
    template <typename T>
    using Promise = std::promise<T>;
    template <typename T>
    using SharedFuture = std::shared_future<T>;
#endif
};

} // namespace DB
