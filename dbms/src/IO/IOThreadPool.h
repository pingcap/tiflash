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

#include <Common/Exception.h>
#include <Common/UniThreadPool.h>

namespace DB
{
struct Settings;

template <typename Type>
class IOThreadPool
{
    friend void adjustThreadPoolSize(const Settings & settings, size_t logical_cores);

    static inline std::unique_ptr<ThreadPool> instance;

public:
    static void initialize(size_t max_threads, size_t max_free_threads, size_t queue_size)
    {
        RUNTIME_CHECK_MSG(!instance, "IO thread pool is initialized twice");
        instance
            = std::make_unique<ThreadPool>(max_threads, max_free_threads, queue_size, false /*shutdown_on_exception*/);
        GlobalThreadPool::instance().registerFinalizer([] { instance.reset(); });
    }

    static ThreadPool & get()
    {
        RUNTIME_CHECK_MSG(instance, "IO thread pool is not initialized");
        return *instance;
    }

    static void shutdown() noexcept { instance.reset(); }
};

} // namespace DB
