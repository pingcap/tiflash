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

#include <Common/ThreadManager.h>

#include <condition_variable>
#include <mutex>

namespace DB
{
class SimpleFixedThreadPool
{
public:
    /// Size is constant
    explicit SimpleFixedThreadPool(const std::string & name_, size_t m_size_)
        : name(name_)
        , m_size(m_size_)
        , thread_mgr(newThreadManager())
    {}

    /// Add new job. Locks until free thread in pool become available.
    void schedule(ThreadManager::Job job);

    /// Wait for all currently active jobs to be done.
    ~SimpleFixedThreadPool();

    size_t size() const { return m_size; }

    /// Returns number of active jobs.
    size_t active() const
    {
        std::unique_lock lock(mutex);
        return active_jobs;
    }

private:
    mutable std::mutex mutex;
    std::condition_variable has_free_thread;

    const std::string name;
    const size_t m_size;
    std::shared_ptr<ThreadManager> thread_mgr;

    size_t active_jobs = 0;
};
} // namespace DB
