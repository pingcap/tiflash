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

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/SimpleFixedThreadPool.h>
#include <common/logger_useful.h>

namespace DB
{
SimpleFixedThreadPool::SimpleFixedThreadPool(const std::string & name_, size_t m_size_)
    : name(name_)
    , m_size(m_size_)
    , thread_mgr(newThreadManager())
{
    RUNTIME_ASSERT(m_size > 0);
}

void SimpleFixedThreadPool::schedule(ThreadManager::Job job)
{
    {
        std::unique_lock lock(mutex);
        has_free_thread.wait(lock, [this] { return active_jobs < m_size; });
        ++active_jobs;
    }
    thread_mgr->schedule(false, name, [&, job]() {
        job();
        {
            std::lock_guard lock(mutex);
            --active_jobs;
            has_free_thread.notify_one();
        }
    });
}

/// Wait for all currently active jobs to be done.
SimpleFixedThreadPool::~SimpleFixedThreadPool()
{
    {
        std::unique_lock lock(mutex);
        has_free_thread.wait(lock, [this] { return active_jobs == 0; });
    }
    try
    {
        thread_mgr->wait();
    }
    catch (...)
    {
        LOG_WARNING(Logger::get(), "exception occur in {}: {}", name, getCurrentExceptionMessage(true, true));
    }
}
} // namespace DB
