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

#include <Poco/Logger.h>
#include <common/ThreadPool.h>
#include <common/logger_useful.h>

#include <iostream>


static Poco::Logger * getLogger()
{
    static Poco::Logger * logger = &Poco::Logger::get("ThreadPool");
    return logger;
}

namespace legacy
{

ThreadPool::ThreadPool(size_t m_size, Job pre_worker)
    : m_size(m_size)
{
    threads.reserve(m_size);
    try
    {
        for (size_t i = 0; i < m_size; ++i)
            threads.emplace_back([this, pre_worker] {
                pre_worker();
                worker();
            });
    }
    catch (...)
    {
        LOG_ERROR(getLogger(), "ThreadPool failed to allocate threads.");
        finalize();
        throw;
    }
}

void ThreadPool::finalize()
{
    {
        std::unique_lock lock(mutex);
        shutdown = true;
    }

    has_new_job_or_shutdown.notify_all();

    for (auto & thread : threads)
        thread.join();
}

void ThreadPool::schedule(Job job)
{
    {
        std::unique_lock lock(mutex);
        has_free_thread.wait(lock, [this] { return active_jobs < m_size || shutdown; });
        if (shutdown)
            return;

        jobs.push(std::move(job));
        ++active_jobs;
    }
    has_new_job_or_shutdown.notify_one();
}

void ThreadPool::wait()
{
    {
        std::unique_lock lock(mutex);
        has_free_thread.wait(lock, [this] { return active_jobs == 0; });

        if (first_exception)
        {
            std::exception_ptr exception;
            std::swap(exception, first_exception);
            std::rethrow_exception(exception);
        }
    }
}

ThreadPool::~ThreadPool()
{
    finalize();
}

size_t ThreadPool::active() const
{
    std::unique_lock lock(mutex);
    return active_jobs;
}


void ThreadPool::worker()
{
    while (true)
    {
        Job job;
        bool need_shutdown = false;

        {
            std::unique_lock lock(mutex);
            has_new_job_or_shutdown.wait(lock, [this] { return shutdown || !jobs.empty(); });
            need_shutdown = shutdown;

            if (!jobs.empty())
            {
                job = std::move(jobs.front());
                jobs.pop();
            }
            else
            {
                return;
            }
        }

        if (!need_shutdown)
        {
            try
            {
                job();
            }
            catch (...)
            {
                {
                    std::unique_lock lock(mutex);
                    if (!first_exception)
                        first_exception = std::current_exception();
                    shutdown = true;
                    --active_jobs;
                }
                has_free_thread.notify_all();
                has_new_job_or_shutdown.notify_all();
                return;
            }
        }

        {
            std::unique_lock lock(mutex);
            --active_jobs;
        }

        has_free_thread.notify_all();
    }
}

} // namespace legacy
