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

#include <Common/DynamicThreadPool.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <Common/wrapInvocable.h>
#include <common/ThreadPool.h>

namespace DB
{
namespace
{
// may throw
void waitTasks(std::vector<std::future<void>> & futures)
{
    std::exception_ptr first_exception;
    for (auto & future : futures)
    {
        // ensure all futures finished
        try
        {
            future.get();
        }
        catch (...)
        {
            if (!first_exception)
                first_exception = std::current_exception();
        }
    }
    if (first_exception)
        std::rethrow_exception(first_exception);
}

class DynamicThreadManager
    : public ThreadManager
    , public ThreadPoolManager
{
public:
    void scheduleThenDetach(bool propagate_memory_tracker, std::string /*thread_name*/, ThreadManager::Job job) override
    {
        DynamicThreadPool::global_instance->scheduleRaw(propagate_memory_tracker, std::move(job));
    }

    void schedule(bool propagate_memory_tracker, std::string /*thread_name*/, ThreadManager::Job job) override
    {
        futures.push_back(DynamicThreadPool::global_instance->schedule(propagate_memory_tracker, std::move(job)));
    }

    void schedule(bool propagate_memory_tracker, ThreadPoolManager::Job job) override
    {
        futures.push_back(DynamicThreadPool::global_instance->schedule(propagate_memory_tracker, std::move(job)));
    }

    void wait() override { waitTasks(futures); }

protected:
    std::vector<std::future<void>> futures;
};

class RawThreadManager : public ThreadManager
{
public:
    void schedule(bool propagate_memory_tracker, std::string thread_name, Job job) override
    {
        auto t = ThreadFactory::newThread(propagate_memory_tracker, std::move(thread_name), std::move(job));
        workers.push_back(std::move(t));
    }

    void scheduleThenDetach(bool propagate_memory_tracker, std::string thread_name, Job job) override
    {
        auto t = ThreadFactory::newThread(propagate_memory_tracker, std::move(thread_name), std::move(job));
        t.detach();
    }

    void wait() override { waitAndClear(); }

    ~RawThreadManager() override { waitAndClear(); }

protected:
    void waitAndClear()
    {
        for (auto & worker : workers)
            worker.join();
        workers.clear();
    }

    std::vector<std::thread> workers;
};

class FixedThreadPoolManager : public ThreadPoolManager
{
public:
    explicit FixedThreadPoolManager(size_t size)
        : pool(size)
    {}

    void schedule(bool propagate_memory_tracker, Job job) override
    {
        pool.schedule(wrapInvocable(propagate_memory_tracker, std::move(job)));
    }

    void wait() override { pool.wait(); }

protected:
    legacy::ThreadPool pool;
};
} // namespace

std::shared_ptr<ThreadManager> newThreadManager()
{
    if (DynamicThreadPool::global_instance)
        return std::make_shared<DynamicThreadManager>();
    else
        return std::make_shared<RawThreadManager>();
}

std::shared_ptr<ThreadPoolManager> newThreadPoolManager(size_t capacity)
{
    if (DynamicThreadPool::global_instance)
        return std::make_shared<DynamicThreadManager>();
    else
        return std::make_shared<FixedThreadPoolManager>(capacity);
}

} // namespace DB
