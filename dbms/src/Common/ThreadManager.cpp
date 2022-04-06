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

#include <Common/DynamicThreadPool.h>
#include <Common/FiberPool.hpp>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <Common/wrapInvocable.h>

namespace DB
{
namespace
{
// may throw
template <typename Future>
void waitTasks(std::vector<Future> & futures)
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

class DynamicThreadManager : public ThreadManager
    , public ThreadPoolManager
{
public:
    void scheduleThenDetach(bool propagate_memory_tracker, String /*thread_name*/, ThreadManager::Job job) override
    {
        DynamicThreadPool::global_instance->scheduleRaw(propagate_memory_tracker, std::move(job));
    }

    void schedule(bool propagate_memory_tracker, String /*thread_name*/, ThreadManager::Job job) override
    {
        futures.push_back(DynamicThreadPool::global_instance->schedule(propagate_memory_tracker, std::move(job)));
    }

    void schedule(bool propagate_memory_tracker, ThreadPoolManager::Job job) override
    {
        futures.push_back(DynamicThreadPool::global_instance->schedule(propagate_memory_tracker, std::move(job)));
    }

    void wait() override
    {
        waitTasks(futures);
    }

protected:
    std::vector<std::future<void>> futures;
};

class FiberManager : public ThreadManager
    , public ThreadPoolManager
{
public:
    void scheduleThenDetach(bool /*propagate_memory_tracker*/, String /*thread_name*/, ThreadManager::Job job) override
    {
        DefaultFiberPool::submit_job(job);
    }

    void schedule(bool /*propagate_memory_tracker*/, String /*thread_name*/, ThreadManager::Job job) override
    {
        futures.push_back(DefaultFiberPool::submit_job(job).value());
    }

    void schedule(bool /*propagate_memory_tracker*/, ThreadPoolManager::Job job) override
    {
        futures.push_back(DefaultFiberPool::submit_job(job).value());
    }

    void wait() override
    {
        waitTasks(futures);
    }

protected:
    std::vector<boost::fibers::future<void>> futures;
};

class RawThreadManager : public ThreadManager
{
public:
    void schedule(bool propagate_memory_tracker, String thread_name, Job job) override
    {
        auto t = ThreadFactory::newThread(propagate_memory_tracker, std::move(thread_name), std::move(job));
        workers.push_back(std::move(t));
    }

    void scheduleThenDetach(bool propagate_memory_tracker, String thread_name, Job job) override
    {
        auto t = ThreadFactory::newThread(propagate_memory_tracker, std::move(thread_name), std::move(job));
        t.detach();
    }

    void wait() override
    {
        waitAndClear();
    }

    ~RawThreadManager()
    {
        waitAndClear();
    }

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

    void wait() override
    {
        pool.wait();
    }

protected:
    ThreadPool pool;
};
} // namespace

std::shared_ptr<ThreadManager> newThreadManager()
{
#ifdef TIFLASH_USE_FIBER
    return std::make_shared<FiberManager>;
#else
    if (DynamicThreadPool::global_instance)
        return std::make_shared<DynamicThreadManager>();
    else
        return std::make_shared<RawThreadManager>();
#endif
}

std::shared_ptr<ThreadManager> newIOThreadManager()
{
    if (DynamicThreadPool::global_instance)
        return std::make_shared<DynamicThreadManager>();
    else
        return std::make_shared<RawThreadManager>();
}

std::shared_ptr<ThreadPoolManager> newThreadPoolManager(size_t capacity)
{
#ifdef TIFLASH_USE_FIBER
    return std::make_shared<FiberManager>;
#else
    if (DynamicThreadPool::global_instance)
        return std::make_shared<DynamicThreadManager>();
    else
        return std::make_shared<FixedThreadPoolManager>(capacity);
#endif
}

} // namespace DB
