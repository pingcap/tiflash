#include <Common/DynamicThreadPool.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>
#include <Common/wrapInvocable.h>

namespace DB
{
namespace
{
// may throw
void waitTasks(std::vector<std::future<void>> & futures)
{
    for (auto & future : futures)
        future.get();
}

class DynamicThreadManager : public ThreadManager
    , public ThreadPoolManager
{
public:
    void scheduleThenDetach(bool propagate_memory_tracker, String /*thread_name*/, ThreadManager::Job job) override
    {
        DynamicThreadPool::global_instance->schedule(propagate_memory_tracker, job);
    }

    void schedule(bool propagate_memory_tracker, String /*thread_name*/, ThreadManager::Job job) override
    {
        futures.push_back(DynamicThreadPool::global_instance->schedule(propagate_memory_tracker, job));
    }

    void schedule(bool propagate_memory_tracker, ThreadPoolManager::Job job) override
    {
        futures.push_back(DynamicThreadPool::global_instance->schedule(propagate_memory_tracker, job));
    }

    void wait() override
    {
        waitTasks(futures);
    }

protected:
    std::vector<std::future<void>> futures;
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
