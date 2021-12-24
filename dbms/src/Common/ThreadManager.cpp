#include <Common/ElasticThreadPool.h>
#include <Common/ThreadFactory.h>
#include <Common/ThreadManager.h>

namespace DB
{
std::shared_ptr<ThreadManager> ThreadManager::createElasticOrRawThreadManager(bool detach_if_possible)
{
    if (ElasticThreadPool::glb_instance)
        return std::make_shared<ElasticThreadManager>();
    else
        return std::make_shared<RawThreadManager>(detach_if_possible);
}

std::shared_ptr<ThreadManager> ThreadManager::createElasticOrFixedThreadManager(size_t fixed_thread_pool_size)
{
    if (ElasticThreadPool::glb_instance)
        return std::make_shared<ElasticThreadManager>();
    else
        return std::make_shared<FixedPoolThreadManager>(fixed_thread_pool_size);
}

void ElasticThreadManager::schedule(Job job)
{
    futures.emplace_back(ElasticThreadPool::glb_instance->schedule(job));
}

void ElasticThreadManager::wait()
{
    waitTasks(futures);
}

void RawThreadManager::schedule(Job job)
{
    workers.emplace_back(ThreadFactory(true).newThread(job));
    if (detach_if_possible)
        workers.back().detach();
}

void RawThreadManager::wait()
{
    if (detach_if_possible)
        return;
    for (auto & worker : workers)
        worker.join();
}

void FixedPoolThreadManager::schedule(Job job)
{
    pool.schedule(job);
}

void FixedPoolThreadManager::wait()
{
    pool.wait();
}

} // namespace DB