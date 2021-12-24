#pragma once


#include <common/ThreadPool.h>

#include <future>
#include <vector>


namespace DB
{
class ThreadManager
{
public:
    enum Type
    {
        kElasticThreadPool = 0,
        kRawThread = 1,
        kFixedThreadPool = 2,
    };
    using Job = std::function<void()>;
    virtual ~ThreadManager() = default;
    virtual void wait() = 0;
    virtual void schedule(Job job) = 0;
    static std::shared_ptr<ThreadManager> createElasticOrRawThreadManager(bool detach_if_possible = false);
    static std::shared_ptr<ThreadManager> createElasticOrFixedThreadManager(size_t fixed_thread_pool_size);
};

class ElasticThreadManager : public ThreadManager
{
public:
    ~ElasticThreadManager() {}

    void schedule(Job job) override;

    void wait() override;

protected:
    std::vector<std::future<void>> futures;
};

class RawThreadManager : public ThreadManager
{
public:
    RawThreadManager(bool detach_if_possible_)
        : detach_if_possible(detach_if_possible_)
    {}
    ~RawThreadManager() {}

    void schedule(Job job) override;

    void wait() override;

protected:
    std::vector<std::thread> workers;
    bool detach_if_possible;
};

class FixedPoolThreadManager : public ThreadManager
{
public:
    FixedPoolThreadManager(size_t size)
        : pool(size)
    {}
    ~FixedPoolThreadManager() {}
    void schedule(Job job) override;

    void wait() override;

protected:
    ThreadPool pool;
};

} // namespace DB
