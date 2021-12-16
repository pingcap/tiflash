#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "Poco/Logger.h"

class ElasticThreadPool
{
public:
    using Job = std::function<void()>;
    static std::unique_ptr<ElasticThreadPool> glb_instance;
    struct Worker
    {
        explicit Worker(ElasticThreadPool * thd_pool)
            : end_syn(false)
            , state(0)
            , thd(std::make_shared<std::thread>([this, thd_pool] {
                thd_pool->pre_worker();
                thd_pool->work(this);
            }))
        {}
        std::atomic_bool end_syn; //someone wants it end
        std::atomic_int state; // 0.idle 1.working 2.ended
        std::shared_ptr<std::thread> thd;
    };

    /// Every threads will execute pre_worker firstly when they are created.
    explicit ElasticThreadPool(
        size_t m_size,
        std::chrono::milliseconds recycle_period_ = std::chrono::milliseconds(10000),
        size_t idle_buffer_size_ = 50,
        Job pre_worker_ = [] {});

    /// Add new job.
    std::future<void> schedule(Job job);

    ~ElasticThreadPool();

    bool shrink(std::chrono::milliseconds wait_interval);

    size_t getAvailableCnt() const;

    size_t getAliveCnt() const;

    size_t getIdleBufferSize() const;

protected:
    mutable std::mutex mutex;
    std::condition_variable has_new_job_or_shutdown;
    std::condition_variable cv_shutdown;
    size_t history_min_available_cnt = std::numeric_limits<size_t>::max();

    const size_t init_cap;
    size_t available_cnt, alive_cnt;
    std::chrono::milliseconds recycle_period;
    size_t idle_buffer_size;
    Job pre_worker;
    std::atomic<bool> shutdown = false;

    std::queue<Job> jobs;
    std::shared_ptr<std::vector<std::shared_ptr<Worker>>> threads;
    std::thread bk_thd;

    void backgroundJob();

    void work(Worker * thdctx);

    std::future<void> schedule0(std::shared_ptr<std::promise<void>> p, Job job);

    std::function<void()> newJob(std::shared_ptr<std::promise<void>> p, Job job);
};

inline void waitTask(std::future<void> & f)
{
    try
    {
        if (f.valid())
            f.get();
    }
    catch (const std::exception & e)
    {
        std::cerr << "Caught exception \"" << e.what() << "\"\n";
    }
}

inline void waitTasks(std::vector<std::future<void>> & futures)
{
    for (auto & f : futures)
    {
        waitTask(f);
    }
}
