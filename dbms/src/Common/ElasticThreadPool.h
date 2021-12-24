#pragma once

#include <common/logger_useful.h>

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

class ElasticThreadPool
{
public:
    using Job = std::function<void()>;
    static std::unique_ptr<ElasticThreadPool> glb_instance;
    struct Worker
    {
        enum class State
        {
            Idle = 0,
            Working,
            Ended,
        };

        explicit Worker(ElasticThreadPool * thd_pool)
            : end_syn(false)
            , state(State::Idle)
            , thd(std::make_shared<std::thread>([this, thd_pool] {
                thd_pool->pre_worker();
                thd_pool->work(this);
            }))
        {}

        std::atomic_bool end_syn; // someone wants it end
        std::atomic<State> state; // 0.idle 1.working 2.ended
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

    /// get available_cnt
    size_t getAvailableCnt() const;

    /// get alive_cnt
    size_t getAliveCnt() const;

    /// get idle_buffer_size
    size_t getIdleBufferSize() const;

protected:
    mutable std::mutex mutex;
    std::condition_variable has_new_job_or_shutdown;
    std::condition_variable cv_shutdown;
    /// min available workers cnt in history
    size_t history_min_available_cnt = std::numeric_limits<size_t>::max();

    /// init threads size when thread pool is created
    const size_t init_cap;
    /// count of workers which is idle
    size_t available_cnt;
    /// count of workers which is active
    size_t alive_cnt;
    /// period of shink shink threads
    std::chrono::milliseconds recycle_period;
    /// the count of threads we want to preserve, even if the thread is idle
    size_t idle_buffer_size;
    /// init job for the worker, invoked when worker is created
    Job pre_worker;
    bool shutdown = false;

    /// jobs to schedule
    std::queue<Job> jobs;
    /// workers
    std::shared_ptr<std::vector<std::shared_ptr<Worker>>> threads;
    /// background task for shink threads size
    std::thread bk_thd;
    Poco::Logger * log;

    /// try to shrink threads size, invoked by backgroundJob()
    bool shrink(std::chrono::milliseconds wait_interval);

    /// loop for background task, to periodically remove excessive threads when load keeps low.
    void backgroundJob();

    /// loop for the worker
    void work(Worker * thdctx);

    /// do the real schedule thing, judged and invoked by schedule()
    std::future<void> schedule0(std::shared_ptr<std::promise<void>> p, Job job);

    /// wrap the job, and the wrapped one will be scheduled and used
    Job newJob(std::shared_ptr<std::promise<void>> p, Job job);
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
