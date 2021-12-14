#pragma once

#include <Common/LogWithPrefix.h>

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

//using namespace DB;

class ElasticThreadPool
{
public:
    using Job = std::function<void()>;
    static std::unique_ptr<ElasticThreadPool> glb_instance;
    struct Thd
    {
        explicit Thd(ElasticThreadPool * thd_pool)
            : end_syn(false)
            , state(0)
            , thd(std::make_shared<std::thread>([this, thd_pool] {
                thd_pool->pre_worker();
                thd_pool->worker(this);
            }))
        {}
        std::atomic_bool end_syn; //someone wants it end
        std::atomic_int state; // 0.idle 1.working 2.ended
        std::shared_ptr<std::thread> thd;
    };

    /// Every threads will execute pre_worker firstly when they are created.
    explicit ElasticThreadPool(
        size_t m_size,
        Job pre_worker_ = [] {},
        const DB::LogWithPrefixPtr & log_ = nullptr);

    /// Add new job.
    std::future<void> schedule(Job job);

    ~ElasticThreadPool();

    void backgroundJob();

protected:
    mutable std::mutex mutex;
    std::condition_variable has_new_job_or_shutdown;
    std::condition_variable cv_shutdown;
    size_t history_min_available_cnt = std::numeric_limits<size_t>::max();

    const size_t init_cap;
    size_t available_cnt;
    Job pre_worker;
    std::atomic<bool> shutdown = false;

    std::queue<Job> jobs;
    std::shared_ptr<std::vector<std::shared_ptr<Thd>>> threads;
    std::thread bk_thd;
    std::chrono::seconds recycle_period = std::chrono::seconds(10);
    const DB::LogWithPrefixPtr log;

    void worker(Thd * thdctx);

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
