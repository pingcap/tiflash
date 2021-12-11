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


class ScalableThreadPool
{
public:
    using Job = std::function<void()>;

    struct ThdCtx
    {
        ThdCtx()
            : end_syn(false)
            , status(0)
        {}
        ThdCtx(ScalableThreadPool * thd_pool)
            : end_syn(false)
            , status(0)
            , thd(std::make_shared<std::thread>([this, thd_pool] {
                thd_pool->pre_worker();
                thd_pool->worker(this);
            }))
        {}
        std::atomic_bool end_syn; //someone wants it end
        std::atomic_int status; // 0.idle 1.working 2.ended
        std::shared_ptr<std::thread> thd;
    };

    /// Size is constant, all threads are created immediately.
    /// Every threads will execute pre_worker firstly when they are created.
    explicit ScalableThreadPool(
        size_t m_size,
        Job pre_worker_ = [] {});

    /// Add new job. Locks until free thread in pool become available or exception in one of threads was thrown.
    /// If an exception in some thread was thrown, method silently returns, and exception will be rethrown only on call to 'wait' function.
    std::future<int> schedule(Job job);

    /// Waits for all threads. Doesn't rethrow exceptions (use 'wait' method to rethrow exceptions).
    /// You should not destroy object while calling schedule or wait methods from another threads.
    ~ScalableThreadPool();

    size_t size() const { return init_cap; }

    /// Returns number of active jobs.
    size_t active() const;

    void backgroundJob();

protected:
    mutable std::mutex mutex;
    std::condition_variable has_free_thread;
    std::condition_variable has_new_job_or_shutdown;
    std::condition_variable cv_shutdown;
    size_t max_history_active_cnt = 0;

    const size_t init_cap;
    Job pre_worker;
    size_t active_jobs = 0;
    std::atomic<bool> shutdown = false;

    std::queue<Job> jobs;
    std::shared_ptr<std::vector<std::shared_ptr<ThdCtx>>> threads;
    std::exception_ptr first_exception;
    std::thread bk_thd;


    void worker(ThdCtx *thdctx);

    /// Add new job. Locks until free thread in pool become available or exception in one of threads was thrown.
    /// If an exception in some thread was thrown, method silently returns, and exception will be rethrown only on call to 'wait' function.
    std::future<int> schedule0(std::shared_ptr<std::promise<int>> p, Job job);

//    template <typename F, typename... Args>
    std::function<void()> newJob(std::shared_ptr<std::promise<int>> p, Job job);
};

static void waitTask(std::future<int> & f)
{
    try
    {
        if (f.valid()) f.get();
    }
    catch (const std::exception & e)
    {
        std::cerr << "Caught exception \"" << e.what() << "\"\n";
    }
}

[[maybe_unused]] static void waitTasks(std::vector<std::future<int>> &futures) {
    for(auto &f: futures) {
        waitTask(f);
    }
}

extern std::unique_ptr<ScalableThreadPool> glb_thd_pool;