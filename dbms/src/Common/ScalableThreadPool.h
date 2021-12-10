#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
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
        std::shared_ptr<std::thread> thd;
        std::atomic_bool end_syn; //someone wants it end
        std::atomic_int status; // 0.idle 1.working 2.ended
    };

    /// Size is constant, all threads are created immediately.
    /// Every threads will execute pre_worker firstly when they are created.
    explicit ScalableThreadPool(
        size_t m_size,
        Job pre_worker_ = [] {});

    /// Add new job. Locks until free thread in pool become available or exception in one of threads was thrown.
    /// If an exception in some thread was thrown, method silently returns, and exception will be rethrown only on call to 'wait' function.
    void schedule(Job job);

    void scheduleWithMemTracker(Job job);

    /// Wait for all currently active jobs to be done.
    /// You may call schedule and wait many times in arbitary order.
    /// If any thread was throw an exception, first exception will be rethrown from this method,
    ///  and exception will be cleared.
    void wait();

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

    template <typename F, typename... Args>
    std::function<void()> newJobWithMemTracker(F && f, Args &&... args);
};

extern std::unique_ptr<ScalableThreadPool> glb_thd_pool;