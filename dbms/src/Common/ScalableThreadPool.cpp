#include <Common/ScalableThreadPool.h>
#include <Common/setThreadName.h>

#include <exception>
#include <iostream>
#include <stdexcept>

#include "MemoryTracker.h"

std::unique_ptr<ScalableThreadPool> glb_thd_pool = std::make_unique<ScalableThreadPool>(200, [] { setThreadName("glb-thd-pool"); });

void handle_eptr(std::exception_ptr eptr) // passing by value is ok
{
    try
    {
        if (eptr)
        {
            std::rethrow_exception(eptr);
        }
    }
    catch (const std::exception & e)
    {
        std::cerr << "Caught exception \"" << e.what() << "\"\n";
    }
}

//template <typename F, typename... Args>
std::function<void()> ScalableThreadPool::newJob(std::shared_ptr<std::promise<int>> p, Job job)
{
    auto memory_tracker = current_memory_tracker;
    /// Use std::tuple to workaround the limit on the lambda's init-capture of C++17.
    /// See https://stackoverflow.com/questions/47496358/c-lambdas-how-to-capture-variadic-parameter-pack-from-the-upper-scope
    return [p, memory_tracker, job] {
        if (memory_tracker) current_memory_tracker = memory_tracker;
        try
        {
            job();
            p->set_value(0);
        }
        catch (...)
        {
            try
            {
                // store anything thrown in the promise
                p->set_exception(std::current_exception());
            }
            catch (...)
            {
            } // set_exception() may throw too
        }
        if (memory_tracker) current_memory_tracker = nullptr;
    };
}

ScalableThreadPool::ScalableThreadPool(size_t m_size, Job pre_worker_)
    : init_cap(m_size)
    , pre_worker(pre_worker_)
    , threads(std::make_shared<std::vector<std::shared_ptr<ThdCtx>>>())
    , bk_thd(std::thread([this] { backgroundJob(); }))
{
    threads->reserve(m_size);
    for (size_t i = 0; i < m_size; ++i)
    {
        threads->emplace_back(std::make_shared<ThdCtx>(this));
    }
}

std::future<int> ScalableThreadPool::schedule0(std::shared_ptr<std::promise<int>> p, Job job)
{
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (shutdown)
            return std::future<int>();
        if (active_jobs >= threads->size())
        {
            threads->emplace_back(std::make_shared<ThdCtx>(this));
        }

        jobs.push(std::move(job));
        ++active_jobs;
        max_history_active_cnt = std::max(max_history_active_cnt, active_jobs);
    }
    has_new_job_or_shutdown.notify_one();
    return p->get_future();
}

std::future<int> ScalableThreadPool::schedule(Job job)
{
    std::shared_ptr<std::promise<int>> p = std::make_shared<std::promise<int>>();
    return schedule0(p, newJob(p, job));
}

void ScalableThreadPool::wait()
{
    {
        std::unique_lock<std::mutex> lock(mutex);
        has_free_thread.wait(lock, [this] { return active_jobs == 0; });

        if (first_exception)
        {
            std::exception_ptr exception;
            std::swap(exception, first_exception);
            std::rethrow_exception(exception);
        }
    }
}

ScalableThreadPool::~ScalableThreadPool()
{
    {
        std::unique_lock<std::mutex> lock(mutex);
        shutdown = true;
    }

    has_new_job_or_shutdown.notify_all();
    cv_shutdown.notify_all();
    bk_thd.join();
    for (auto & thread_ctx : *threads)
        thread_ctx->thd->join();
}

size_t ScalableThreadPool::active() const
{
    std::unique_lock<std::mutex> lock(mutex);
    return active_jobs;
}

void ScalableThreadPool::backgroundJob()
{
    while (!shutdown)
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (cv_shutdown.wait_for(lock, std::chrono::seconds(10), [this] { return shutdown.load(); }))
            break;
        if (max_history_active_cnt > init_cap && max_history_active_cnt < threads->size() * 0.9)
        {
            int cnt_to_clean = static_cast<int>(threads->size()) - std::max(max_history_active_cnt + 10, init_cap);
            if (cnt_to_clean <= 0)
                continue;
            auto old_threads = threads;
            int old_threads_size = threads->size();
            lock.unlock();
            auto new_threads = std::make_shared<std::vector<std::shared_ptr<ThdCtx>>>();
            int cnt_cleaned = 0;
            for (auto & thd_ctx : *old_threads)
            {
                //status.end: can be removed safely
                if (thd_ctx->status == 2)
                    cnt_cleaned++;
            }
            for (auto & thd_ctx : *old_threads)
            {
                if (thd_ctx->status != 2)
                {
                    new_threads->push_back(thd_ctx);
                    if (cnt_cleaned < cnt_to_clean && !(thd_ctx->end_syn) && thd_ctx->status == 0)
                    {
                        thd_ctx->end_syn = true;
                        cnt_cleaned++;
                    }
                }
            }
            {
                std::unique_lock<std::mutex> lock2(mutex);
                threads = new_threads; //update threads
                for (size_t i = old_threads_size; i < old_threads->size(); i++)
                { //update new threads created during this loop
                    threads->push_back(old_threads->at(i));
                }
            }
        }

        max_history_active_cnt = 0;
    }
}


void ScalableThreadPool::worker(ThdCtx * thdctx)
{
    while (!thdctx->end_syn)
    {
        Job job;
        bool need_shutdown = false;
        {
            std::unique_lock<std::mutex> lock(mutex);
            has_new_job_or_shutdown.wait(lock, [this] { return shutdown || !jobs.empty(); });
            need_shutdown = shutdown;

            if (!jobs.empty())
            {
                job = std::move(jobs.front());
                jobs.pop();
                thdctx->status = 1;
            }
            else
            {
                return;
            }
        }

        if (!need_shutdown)
        {
            std::exception_ptr eptr;
            try
            {
                job();
            }
            catch (...)
            {
                std::unique_lock<std::mutex> lock(mutex);
                if (!first_exception)
                {
                    first_exception = std::current_exception();
                    eptr = std::current_exception();
                }
            }
            handle_eptr(eptr);
        }

        {
            std::unique_lock<std::mutex> lock(mutex);
            --active_jobs;
        }
        thdctx->status = 0;
        has_free_thread.notify_all();
    }
    thdctx->status = 2;
}


