#include <Common/ScalableThreadPool.h>
#include <Common/setThreadName.h>

#include <exception>
#include <iostream>

#include "MemoryTracker.h"

std::unique_ptr<ScalableThreadPool> ScalableThreadPool::glb_instance = std::make_unique<ScalableThreadPool>(200, [] { setThreadName("glb-thd-pool"); });

std::function<void()> ScalableThreadPool::newJob(std::shared_ptr<std::promise<void>> p, Job job)
{
    auto memory_tracker = current_memory_tracker;
    return [p, memory_tracker, job] {
        current_memory_tracker = memory_tracker;
        try
        {
            job();
            p->set_value();
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
        current_memory_tracker = nullptr;
    };
}

ScalableThreadPool::ScalableThreadPool(size_t m_size, Job pre_worker_)
    : init_cap(m_size)
    , pre_worker(pre_worker_)
    , threads(std::make_shared<std::vector<std::shared_ptr<Thd>>>())
    , bk_thd(std::thread([this] { backgroundJob(); }))
{
    threads->reserve(m_size);
    for (size_t i = 0; i < m_size; ++i)
    {
        threads->emplace_back(std::make_shared<Thd>(this));
    }
}

std::future<void> ScalableThreadPool::schedule0(std::shared_ptr<std::promise<void>> p, Job job)
{
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (shutdown)
            return std::future<void>();
        if (wait_cnt <= 0)
        {
            threads->emplace_back(std::make_shared<Thd>(this));
        }

        jobs.push(std::move(job));
        min_history_wait_cnt = std::min(min_history_wait_cnt, wait_cnt);
    }
    has_new_job_or_shutdown.notify_one();
    return p->get_future();
}

std::future<void> ScalableThreadPool::schedule(Job job)
{
    std::shared_ptr<std::promise<void>> p = std::make_shared<std::promise<void>>();
    return schedule0(p, newJob(p, job));
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

void ScalableThreadPool::backgroundJob()
{
    while (!shutdown)
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (cv_shutdown.wait_for(lock, std::chrono::seconds(10), [this] { return shutdown.load(); }))
            break;
        size_t idle_buffer_cnt = 50;
        if (min_history_wait_cnt != std::numeric_limits<size_t>::max() && min_history_wait_cnt > idle_buffer_cnt)
        {
            int cnt_to_clean = static_cast<int>(min_history_wait_cnt) - idle_buffer_cnt;
            if (cnt_to_clean <= 0)
                continue;
            auto old_threads = threads;
            int old_threads_size = threads->size();
            lock.unlock();
            auto new_threads = std::make_shared<std::vector<std::shared_ptr<Thd>>>();
            int cnt_cleaned = 0;
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
                } else { //status.end: can be removed safely
                    thd_ctx->thd->join();
                }
            }
            {
                std::unique_lock<std::mutex> lock2(mutex);
                for (size_t i = old_threads_size; i < old_threads->size(); i++)
                { //update new threads created during this loop
                    new_threads->push_back(old_threads->at(i));
                }
                threads = new_threads; //update threads
            }
        }

        min_history_wait_cnt = std::numeric_limits<size_t>::max();
    }
}

void ScalableThreadPool::worker(Thd * thdctx)
{
    while (!thdctx->end_syn)
    {
        Job job;
        bool need_shutdown = false;
        {
            std::unique_lock<std::mutex> lock(mutex);
            wait_cnt++;
            min_history_wait_cnt = std::min(min_history_wait_cnt, wait_cnt);
            has_new_job_or_shutdown.wait(lock, [this] { return shutdown || !jobs.empty(); });
            wait_cnt--;
            min_history_wait_cnt = std::min(min_history_wait_cnt, wait_cnt);
            need_shutdown = shutdown;

            if (!jobs.empty())
            {
                job = std::move(jobs.front());
                jobs.pop();
                thdctx->status = 1;
            }
            else
            {
                break;
            }
        }

        if (!need_shutdown)
            job();

        thdctx->status = 0;
    }
    thdctx->status = 2;
}
