#include <Common/ElasticThreadPool.h>
#include <Common/setThreadName.h>

#include <exception>
#include <iostream>

#include "MemoryTracker.h"

std::unique_ptr<ElasticThreadPool> ElasticThreadPool::glb_instance = std::make_unique<ElasticThreadPool>(200, [] { setThreadName("glb-thd-pool"); });

std::function<void()> ElasticThreadPool::newJob(std::shared_ptr<std::promise<void>> p, Job job)
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

ElasticThreadPool::ElasticThreadPool(size_t m_size, Job pre_worker_, const DB::LogWithPrefixPtr & log_)
    : init_cap(m_size)
    , available_cnt(m_size)
    , pre_worker(pre_worker_)
    , threads(std::make_shared<std::vector<std::shared_ptr<Thd>>>())
    , bk_thd(std::thread([this] { backgroundJob(); }))
    , log(getLogWithPrefix(log_, "ElasticThreadPool"))
{
    threads->reserve(m_size);
    for (size_t i = 0; i < m_size; ++i)
    {
        threads->emplace_back(std::make_shared<Thd>(this));
    }
}

std::future<void> ElasticThreadPool::schedule0(std::shared_ptr<std::promise<void>> p, Job job)
{
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (shutdown)
            return std::future<void>();
        if (available_cnt <= 0)
        {
            available_cnt++;
            threads->emplace_back(std::make_shared<Thd>(this));
        }

        jobs.push(std::move(job));
        available_cnt--;
        history_min_available_cnt = std::min(history_min_available_cnt, available_cnt);
    }
    has_new_job_or_shutdown.notify_one();
    return p->get_future();
}

std::future<void> ElasticThreadPool::schedule(Job job)
{
    std::shared_ptr<std::promise<void>> p = std::make_shared<std::promise<void>>();
    return schedule0(p, newJob(p, job));
}

ElasticThreadPool::~ElasticThreadPool()
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

void ElasticThreadPool::backgroundJob()
{
    while (!shutdown)
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (cv_shutdown.wait_for(lock, recycle_period, [this] { return shutdown.load(); }))
            break;
        size_t idle_buffer_cnt = 50;
        LOG_INFO(log, "[ElasticThreadPool] loop_start, history_min_available_cnt: " << history_min_available_cnt << " available_cnt: " << available_cnt << " thread_list_size: " << threads->size());
        if (history_min_available_cnt != std::numeric_limits<size_t>::max() && history_min_available_cnt > idle_buffer_cnt)
        {
            int cnt_to_clean = static_cast<int>(history_min_available_cnt) - idle_buffer_cnt;
            if (cnt_to_clean <= 0)
            {
                continue;
            }
            auto old_threads = threads;
            int old_threads_size = threads->size();
            lock.unlock();
            auto new_threads = std::make_shared<std::vector<std::shared_ptr<Thd>>>();
            int cnt_cleaned = 0;
            for (auto & thd_ctx : *old_threads)
            {
                if (thd_ctx->state != 2)
                {
                    new_threads->push_back(thd_ctx);
                    if (cnt_cleaned < cnt_to_clean && !(thd_ctx->end_syn) && thd_ctx->state == 0)
                    {
                        thd_ctx->end_syn = true;
                        cnt_cleaned++;
                    }
                }
                else
                { //state.end: can be removed safely
                    thd_ctx->thd->join();
                }
            }
            int new_thds = 0;
            {
                std::unique_lock<std::mutex> lock2(mutex);
                for (size_t i = old_threads_size; i < old_threads->size(); i++)
                { //update new threads created during this loop
                    new_threads->push_back(old_threads->at(i));
                    new_thds++;
                }
                threads = new_threads; //update threads
                history_min_available_cnt = std::numeric_limits<size_t>::max();
            }
            LOG_INFO(log, "[ElasticThreadPool] loop_end, cnt_to_clean: " << cnt_to_clean << " cnt_cleaned: " << cnt_cleaned << " new_thds: " << new_thds);
        }
    }
}

void ElasticThreadPool::worker(Thd * thdctx)
{
    while (!thdctx->end_syn)
    {
        Job job;
        bool need_shutdown = false;
        {
            std::unique_lock<std::mutex> lock(mutex);
            has_new_job_or_shutdown.wait_for(lock, recycle_period, [this] { return shutdown.load() || !jobs.empty(); });
            if (thdctx->end_syn)
                break;
            need_shutdown = shutdown;

            if (!jobs.empty())
            {
                job = std::move(jobs.front());
                jobs.pop();
                thdctx->state = 1;
            }
            else
            {
                break;
            }
        }

        if (!need_shutdown)
            job();

        {
            std::unique_lock<std::mutex> lock(mutex);
            available_cnt++;
            history_min_available_cnt = std::min(history_min_available_cnt, available_cnt);
        }

        thdctx->state = 0;
    }
    {
        std::unique_lock<std::mutex> lock(mutex);
        available_cnt--;
        history_min_available_cnt = std::min(history_min_available_cnt, available_cnt);
    }
    thdctx->state = 2;
}
