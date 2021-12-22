#include <Common/ElasticThreadPool.h>
#include <Common/MemoryTracker.h>
#include <Common/setThreadName.h>

#include <exception>
#include <iostream>


std::unique_ptr<ElasticThreadPool> ElasticThreadPool::glb_instance;

ElasticThreadPool::Job ElasticThreadPool::newJob(std::shared_ptr<std::promise<void>> p, Job job)
{
    auto memory_tracker = current_memory_tracker;
    return [&, p, memory_tracker, job] {
        current_memory_tracker = memory_tracker;
        try
        {
            job();
            {
                std::unique_lock<std::mutex> lock(mutex);
                available_cnt++;
                history_min_available_cnt = std::min(history_min_available_cnt, available_cnt);
            }
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

ElasticThreadPool::ElasticThreadPool(size_t m_size, std::chrono::milliseconds recycle_period_, size_t idle_buffer_size_, Job pre_worker_)
    : init_cap(m_size)
    , available_cnt(m_size)
    , alive_cnt(m_size)
    , recycle_period(recycle_period_)
    , idle_buffer_size(idle_buffer_size_)
    , pre_worker(pre_worker_)
    , threads(std::make_shared<std::vector<std::shared_ptr<Worker>>>())
    , bk_thd(std::thread([this] { backgroundJob(); }))
{
    threads->reserve(m_size);
    for (size_t i = 0; i < m_size; ++i)
    {
        threads->emplace_back(std::make_shared<Worker>(this));
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
            alive_cnt++;
            threads->emplace_back(std::make_shared<Worker>(this));
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

bool ElasticThreadPool::shrink(std::chrono::milliseconds wait_interval)
{
    std::unique_lock<std::mutex> lock(mutex);
    if (cv_shutdown.wait_for(lock, wait_interval, [this] { return shutdown.load(); }))
        return false;

    size_t cur_min_available_cnt = history_min_available_cnt;
    history_min_available_cnt = std::numeric_limits<size_t>::max();
//    std::cerr << "[ElasticThreadPool] loop_start, history_min_available_cnt: " << cur_min_available_cnt << " available_cnt: " << available_cnt << " alive_cnt: " << alive_cnt << " thread_list_size: " << threads->size() << std::endl;
    if (cur_min_available_cnt > idle_buffer_size)
    {
        size_t old_threads_size = threads->size();
        int max_cnt_to_clean = alive_cnt - init_cap;
        int cnt_to_clean = std::min(max_cnt_to_clean, static_cast<int>(cur_min_available_cnt == std::numeric_limits<size_t>::max() ? available_cnt : cur_min_available_cnt) - idle_buffer_size);
        if (cnt_to_clean <= 0)
        {
            if (old_threads_size == alive_cnt)
                return true;
            cnt_to_clean = 0;
        }
        std::vector<std::shared_ptr<Worker>> old_threads = *threads;
        lock.unlock();
        auto new_threads = std::make_shared<std::vector<std::shared_ptr<Worker>>>();
        int cnt_cleaned = 0;
        for (auto & thd_ctx : old_threads)
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
            {
                thd_ctx->thd->join(); //state.end: can be removed safely
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
        }
//        std::cerr << "[ElasticThreadPool] loop_end, cnt_to_clean: " << cnt_to_clean << " cnt_cleaned: " << cnt_cleaned << " new_thds: " << new_thds << std::endl;
    }
    return true;
}

void ElasticThreadPool::backgroundJob()
{
    while (!shutdown)
    {
        if (!shrink(recycle_period))
            break;
    }
}

void ElasticThreadPool::work(Worker * thdctx)
{
    while (!thdctx->end_syn)
    {
        Job job;
        {
            std::unique_lock<std::mutex> lock(mutex);
            has_new_job_or_shutdown.wait_for(lock, recycle_period, [this] { return shutdown.load() || !jobs.empty(); });
            if (thdctx->end_syn)
                break;

            if (!jobs.empty())
            {
                job = std::move(jobs.front());
                jobs.pop();
                thdctx->state = 1;
            }
            else if (shutdown.load())
                break;
            else
                continue;
        }

        job();
        thdctx->state = 0;
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        available_cnt--;
        alive_cnt--;
        history_min_available_cnt = std::min(history_min_available_cnt, available_cnt);
    }
    //    std::cerr << "[ElasticThreadPool] work_end, getAvailableCnt(): " << getAvailableCnt() << std::endl;
    thdctx->state = 2;
}

size_t ElasticThreadPool::getAvailableCnt() const
{
    std::unique_lock<std::mutex> lock(mutex);
    return available_cnt;
}

size_t ElasticThreadPool::getAliveCnt() const
{
    std::unique_lock<std::mutex> lock(mutex);
    return alive_cnt;
}

size_t ElasticThreadPool::getIdleBufferSize() const
{
    return idle_buffer_size;
}
