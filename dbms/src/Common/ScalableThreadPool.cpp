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

template <typename F, typename... Args>
std::function<void()> ScalableThreadPool::newJobWithMemTracker(F && f, Args &&... args)
{
    auto memory_tracker = current_memory_tracker;
    /// Use std::tuple to workaround the limit on the lambda's init-capture of C++17.
    /// See https://stackoverflow.com/questions/47496358/c-lambdas-how-to-capture-variadic-parameter-pack-from-the-upper-scope
    return [memory_tracker, f = std::move(f), args = std::make_tuple(std::move(args)...)] {
        current_memory_tracker = memory_tracker;
        std::apply(f, std::move(args));
        current_memory_tracker = nullptr;
    };
}

ScalableThreadPool::ScalableThreadPool(size_t m_size, Job pre_worker_)
    : m_size(m_size)
    , pre_worker(pre_worker_)
{
    threads.reserve(m_size);
    for (size_t i = 0; i < m_size; ++i)
        threads.emplace_back(std::make_shared<std::thread>([this] {
            pre_worker();
            worker();
        }));
}

void ScalableThreadPool::schedule(Job job)
{
    {
        std::unique_lock<std::mutex> lock(mutex);
        if (shutdown)
            return;
        if (active_jobs >= threads.size())
        {
            threads.emplace_back(std::make_shared<std::thread>([this] {
                pre_worker();
                worker();
            }));
        }

        jobs.push(std::move(job));
        ++active_jobs;
    }
    has_new_job_or_shutdown.notify_one();
}

void ScalableThreadPool::scheduleWithMemTracker(Job job)
{
    schedule(newJobWithMemTracker(job));
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

    for (auto & thread : threads)
        thread->join();
}

size_t ScalableThreadPool::active() const
{
    std::unique_lock<std::mutex> lock(mutex);
    return active_jobs;
}


void ScalableThreadPool::worker()
{
    while (true)
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

        has_free_thread.notify_all();
    }
}