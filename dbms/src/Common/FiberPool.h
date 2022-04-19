// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Source: https://github.com/moneroexamples/fiberpool
// FiberPool based on:
// http://roar11.com/2016/01/a-platform-independent-thread-pool-using-c14/

#pragma once

#include <Common/BitHelpers.h>
#include <Common/ExecutableTask.h>
#include <Common/setThreadName.h>

#include <boost/fiber/all.hpp>

inline thread_local bool g_run_in_fiber = false;

inline void adaptiveYield()
{
#ifdef TIFLASH_USE_FIBER
    if (g_run_in_fiber)
        boost::this_fiber::yield();
    else
        std::this_thread::yield();
#endif
}

namespace FiberPool
{
/**
 * A wrapper primarly for boost::fibers::buffered_channel
 * The buffered_channel has non-virtual member functions
 * thus cant inherit from it and use it polyformically.
 * This makes it diffuclt to mock its behaviour in unit tests
 * The wrapper solves this (see tests for example mock channel)
 */
template <typename BaseChannel>
class TaskQueue
{
public:
    using value_type = typename BaseChannel::value_type;

    explicit TaskQueue(std::size_t capacity)
        : m_base_channel{capacity}
    {}

    TaskQueue(const TaskQueue & rhs) = delete;
    TaskQueue & operator=(TaskQueue const & rhs) = delete;

    TaskQueue(TaskQueue && other) = default;
    TaskQueue & operator=(TaskQueue && other) = default;

    virtual ~TaskQueue() = default;

    boost::fibers::channel_op_status push(typename BaseChannel::value_type const & value)
    {
        return m_base_channel.push(value);
    }

    boost::fibers::channel_op_status push(typename BaseChannel::value_type && value)
    {
        return m_base_channel.push(std::move(value));
    }

    boost::fibers::channel_op_status pop(typename BaseChannel::value_type & value)
    {
        return m_base_channel.pop(value);
    }

    void close() noexcept
    {
        m_base_channel.close();
    }

private:
    BaseChannel m_base_channel;
};

template <
    bool use_work_steal = true,
    template <typename> typename TaskQueueT = boost::fibers::buffered_channel,
    typename WorkTaskT = std::tuple<boost::fibers::launch, std::unique_ptr<IExecutableTask>>>
class FiberPool
{
public:
    static constexpr bool work_stealing = use_work_steal;

    FiberPool()
        : FiberPool{std::thread::hardware_concurrency()}
    {}

    FiberPool(
        size_t no_of_threads,
        size_t work_queue_size = 4096)
        : m_threads_no{no_of_threads}
        , m_work_queue{work_queue_size}
    {
        try
        {
            for (std::uint32_t i = 0; i < m_threads_no; ++i)
            {
                m_threads.emplace_back(
                    &FiberPool::worker,
                    this);
            }
        }
        catch (...)
        {
            close_queue();
            throw;
        }
    }

    /**
     * Submit a task to be executed as fiber by worker threads
     */
    template <typename Func, typename... Args>
    auto submit(
        boost::fibers::launch launch_policy,
        Func && func,
        Args &&... args)
    {
        auto task = packTask(false, std::forward<Func>(func), std::forward<Args>(args)...);

        auto result_future = task.get_future();

        // finally submit the packaged task into work queue
        auto status = m_work_queue.push(
            std::make_tuple(
                launch_policy,
                std::make_unique<ExecutableTask<decltype(task)>> > (std::move(task))));

        if (status != boost::fibers::channel_op_status::success)
        {
            return std::optional<std::decay_t<decltype(result_future)>>{};
        }

        // return the future to the caller so that
        // we can get the result when the fiber with our task
        // completes
        return std::make_optional(std::move(result_future));
    }


    /**
     * Use boost::fibers:launch::post as 
     * default lanuch strategy for fibers
     */
    template <typename Func, typename... Args>
    auto submit(Func && func, Args &&... args)
    {
        return submit(
            boost::fibers::launch::post,
            std::forward<Func>(func),
            std::forward<Args>(args)...);
    }

    /**
     * Non-copyable.
     */
    FiberPool(FiberPool const & rhs) = delete;

    /**
     * Non-assignable.
     */
    FiberPool & operator=(FiberPool const & rhs) = delete;

    void closeQueue() noexcept
    {
        m_work_queue.close();
    }

    ~FiberPool()
    {
        for (auto & thread : m_threads)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
    }

private:
    /**
     * worker thread method. It participates with 
     * shared_work sheduler of fibers.
     *
     * It takes packaged taskes from the work_queue
     * and launches fibers executing the tasks
     */
    void worker()
    {
        g_run_in_fiber = true;
        setThreadName("FiberPool");
        // make this thread participate in shared_work
        // fiber sharing
        //


        if constexpr (work_stealing)
        {
            // work_stealing sheduling is much faster
            // than work_shearing, but it does not
            // allow for modifying number of threads
            // at runtime. Therefore if one uses
            // DefaultFiberPool, no other instance
            // of the fiber pool can be created
            // as this would change the number of
            // worker threads
            boost::fibers::use_scheduling_algorithm<boost::fibers::algo::work_stealing>(
                m_threads_no,
                true);
        }
        else
        {
            // it is slower but, can vary number of
            // worker threads at runtime. So you can
            // use DefaultFiberPool in one part of
            // you application, and custom instance
            // of the fiber pool in other part.
            boost::fibers::use_scheduling_algorithm<
                boost::fibers::algo::shared_work>(true);
        }

        // create a placeholder for packaged task for
        // to-be-created fiber to execute
        auto task_tuple
            = typename decltype(m_work_queue)::value_type{};

        // fetch a packaged task from the work queue.
        // if there is nothing, we are just going to wait
        // here till we get some task
        while (boost::fibers::channel_op_status::success
               == m_work_queue.pop(task_tuple))
        {
            // creates a fiber from the pacakged task.
            //
            // the fiber is immedietly detached so that we
            // fetch next task from the queue without blocking
            // the thread and waiting here for the fiber to
            // complete

            // the task is tuple with launch policy and
            // accutal packaged_task to run
            auto & [launch_policy, task_to_run] = task_tuple;

            // earlier we already got future for the fiber
            // so we can get the result of our task if we want
            boost::fibers::fiber(
                launch_policy,
                [task = std::move(task_to_run)]() {
                    task->execute();
                })
                .detach();
        }
    }

    size_t m_threads_no{1};

    // worker threads. these are the threads which will
    // be executing our fibers. Since we use work_shearing scheduling
    // algorithm, the fibers should be shared evenly
    // between these threads
    std::vector<std::thread> m_threads;

    // use buffered_channel (by default) so that we dont block when
    // there is no  reciver for the fiber. we are only
    // going to block when the buffered_channel is full.
    // Otherwise, tasks will be just waiting in the
    // queue till some fiber picks them up.
    TaskQueue<TaskQueueT<WorkTaskT>> m_work_queue;
};


template <
    template <typename> typename TaskQueueT = boost::fibers::buffered_channel,
    typename WorkTaskT = std::tuple<boost::fibers::launch, std::unique_ptr<IExecutableTask>>>
using FiberPoolStealing = FiberPool<true, TaskQueueT, WorkTaskT>;


template <
    template <typename> typename TaskQueueT = boost::fibers::buffered_channel,
    typename WorkTaskT = std::tuple<boost::fibers::launch, std::unique_ptr<IExecutableTask>>>
using FiberPoolSharing = FiberPool<false, TaskQueueT, WorkTaskT>;

} // namespace FiberPool

/**
 * A static default FiberPool in which
 * number of threads is set automatically based
 * on your hardware
 */
namespace DefaultFiberPool
{
inline auto & getPool()
{
    static FiberPool::FiberPool default_fp{};
    return default_fp;
};


template <typename Func, typename... Args>
inline auto submitJob(
    boost::fibers::launch launch_policy,
    Func && func,
    Args &&... args)
{
    return getPool().submit(
        launch_policy,
        std::forward<Func>(func),
        std::forward<Args>(args)...);
}

template <typename Func, typename... Args>
inline auto submitJob(Func && func, Args &&... args)
{
    return getPool().submit(
        std::forward<Func>(func),
        std::forward<Args>(args)...);
}

inline void close()
{
    getPool().close_queue();
}

} // namespace DefaultFiberPool
