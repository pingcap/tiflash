// Source: https://github.com/moneroexamples/fiberpool
// FiberPool based on:
// http://roar11.com/2016/01/a-platform-independent-thread-pool-using-c14/

#pragma once

#include <Common/setThreadName.h>
#include <Common/BitHelpers.h>
#include <boost/fiber/all.hpp> 

        
namespace FiberPool
{

inline auto 
no_of_defualt_threads()
{
    return std::max(std::thread::hardware_concurrency(), 2u) - 1u;
}

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
        : m_base_channel {capacity}
    {}

    TaskQueue(const TaskQueue& rhs) = delete;
    TaskQueue& operator=(TaskQueue const& rhs) = delete;

    TaskQueue(TaskQueue&& other) = default;
    TaskQueue& operator=(TaskQueue&& other) = default;

    virtual ~TaskQueue() = default;

    boost::fibers::channel_op_status 
    push(typename BaseChannel::value_type const& value)
    {
        return m_base_channel.push(value);
    }
    
    boost::fibers::channel_op_status 
    push(typename BaseChannel::value_type&& value)
    {
        return m_base_channel.push(std::move(value));
    }
    
    boost::fibers::channel_op_status 
    pop(typename BaseChannel::value_type& value)
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

/**
 * All tasks executed by the FiberPool are 
 * automatically wrapped to use the 
 * following interface
 */
class IFiberTask
{
public:

    // how many running fibers there are
    inline static std::atomic<size_t> no_of_fibers {0};

    IFiberTask(void) = default;

    virtual ~IFiberTask(void) = default;
    IFiberTask(const IFiberTask& rhs) = delete;
    IFiberTask& operator=(const IFiberTask& rhs) = delete;
    IFiberTask(IFiberTask&& other) = default;
    IFiberTask& operator=(IFiberTask&& other) = default;

    /**
     * Run the task.
     */
    virtual void execute() = 0;
};


template<
    bool use_work_steal = true,
    template<typename> typename task_queue_t 
        = boost::fibers::buffered_channel,
    typename work_task_t = std::tuple<boost::fibers::launch,
                                      std::unique_ptr<IFiberTask>>
>
class FiberPool
{
private:

    /**
     * A wrapper for packaged fiber task
     */
    template <typename Func>
    class FiberTask: public IFiberTask
    {
    public:
        FiberTask(Func&& func)
            :m_func{std::move(func)}
        {}

        ~FiberTask(void) override = default;
        FiberTask(const FiberTask& rhs) = delete;
        FiberTask& operator=(const FiberTask& rhs) = delete;
        FiberTask(FiberTask&& other) = default;
        FiberTask& operator=(FiberTask&& other) = default;

        /**
         * Run the task.
         */
        void execute() override
        {
                        ++no_of_fibers;
            m_func();
                        --no_of_fibers;
        }

    private:
        Func m_func;
    }; 

public:

    static constexpr bool work_stealing 
        = use_work_steal;

    FiberPool()
        : FiberPool {no_of_defualt_threads()}
    {}

    FiberPool(
            size_t no_of_threads,
            size_t work_queue_size = 32)
                :   m_threads_no {no_of_threads},
            m_work_queue {work_queue_size}
    {
        try 
        {
            for(std::uint32_t i = 0; i < m_threads_no; ++i)
            {
                m_threads.emplace_back(
                        &FiberPool::worker, this);
            }
        }
        catch(...)
        {
            close_queue();
            throw;
        }
    }

    /**
     * Submit a task to be executed as fiber by worker threads
     */
    template<typename Func, typename... Args>
    auto submit(boost::fibers::launch launch_policy, 
                Func&& func, Args&&... args)
    {
        // capature our task into lambda with all its parameters
        auto capture = [func = std::forward<Func>(func),
                        args = std::make_tuple(std::forward<Args>(args)...)]() 
                            mutable
            {
                // run the tesk with the parameters provided
                // this will be what our fibers execute
                return std::apply(std::move(func), std::move(args));
            };

        // get return type of our task
        using task_result_t = std::invoke_result_t<decltype(capture)>;

        // create fiber package_task
        using packaged_task_t 
            = boost::fibers::packaged_task<task_result_t()>; 
        
        packaged_task_t task {std::move(capture)};
        
        using task_t = FiberTask<packaged_task_t>;

        // get future for obtaining future result when 
        // the fiber completes
        auto result_future = task.get_future();

        // finally submit the packaged task into work queue
        auto status = m_work_queue.push(
                std::make_tuple(launch_policy, 
                                std::make_unique<task_t>(
                                    std::move(task))));

        if (status != boost::fibers::channel_op_status::success)
        {
            return std::optional<std::decay_t<decltype(result_future)>> {};
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
    template<typename Func, typename... Args>
    auto submit(Func&& func, Args&&... args)
    {
        return submit(boost::fibers::launch::post,
                      std::forward<Func>(func),
                      std::forward<Args>(args)...);
    }

    /**
     * Non-copyable.
     */
    FiberPool(FiberPool const& rhs) = delete;

    /**
     * Non-assignable.
     */
    FiberPool& operator=(FiberPool const& rhs) = delete;

    void close_queue() noexcept 
    {
        m_work_queue.close();
    }

    auto threads_no() const noexcept
    {
        return m_threads.size();
    }

        auto fibers_no() const noexcept
        {
                return IFiberTask::no_of_fibers.load();
        }
   
        ~FiberPool()
        {
                for(auto& thread : m_threads)
                {
                         if(thread.joinable())
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
        setThreadName("FiberPool");
        // make this thread participate in shared_work 
        // fiber sharing
        //
        

        if constexpr(work_stealing)
        { 
            // work_stealing sheduling is much faster
            // than work_shearing, but it does not 
            // allow for modifying number of threads
            // at runtime. Therefore if one uses
            // DefaultFiberPool, no other instance 
            // of the fiber pool can be created
            // as this would change the number of
            // worker threads
            boost::fibers::use_scheduling_algorithm<
                boost::fibers::algo::work_stealing>(
                        m_threads_no, true);
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
            = typename decltype(m_work_queue)::value_type {}; 

        // fetch a packaged task from the work queue.
        // if there is nothing, we are just going to wait
        // here till we get some task
        while(boost::fibers::channel_op_status::success 
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
            auto& [launch_policy, task_to_run] = task_tuple; 
            
            // earlier we already got future for the fiber
            // so we can get the result of our task if we want
            boost::fibers::fiber(launch_policy,
                    [task = std::move(task_to_run)]()
                    {
                        // execute our task in the newly created
                        // fiber
                        task->execute();
                    }).detach();
        }
    }

    size_t m_threads_no {1};

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
    TaskQueue<task_queue_t<work_task_t>> m_work_queue;
};


template<
    template<typename> typename task_queue_t 
        = boost::fibers::buffered_channel,
    typename work_task_t = std::tuple<boost::fibers::launch,
                                      std::unique_ptr<IFiberTask>>
>
using FiberPoolStealing = FiberPool<true, task_queue_t, work_task_t>; 


template<
    template<typename> typename task_queue_t 
        = boost::fibers::buffered_channel,
    typename work_task_t = std::tuple<boost::fibers::launch,
                                      std::unique_ptr<IFiberTask>>
>
using FiberPoolSharing = FiberPool<false, task_queue_t, work_task_t>; 

}

/**
 * A static default FiberPool in which
 * number of threads is set automatically based
 * on your hardware
 */
namespace DefaultFiberPool
{

inline auto& 
get_pool()
{
    static FiberPool::FiberPool default_fp {};
    return default_fp;
};


template <typename Func, typename... Args>
inline auto 
submit_job(boost::fibers::launch launch_policy,
           Func&& func, Args&&... args)
{
        return get_pool().submit(
            launch_policy,
            std::forward<Func>(func), 
                        std::forward<Args>(args)...);
}

template <typename Func, typename... Args>
inline auto 
submit_job(Func&& func, Args&&... args)
{
        return get_pool().submit(
            std::forward<Func>(func), 
                        std::forward<Args>(args)...);
}

inline void
close()
{
        get_pool().close_queue();
}

}
