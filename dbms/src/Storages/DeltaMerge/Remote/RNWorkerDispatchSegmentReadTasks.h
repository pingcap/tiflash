#pragma once

#include <Common/Logger.h>
#include <Common/MPMCQueue.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadManager.h>
#include <Storages/DeltaMerge/Remote/RNReadTask.h>
#include <Storages/DeltaMerge/Remote/RNReadTask_fwd.h>
#include <common/logger_useful.h>

#include <ext/scope_guard.h>
#include <magic_enum.hpp>
#include <mutex>

namespace DB::DM::Remote
{
class RNWorkerDispatchSegmentReadTasks;
using RNWorkerDispatchSegmentReadTasksPtr = std::shared_ptr<RNWorkerDispatchSegmentReadTasks>;

class RNWorkerDispatchSegmentReadTasks
{
public:
    void start() noexcept
    {
        std::call_once(start_flag, [this] {
            LOG_DEBUG(log, "Starting {} workers, concurrency={}", getName(), concurrency);
            for (size_t index = 0; index < concurrency; ++index)
            {
                thread_manager->schedule(true, getName(), [this, index] { workerLoop(index); });
            }
        });
    }

    void wait() noexcept
    {
        std::call_once(wait_flag, [this] {
            try
            {
                // thread_manager->wait can be only called once.
                thread_manager->wait();
            }
            catch (...)
            {
                // This should not occur, as we should have caught all exceptions in workerLoop.
                auto error = getCurrentExceptionMessage(false);
                LOG_WARNING(log, "{} meet unexepcted error: {}", getName(), error);
            }
        });
    }

    RNWorkerDispatchSegmentReadTasks(
        std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> source_queue_,
        size_t concurrency_)
        : source_queue(source_queue_)
        , concurrency(concurrency_)
        , log(DB::Logger::get(getName()))
        , thread_manager(newThreadManager())
    {
        RUNTIME_CHECK(concurrency > 0, concurrency);
    }

    static RNWorkerDispatchSegmentReadTasksPtr create(
        std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> source_queue,
        size_t concurrency)
    {
        return std::make_shared<RNWorkerDispatchSegmentReadTasks>(source_queue, concurrency);
    }

    std::shared_ptr<MPMCQueue<RNReadSegmentTaskPtr>> source_queue;
private:
    static String getName() { return "DispatchSegmentReadTasks"; };

    void workerLoop(size_t thread_idx) noexcept
    {
        while (true)
        {
            RNReadSegmentTaskPtr task;
            auto pop_result = source_queue->pop(task);
            if (pop_result != MPMCQueueResult::OK)
            {
                LOG_INFO(log, "{}#{} pop MPMCQueueResult: {}", getName(), thread_idx, magic_enum::enum_name(pop_result));
                break;
            }
            auto res = task->param->prepared_tasks->push(task);
            if (res != MPMCQueueResult::OK)
            {
                LOG_INFO(task->param->log, "{}#{} push MPMCQueueResult: {}", getName(), thread_idx, magic_enum::enum_name(res));
            }
        }
    }

private:
    const size_t concurrency;
    const LoggerPtr log;
    std::shared_ptr<ThreadManager> thread_manager;
    std::once_flag start_flag;
    std::once_flag wait_flag;
};
} // namespace DB::DM::Remote