#pragma once

#include <Common/MemoryTrackerSetter.h>
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>
#include <common/ThreadPool.h>

#include <ext/scope_guard.h>
#include <thread>

namespace DB
{
/// ThreadFactory helps to set attributes on new threads or threadpool's jobs.
/// Current supported attributes:
/// 1. MemoryTracker
/// 2. ThreadName
///
/// ThreadFactory should only be constructed on stack.
class ThreadFactory
{
public:
    template <typename F, typename... Args>
    static std::thread newThread(bool propagate_memory_tracker, String thread_name, F && f, Args &&... args)
    {
        auto memory_tracker = current_memory_tracker;
        auto wrapped_func = [propagate_memory_tracker, memory_tracker, thread_name = std::move(thread_name), f = std::move(f)](auto &&... args) {
            UPDATE_CUR_AND_MAX_METRIC(tiflash_thread_count, type_total_threads_of_raw, type_max_threads_of_raw);
            MemoryTrackerSetter setter(propagate_memory_tracker, memory_tracker);
            if (!thread_name.empty())
                setThreadName(thread_name.c_str());
            return std::invoke(f, std::forward<Args>(args)...);
        };
        return std::thread(wrapped_func, std::forward<Args>(args)...);
    }
};

} // namespace DB
