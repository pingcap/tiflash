#pragma once

#include <Common/MemoryTrackerSetter.h>
#include <Common/setThreadName.h>
#include <common/ThreadPool.h>

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
    static std::thread newThread(String thread_name, F && f, Args &&... args)
    {
        auto memory_tracker = current_memory_tracker;
        auto wrapped_func = [memory_tracker, thread_name = std::move(thread_name), f = std::move(f)](auto &&... args) {
            MemoryTrackerSetter setter(true, memory_tracker);
            if (!thread_name.empty())
                setThreadName(thread_name.c_str());
            return std::invoke(f, std::forward<Args>(args)...);
        };
        return std::thread(wrapped_func, std::forward<Args>(args)...);
    }

    template <typename F, typename... Args>
    static ThreadPool::Job newJob(F && f, Args &&... args)
    {
        auto memory_tracker = current_memory_tracker;
        /// Use std::tuple to workaround the limit on the lambda's init-capture of C++17.
        /// See https://stackoverflow.com/questions/47496358/c-lambdas-how-to-capture-variadic-parameter-pack-from-the-upper-scope
        return [memory_tracker, f = std::move(f), args = std::make_tuple(std::move(args)...)] {
            MemoryTrackerSetter setter(true, memory_tracker);
            return std::apply(f, std::move(args));
        };
    }
};

} // namespace DB
