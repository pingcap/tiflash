#pragma once

#include <Common/MemoryTracker.h>
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
    /// force_overwrite_thread_attribute is only used for ThreadPool's jobs.
    /// For new threads it is treated as always true.
    explicit ThreadFactory(bool force_overwrite_thread_attribute = false, std::string thread_name_ = "")
        : force_overwrite(force_overwrite_thread_attribute)
        , thread_name(thread_name_)
    {}

    ThreadFactory(const ThreadFactory &) = delete;
    ThreadFactory & operator=(const ThreadFactory &) = delete;

    ThreadFactory(ThreadFactory &&) = default;
    ThreadFactory & operator=(ThreadFactory &&) = default;

    template <typename F, typename... Args>
    std::thread newThread(F && f, Args &&... args)
    {
        auto memory_tracker = current_memory_tracker;
        auto wrapped_func = [memory_tracker, thread_name = thread_name, f = std::move(f)](auto &&... args) {
            setAttributes(memory_tracker, thread_name, true);
            return std::invoke(f, std::forward<Args>(args)...);
        };
        return std::thread(wrapped_func, std::forward<Args>(args)...);
    }

    template <typename F, typename... Args>
    ThreadPool::Job newJob(F && f, Args &&... args)
    {
        auto memory_tracker = current_memory_tracker;
        /// Use std::tuple to workaround the limit on the lambda's init-capture of C++17.
        /// See https://stackoverflow.com/questions/47496358/c-lambdas-how-to-capture-variadic-parameter-pack-from-the-upper-scope
        return [force_overwrite = force_overwrite, memory_tracker, thread_name = thread_name, f = std::move(f), args = std::make_tuple(std::move(args)...)] {
            setAttributes(memory_tracker, thread_name, force_overwrite);
            return std::apply(f, std::move(args));
        };
    }

private:
    static void setAttributes(MemoryTracker * memory_tracker, const std::string & thread_name, bool force_overwrite)
    {
        if (force_overwrite || !current_memory_tracker)
        {
            current_memory_tracker = memory_tracker;
            if (!thread_name.empty())
                setThreadName(thread_name.c_str());
        }
    }

    bool force_overwrite = false;
    std::string thread_name;
};

} // namespace DB
