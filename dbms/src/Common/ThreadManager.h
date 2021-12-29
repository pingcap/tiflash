#pragma once

#include <functional>
#include <memory>
#include <string>

namespace DB
{
class ThreadManager
{
public:
    using Job = std::function<void()>;
    virtual ~ThreadManager() = default;
    // only wait non-detached tasks
    virtual void wait() = 0;
    virtual void schedule(bool propagate_memory_tracker, String thread_name, Job job) = 0;
    virtual void scheduleThenDetach(bool propagate_memory_tracker, String thread_name, Job job) = 0;
};

std::shared_ptr<ThreadManager> newThreadManager();

class ThreadPoolManager
{
public:
    using Job = std::function<void()>;
    virtual ~ThreadPoolManager() = default;
    virtual void wait() = 0;
    virtual void schedule(bool propagate_memory_tracker, Job job) = 0;
};

std::shared_ptr<ThreadPoolManager> newThreadPoolManager(size_t capacity);

} // namespace DB
