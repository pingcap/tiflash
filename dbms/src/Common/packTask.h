#pragma once

#include <Common/MemoryTrackerSetter.h>

#include <future>

namespace DB
{
template <typename Func, typename... Args>
inline auto packTask(Func && func, Args &&... args)
{
    auto memory_tracker = current_memory_tracker;

    // capature our task into lambda with all its parameters
    auto capture = [memory_tracker,
                    func = std::forward<Func>(func),
                    args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
        MemoryTrackerSetter setter(true, memory_tracker);
        // run the task with the parameters provided
        return std::apply(std::move(func), std::move(args));
    };

    // get return type of our task
    using TaskResult = std::invoke_result_t<decltype(capture)>;

    // create package_task to capture exceptions
    using PackagedTask = std::packaged_task<TaskResult()>;
    PackagedTask task{std::move(capture)};

    return std::move(task);
}
} // namespace DB
