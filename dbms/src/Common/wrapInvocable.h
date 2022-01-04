#pragma once

#include <Common/MemoryTrackerSetter.h>

namespace DB
{
template <typename Func, typename... Args>
inline auto wrapInvocable(bool propagate_memory_tracker, Func && func, Args &&... args)
{
    auto memory_tracker = current_memory_tracker;

    // capature our task into lambda with all its parameters
    auto capture = [propagate_memory_tracker,
                    memory_tracker,
                    func = std::forward<Func>(func),
                    args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
        MemoryTrackerSetter setter(propagate_memory_tracker, memory_tracker);
        // run the task with the parameters provided
        return std::apply(std::move(func), std::move(args));
    };

    return std::move(capture);
}
} // namespace DB
