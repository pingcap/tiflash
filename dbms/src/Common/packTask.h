#pragma once

#include <Common/wrapInvocable.h>

#include <future>

namespace DB
{
template <typename Func, typename... Args>
inline auto packTask(bool propagate_memory_tracker, Func && func, Args &&... args)
{
    auto capture = wrapInvocable(propagate_memory_tracker, std::forward<Func>(func), std::forward<Args>(args)...);
    // get return type of our task
    using TaskResult = std::invoke_result_t<decltype(capture)>;

    // create package_task to capture exceptions
    using PackagedTask = std::packaged_task<TaskResult()>;
    PackagedTask task{std::move(capture)};

    return std::move(task);
}
} // namespace DB
