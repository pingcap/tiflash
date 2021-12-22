#pragma once

#include <future>

namespace DB
{
template <typename Func, typename... Args>
inline auto packTask(Func && func, Args &&... args)
{
    // capature our task into lambda with all its parameters
    auto capture = [func = std::forward<Func>(func),
                    args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
        // run the task with the parameters provided
        return std::apply(std::move(func), std::move(args));
    };

    // get return type of our task
    using TaskResult = std::invoke_result_t<decltype(capture)>;

    // create package_task to capture exceptions
    using PackagedTask = std::packaged_task<TaskResult()>;
    PackagedTask task{std::move(capture)};

    return std::move(task);

    // get future for obtaining future result
    auto result_future = task.get_future();
    ExecutableTaskPtr wrapped_task = std::make_shared<ExecutableTask<PackagedTask>>(std::move(task));

    return std::make_tuple(std::move(wrapped_task), std::move(result_future));
}
} // namespace DB
