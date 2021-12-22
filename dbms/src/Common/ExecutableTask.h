#pragma once

#include <memory>
#include <future>

namespace DB
{
class IExecutableTask
{
public:
    IExecutableTask() = default;

    virtual ~IExecutableTask() = default;
    IExecutableTask(const IExecutableTask & rhs) = delete;
    IExecutableTask & operator=(const IExecutableTask & rhs) = delete;
    IExecutableTask(IExecutableTask && other) = default;
    IExecutableTask & operator=(IExecutableTask && other) = default;

    virtual void execute() = 0;
};
using ExecutableTaskPtr = std::shared_ptr<IExecutableTask>;

template <typename Func>
class ExecutableTask : public IExecutableTask
{
public:
    ExecutableTask(Func && func_)
        : func{std::move(func_)}
    {}

    ~ExecutableTask() override = default;
    ExecutableTask(ExecutableTask && other) = default;
    ExecutableTask & operator=(ExecutableTask && other) = default;

    void execute() override
    {
        func();
    }

private:
    Func func;
};
} // namespace DB
