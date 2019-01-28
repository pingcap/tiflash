#pragma once

#include <Poco/Util/TimerTask.h>
#include <functional>

namespace DB
{
class FunctionTimerTask : public Poco::Util::TimerTask
{
public:
    using Task = std::function<void()>;

    FunctionTimerTask(Task task_) : task(task_) {}

    void run() override
    {
        task();
    }

    static Poco::Util::TimerTask::Ptr create(Task task)
    {
        return new FunctionTimerTask(task);
    }

private:
    Task task;
};
}
