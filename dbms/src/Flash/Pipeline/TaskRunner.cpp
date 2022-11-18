// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/setThreadName.h>
#include <Flash/Pipeline/TaskRunner.h>
#include <Flash/Pipeline/TaskScheduler.h>
#include <errno.h>

namespace DB
{
TaskRunner::TaskRunner(TaskScheduler & scheduler_)
    : scheduler(scheduler_)
{
    cpu_thread = std::thread(&TaskRunner::loop, this);
}

TaskRunner::~TaskRunner()
{
    cpu_thread.join();
    LOG_INFO(logger, "stop task runner");
}

void TaskRunner::loop()
{
    setThreadName("TaskRunner");
    LOG_INFO(logger, "start task runner loop");
    TaskPtr task;
    while (likely(scheduler.popTask(task)))
    {
        handleTask(std::move(task));
    }
    LOG_INFO(logger, "task runner loop finished");
}

void TaskRunner::handleTask(TaskPtr && task)
{
    auto [status, err_msg] = task->execute();
    switch (status)
    {
    case PStatus::BLOCKED:
        scheduler.submitIO(std::move(task));
        break;
    case PStatus::FINISHED:
        scheduler.finishOneTask();
        break;
    case PStatus::NEED_MORE:
        scheduler.submitCPU(std::move(task));
        break;
    case PStatus::FAIL:
    {
        LOG_ERROR(logger, "task fail for err: {}", err_msg);
        scheduler.finishOneTask();
        break;
    }
    }
}
} // namespace DB
