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

#include <Flash/Pipeline/TaskScheduler.h>

namespace DB
{
TaskScheduler::TaskScheduler(const TaskSchedulerConfig & config)
    : task_executor(*this, config.task_executor_thread_num)
{
}

void TaskScheduler::submit(std::vector<TaskPtr> & tasks)
{
    task_executor.submit(tasks);
}

void TaskScheduler::close()
{
    task_executor.close();
}

std::unique_ptr<TaskScheduler> TaskScheduler::instance;
} // namespace DB
