// Copyright 2023 PingCAP, Inc.
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

#include <Flash/Pipeline/Schedule/Tasks/NotifyFuture.h>
#include <Flash/Pipeline/Schedule/Tasks/TaskWaiter.h>

namespace DB
{
thread_local NotifyFuture * current_notify_future = nullptr;

void setNotifyFuture(NotifyFuture * new_future)
{
    assert(current_notify_future == nullptr);
    current_notify_future = std::move(new_future);
}

void clearNotifyFuture()
{
    current_notify_future = nullptr;
}

void registerTaskToFuture(TaskPtr && task)
{
    assert(current_notify_future != nullptr);
    auto stop_sources = task->getNotifyStopSources();
    auto waiter = std::make_shared<TaskWaiter>();
    task->setWaiter(waiter);
    auto * future = current_notify_future;
    current_notify_future = nullptr;
    future->registerTask(std::move(task));
    // The resource may already have woken the task.
    if (!waiter->hasTask())
        return;
    // Register stop notifications only after releasing the resource's registration lock.
    for (const auto & stop_source : stop_sources)
        stop_source->registerWaiter(waiter);
}
} // namespace DB
