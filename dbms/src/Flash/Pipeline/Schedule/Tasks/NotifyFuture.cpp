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
    current_notify_future->registerTask(std::move(task));
    current_notify_future = nullptr;
}
} // namespace DB
