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

#pragma once

#include <Flash/Pipeline/Schedule/Tasks/Task.h>

namespace DB
{
struct NotifyFuture
{
    NotifyFuture() = default;
    virtual ~NotifyFuture() = default;
    virtual void registerTask(TaskPtr && task) = 0;
};
using NotifyFuturePtr = std::shared_ptr<NotifyFuture>;

#if __APPLE__ && __clang__
extern __thread NotifyFuturePtr current_notify_future;
#else
extern thread_local NotifyFuturePtr current_notify_future;
#endif

void setNotifyFuture(NotifyFuturePtr new_future);
void registerTaskToFuture(TaskPtr && task);

} // namespace DB
