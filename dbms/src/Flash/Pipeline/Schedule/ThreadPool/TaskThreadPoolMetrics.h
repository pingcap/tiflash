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

#include <atomic>

namespace DB
{
template <bool is_cpu>
class TaskThreadPoolMetrics
{
public:
    TaskThreadPoolMetrics();

    void incPendingTask(size_t task_count);

    void decPendingTask();

    void elapsedPendingTime(TaskPtr & task);

    void incExecutingTask();

    void decExecutingTask();

    void addExecuteTime(TaskPtr & task, UInt64 value);

    void incThreadCnt();

    void decThreadCnt();
};

} // namespace DB
