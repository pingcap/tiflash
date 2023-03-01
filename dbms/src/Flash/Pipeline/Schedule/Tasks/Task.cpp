// Copyright 2023 PingCAP, Ltd.
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
namespace
{
void switchStatus(ExecTaskStatus & cur, ExecTaskStatus to) inline
{
    if (cur != to)
    {
        LOG_TRACE(log, "switch status: {} --> {}", cur, to);
        cur = to;
    }
}
}

ExecTaskStatus Task::execute() noexcept
{
    assert(getMemTracker().get() == current_memory_tracker);
    switchStatus(exec_status, executeImpl());
    return exec_status;
}

ExecTaskStatus Task::await() noexcept
{
    assert(getMemTracker().get() == current_memory_tracker);
    switchStatus(exec_status, awaitImpl());
    return exec_status;
}
} // namespace DB
