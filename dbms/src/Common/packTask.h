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

#include <Common/wrapInvocable.h>

#include <future>

namespace DB
{
template <typename Func, typename... Args>
inline auto packTask(bool propagate_memory_tracker, Func && func, Args &&... args)
{
    auto capture = wrapInvocable(propagate_memory_tracker, std::forward<Func>(func), std::forward<Args>(args)...);
    // get return type of our task
    using TaskResult = std::invoke_result_t<decltype(capture)>;

    // create package_task to capture exceptions
    using PackagedTask = std::packaged_task<TaskResult()>;
    PackagedTask task{std::move(capture)};

    return task;
}
} // namespace DB
