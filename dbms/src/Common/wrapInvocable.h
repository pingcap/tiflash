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

#include <Common/MemoryTrackerSetter.h>

namespace DB
{
template <typename Func, typename... Args>
inline auto wrapInvocable(bool propagate_memory_tracker, Func && func, Args &&... args)
{
    /// submit current local delta memory if the memory tracker needs to be propagated to other threads
    if (propagate_memory_tracker)
        CurrentMemoryTracker::submitLocalDeltaMemory();
    auto * memory_tracker = current_memory_tracker;

    // capature our task into lambda with all its parameters
    auto capture = [propagate_memory_tracker,
                    memory_tracker,
                    func = std::forward<Func>(func),
                    args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
        MemoryTrackerSetter setter(propagate_memory_tracker, memory_tracker);
        // run the task with the parameters provided
        return std::apply(std::move(func), std::move(args));
    };
    return capture;
}
} // namespace DB
