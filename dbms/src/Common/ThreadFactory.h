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
#include <Common/TiFlashMetrics.h>
#include <Common/setThreadName.h>

#include <thread>

namespace DB
{
/// ThreadFactory helps to set attributes on new threads or threadpool's jobs.
/// Current supported attributes:
/// 1. MemoryTracker
/// 2. ThreadName
///
/// ThreadFactory should only be constructed on stack.
class ThreadFactory
{
public:
    template <typename F, typename... Args>
    static std::thread newThread(bool propagate_memory_tracker, String thread_name, F && f, Args &&... args)
    {
        /// submit current local delta memory if the memory tracker needs to be propagated to other threads
        if (propagate_memory_tracker)
            CurrentMemoryTracker::submitLocalDeltaMemory();
        auto * memory_tracker = current_memory_tracker;
        auto wrapped_func
            = [propagate_memory_tracker, memory_tracker, thread_name = std::move(thread_name), f = std::forward<F>(f)](
                  auto &&... args) {
                  UPDATE_CUR_AND_MAX_METRIC(tiflash_thread_count, type_total_threads_of_raw, type_max_threads_of_raw);
                  MemoryTrackerSetter setter(propagate_memory_tracker, memory_tracker);
                  if (!thread_name.empty())
                      setThreadName(thread_name.c_str());
                  return std::invoke(f, std::forward<Args>(args)...);
              };
        return std::thread(wrapped_func, std::forward<Args>(args)...);
    }
};

} // namespace DB
