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

#include <Common/MemoryTracker.h>

#include <boost/noncopyable.hpp>

namespace DB
{
// MemoryTrackerSetter is a guard for `current_memory_tracker`.
// It ensures `current_memory_tracker` unchanged after leaving the scope of
// where MemoryTrackerSetter guards.
class MemoryTrackerSetter : private boost::noncopyable
{
public:
    MemoryTrackerSetter(bool enable_, MemoryTracker * memory_tracker)
        : enable(enable_)
        , old_memory_tracker(current_memory_tracker)
    {
        CurrentMemoryTracker::submitLocalDeltaMemory();
        if (enable)
            current_memory_tracker = memory_tracker;
    }

    ~MemoryTrackerSetter()
    {
        /// submit current local delta memory if the memory tracker is leaving current thread
        CurrentMemoryTracker::submitLocalDeltaMemory();
        current_memory_tracker = old_memory_tracker;
    }

private:
    bool enable;
    MemoryTracker * old_memory_tracker;
};
} // namespace DB
