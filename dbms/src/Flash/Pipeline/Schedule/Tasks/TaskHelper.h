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

#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerSetter.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

#include <magic_enum.hpp>

namespace DB
{
// Hold the shared_ptr of memory tracker.
// To avoid the current_memory_tracker being an illegal pointer.
#define TRACE_MEMORY(task)                         \
    assert(nullptr == current_memory_tracker);     \
    auto memory_tracker = (task)->getMemTracker(); \
    MemoryTrackerSetter memory_tracker_setter{true, memory_tracker.get()};

#define ASSERT_MEMORY_TRACKER                  \
    assert(nullptr == current_memory_tracker); \
    assert(0 == CurrentMemoryTracker::getLocalDeltaMemory());

#define FINISH_STATUS \
    ExecTaskStatus::FINISHED : case ExecTaskStatus::ERROR : case ExecTaskStatus::CANCELLED

#define UNEXPECTED_STATUS(logger, status) \
    RUNTIME_ASSERT(false, (logger), "Unexpected task status {}", magic_enum::enum_name(status));

static constexpr int64_t YIELD_MAX_TIME_SPENT_NS = 100'000'000L;

} // namespace DB
