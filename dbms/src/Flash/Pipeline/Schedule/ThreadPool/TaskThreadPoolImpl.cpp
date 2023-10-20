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

#include <Common/Exception.h>
#include <Flash/Pipeline/Schedule/TaskQueues/IOPriorityQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueues/ResourceControlQueue.h>
#include <Flash/Pipeline/Schedule/ThreadPool/TaskThreadPoolImpl.h>

#include <magic_enum.hpp>

namespace DB
{
TaskQueuePtr CPUImpl::newTaskQueue(TaskQueueType type)
{
    switch (type)
    {
    // the default queue is RCQ_MLFQ.
    case TaskQueueType::DEFAULT:
    case TaskQueueType::RCQ_MLFQ:
        return std::make_unique<ResourceControlQueue<CPUMultiLevelFeedbackQueue>>();
    case TaskQueueType::MLFQ:
        return std::make_unique<CPUMultiLevelFeedbackQueue>();
    default:
        throw Exception(fmt::format("Unsupported queue type: {}", magic_enum::enum_name(type)));
    }
}

TaskQueuePtr IOImpl::newTaskQueue(TaskQueueType type)
{
    switch (type)
    {
    // the default queue is io priority queue.
    case TaskQueueType::DEFAULT:
    case TaskQueueType::IO_PRIORITY:
        return std::make_unique<IOPriorityQueue>();
    case TaskQueueType::MLFQ:
        return std::make_unique<IOMultiLevelFeedbackQueue>();
    default:
        throw Exception(fmt::format("Unsupported queue type: {}", magic_enum::enum_name(type)));
    }
}
} // namespace DB
