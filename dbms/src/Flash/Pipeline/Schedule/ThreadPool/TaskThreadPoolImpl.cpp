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

#include <Flash/Pipeline/Schedule/TaskQueues/FIFOTaskQueue.h>
#include <Flash/Pipeline/Schedule/TaskQueues/MultiLevelFeedbackQueue.h>
#include <Flash/Pipeline/Schedule/ThreadPool/TaskThreadPoolImpl.h>

namespace DB
{
TaskQueuePtr CPUImpl::newTaskQueue(TaskQueueType type)
{
    switch (type)
    {
    // the default queue is mlfq queue.
    case TaskQueueType::DEFAULT:
    case TaskQueueType::MLFQ:
        return std::make_unique<CPUMultiLevelFeedbackQueue>();
    case TaskQueueType::FIFO:
        return std::make_unique<FIFOTaskQueue>();
    }
}

TaskQueuePtr IOImpl::newTaskQueue(TaskQueueType type)
{
    switch (type)
    {
    // the default queue is fifo queue.
    case TaskQueueType::DEFAULT:
    case TaskQueueType::FIFO:
        return std::make_unique<FIFOTaskQueue>();
    case TaskQueueType::MLFQ:
        return std::make_unique<IOMultiLevelFeedbackQueue>();
    }
}
} // namespace DB
