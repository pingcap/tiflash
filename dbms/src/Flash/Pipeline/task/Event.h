// Copyright 2022 PingCAP, Ltd.
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

#include <Flash/Pipeline/task/PipelineTask.h>

namespace DB
{
enum class TaskEventType
{
    submit,
    cancel,
};

struct TaskEvent
{
    static TaskEvent submit(UInt32 pipeline_id, const PipelineTask & task)
    {
        return {pipeline_id, TaskEventType::submit, task};
    }

    static TaskEvent cancel(UInt32 pipeline_id)
    {
        return {pipeline_id, TaskEventType::cancel};
    }

    TaskEvent() = default;

    TaskEvent(
        UInt32 pipeline_id_,
        const TaskEventType & type_,
        const PipelineTask & task_)
        : pipeline_id(pipeline_id_)
        , type(type_)
        , task(task_)
    {}

    TaskEvent & operator=(TaskEvent && event)
    {
        if (this != &event)
        {
            pipeline_id = event.pipeline_id;
            type = std::move(event.type);
            task = std::move(event.task);
        }
        return *this;
    }

    UInt32 pipeline_id;
    TaskEventType type;
    PipelineTask task;
};
} // namespace DB
