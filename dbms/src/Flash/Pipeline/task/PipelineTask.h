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

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Mpp/MPPTaskId.h>

namespace DB
{
enum class PipelineTaskStatus
{
    running,
    finished,
    error,
};

struct PipelineTaskResult
{
    UInt32 task_id;
    UInt32 pipeline_id;
    MPPTaskId mpp_task_id;

    PipelineTaskStatus status;

    String err_msg;
};

class PipelineTask
{
public:
    PipelineTask() = default;

    PipelineTask(
        UInt32 task_id_,
        UInt32 pipeline_id_,
        const MPPTaskId & mpp_task_id_,
        const BlockInputStreamPtr & stream_)
        : task_id(task_id_)
        , pipeline_id(pipeline_id_)
        , mpp_task_id(mpp_task_id_)
        , stream(stream_)
    {}

    PipelineTask(PipelineTask && task)
        : task_id(std::move(task.task_id))
        , pipeline_id(std::move(task.pipeline_id))
        , mpp_task_id(std::move(task.mpp_task_id))
        , stream(std::move(task.stream))
    {}

    PipelineTask & operator=(PipelineTask && task)
    {
        if (this != &task)
        {
            task_id = task.task_id;
            pipeline_id = task.pipeline_id;
            mpp_task_id = task.mpp_task_id;
            stream = std::move(task.stream);
        }
        return *this;
    }

    PipelineTaskResult execute(size_t);

public:
    UInt32 task_id;
    UInt32 pipeline_id;
    MPPTaskId mpp_task_id;
    BlockInputStreamPtr stream;

private:
    PipelineTaskResult finish();
    PipelineTaskResult fail(const String & err_msg);
    PipelineTaskResult running();
};
} // namespace DB
