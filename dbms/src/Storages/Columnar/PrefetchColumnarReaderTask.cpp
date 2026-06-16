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

#include <Storages/Columnar/PrefetchColumnarReaderTask.h>

namespace DB
{

PrefetchColumnarReaderTask::PrefetchColumnarReaderTask(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    RNColumnarReadTaskPtr read_task_,
    RNColumnarReaderWorkPtr reader_work_)
    : Task(exec_context_, req_id, ExecTaskStatus::IO_IN)
    , read_task(std::move(read_task_))
    , reader_work(std::move(reader_work_))
{}

ExecTaskStatus PrefetchColumnarReaderTask::executeImpl()
{
    // The task is submitted with initial status IO_IN, so the scheduler
    // routes it directly to executeIOImpl.  executeImpl should never be called.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "PrefetchColumnarReaderTask: unexpected executeImpl");
}

ExecTaskStatus PrefetchColumnarReaderTask::executeIOImpl()
{
    try
    {
        auto reader = read_task->createColumnarReaderWithBackoff(reader_work);
        {
            std::lock_guard lock(reader_work->mutex);
            if (reader_work->state == RNColumnarReaderMaterializeState::Consumed)
                return ExecTaskStatus::FINISHED;
            reader_work->reader.emplace(std::move(reader));
            reader_work->exception = nullptr;
            reader_work->state = RNColumnarReaderMaterializeState::Ready;
        }
    }
    catch (...)
    {
        {
            std::lock_guard lock(reader_work->mutex);
            if (reader_work->state == RNColumnarReaderMaterializeState::Consumed)
                return ExecTaskStatus::FINISHED;
            reader_work->reader.reset();
            reader_work->exception = std::current_exception();
            reader_work->state = RNColumnarReaderMaterializeState::Failed;
        }
    }
    reader_work->cv.notify_all();
    reader_work->notify_future.notifyAll();
    return ExecTaskStatus::FINISHED;
}

void PrefetchColumnarReaderTask::finalizeImpl()
{
    // Task-level cleanup.  The reader_work and read_task shared_ptrs will
    // release their references naturally.
}

} // namespace DB
