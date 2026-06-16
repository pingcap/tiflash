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

#include <Flash/Pipeline/Schedule/Tasks/Task.h>
#include <Storages/StorageDisaggregatedColumnar.h>

namespace DB
{

/// A lightweight IO-only task that materializes one ColumnarReaderWork
/// asynchronously inside the pipeline IO pool.  On completion it transitions
/// the work to Ready (or Failed) and wakes any waiters (stream-path cv and
/// pipeline notify_future).
///
/// This replaces the previous detached-thread PrefetchRNColumnarReader and
/// keeps all reader materialize work under TaskScheduler scheduling.
class PrefetchColumnarReaderTask : public Task
{
public:
    PrefetchColumnarReaderTask(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        RNColumnarReadTaskPtr read_task_,
        RNColumnarReaderWorkPtr reader_work_);

    ~PrefetchColumnarReaderTask() override = default;

protected:
    ExecTaskStatus executeImpl() override;
    ExecTaskStatus executeIOImpl() override;
    void finalizeImpl() override;

private:
    RNColumnarReadTaskPtr read_task;
    RNColumnarReaderWorkPtr reader_work;
};

} // namespace DB
