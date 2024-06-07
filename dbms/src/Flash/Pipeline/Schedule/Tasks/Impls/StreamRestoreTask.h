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

#include <DataStreams/IBlockInputStream.h>
#include <Flash/Pipeline/Schedule/Tasks/Task.h>

namespace DB
{
class SharedQueueSinkHolder;
using SharedQueueSinkHolderPtr = std::shared_ptr<SharedQueueSinkHolder>;

/// Used to read block from io-based block input stream like `SpilledFilesInputStream` and write block to result queue.
///
/// io_stream (read in io_thread_pool) --> result_queue --> caller.
class StreamRestoreTask : public Task
{
public:
    StreamRestoreTask(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const BlockInputStreamPtr & stream_,
        const SharedQueueSinkHolderPtr & sink_);

protected:
    ExecTaskStatus executeImpl() override;

    ExecTaskStatus executeIOImpl() override;

    void finalizeImpl() override;

    ExecTaskStatus notifyImpl() override { return ExecTaskStatus::IO_IN; }

private:
    ExecTaskStatus tryFlush();

private:
    BlockInputStreamPtr stream;
    SharedQueueSinkHolderPtr sink;

    Block t_block;
    bool is_done = false;
};
} // namespace DB
