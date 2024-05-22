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

#include <Flash/Pipeline/Schedule/Tasks/StreamRestoreTask.h>
#include <Operators/SharedQueue.h>

#include <magic_enum.hpp>

namespace DB
{
StreamRestoreTask::StreamRestoreTask(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    const BlockInputStreamPtr & stream_,
    const SharedQueueSinkHolderPtr & sink_)
    : Task(exec_context_, req_id, ExecTaskStatus::IO_IN)
    , stream(stream_)
    , sink(sink_)
{
    assert(sink);
    assert(stream);
    stream->readPrefix();
}

ExecTaskStatus StreamRestoreTask::executeImpl()
{
    throw Exception("unreachable");
}

ExecTaskStatus StreamRestoreTask::tryFlush()
{
    assert(t_block);
    auto ret = sink->tryPush(std::move(t_block));
    switch (ret)
    {
    case MPMCQueueResult::OK:
        t_block.clear();
        return ExecTaskStatus::IO_IN;
    case MPMCQueueResult::FULL:
        setNotifyFuture(sink);
        return ExecTaskStatus::WAIT_FOR_NOTIFY;
    case MPMCQueueResult::CANCELLED:
        return ExecTaskStatus::CANCELLED;
    default:
        // queue result can not be finished/empty here.
        throw Exception(fmt::format("Unexpect result: {}", magic_enum::enum_name(ret)));
    }
}

ExecTaskStatus StreamRestoreTask::executeIOImpl()
{
    if unlikely (is_done)
        return ExecTaskStatus::FINISHED;

    if (t_block)
    {
        auto try_flush_status = tryFlush();
        if (try_flush_status != ExecTaskStatus::IO_IN)
            return try_flush_status;
    }

    assert(!t_block);
    t_block = stream->read();
    if (unlikely(!t_block))
    {
        is_done = true;
        return ExecTaskStatus::FINISHED;
    }
    return tryFlush();
}

void StreamRestoreTask::finalizeImpl()
{
    stream->readSuffix();
    sink->finish();
}
} // namespace DB
