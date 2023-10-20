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

#include <magic_enum.hpp>

namespace DB
{
namespace
{
ALWAYS_INLINE ExecTaskStatus tryPushBlock(const ResultQueuePtr & result_queue, Block & block)
{
    assert(block);
    auto ret = result_queue->tryPush(std::move(block));
    switch (ret)
    {
    case MPMCQueueResult::OK:
        block = {};
        return ExecTaskStatus::IO_IN;
    case MPMCQueueResult::FULL:
        // If returning Full, the block was not actually moved.
        assert(block); // NOLINT(bugprone-use-after-move)
        return ExecTaskStatus::WAITING;
    default:
        throw Exception(fmt::format("Unexpect result: {}", magic_enum::enum_name(ret)));
    }
}
} // namespace

StreamRestoreTask::StreamRestoreTask(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    const BlockInputStreamPtr & stream_,
    const ResultQueuePtr & result_queue_)
    : Task(exec_context_, req_id, ExecTaskStatus::IO_IN)
    , stream(stream_)
    , result_queue(result_queue_)
{
    assert(stream);
    stream->readPrefix();
    assert(result_queue);
}

ExecTaskStatus StreamRestoreTask::executeImpl()
{
    return is_done ? ExecTaskStatus::FINISHED : ExecTaskStatus::IO_IN;
}

ExecTaskStatus StreamRestoreTask::awaitImpl()
{
    if (unlikely(is_done))
        return ExecTaskStatus::FINISHED;
    if (unlikely(!block))
        return ExecTaskStatus::IO_IN;

    return tryPushBlock(result_queue, block);
}

ExecTaskStatus StreamRestoreTask::executeIOImpl()
{
    if (!block)
    {
        block = stream->read();
        if (unlikely(!block))
        {
            is_done = true;
            return ExecTaskStatus::FINISHED;
        }
    }

    return tryPushBlock(result_queue, block);
}

void StreamRestoreTask::finalizeImpl()
{
    result_queue->finish();
    stream->readSuffix();
}
} // namespace DB
