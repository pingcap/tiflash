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

#include <Flash/Executor/PipelineExecutorContext.h>
#include <Flash/Pipeline/Exec/PipelineExecBuilder.h>
#include <Flash/Pipeline/Schedule/TaskScheduler.h>
#include <Flash/Pipeline/Schedule/Tasks/SimplePipelineTask.h>
#include <Flash/Pipeline/Schedule/Tasks/StreamRestoreTask.h>
#include <Operators/HashJoinBuildSink.h>
#include <Operators/HashProbeTransformExec.h>
#include <Operators/IOBlockInputStreamSourceOp.h>
#include <Operators/Operator.h>

#include <magic_enum.hpp>

namespace DB
{
HashProbeTransformExec::HashProbeTransformExec(
    const String & req_id,
    PipelineExecutorContext & exec_context_,
    size_t op_index_,
    const JoinPtr & join_,
    BlockInputStreamPtr scan_hash_map_after_probe_stream_,
    UInt64 max_block_size_)
    : log(Logger::get(req_id))
    , exec_context(exec_context_)
    , op_index(op_index_)
    , join(join_)
    , scan_hash_map_after_probe_stream(scan_hash_map_after_probe_stream_)
    , max_block_size(max_block_size_)
{}

HashProbeTransformExecPtr HashProbeTransformExec::tryGetRestoreExec()
{
    if unlikely (exec_context.isCancelled())
        return {};

    // first check if current join has a partition to restore
    if (join->isSpilled() && join->hasPartitionToRestore())
    {
        // get a restore join
        if (auto restore_info = join->getOneRestoreStream(max_block_size); restore_info)
        {
            auto restore_probe_exec = std::make_shared<HashProbeTransformExec>(
                log->identifier(),
                exec_context,
                restore_info->stream_index,
                restore_info->join,
                restore_info->scan_hash_map_stream,
                max_block_size);
            restore_probe_exec->parent = shared_from_this();
            restore_probe_exec->probe_restore_stream = restore_info->probe_stream;

            /// Trigger build side restore task to restore and rebuild hash partition.
            /// The probe side operator just waits for the build hash partition to complete.
            ///
            /// SimplePipelineTask [IOStreamSource->HashJoinBuildSink]
            ///                       | (build hash partition)
            ///                       ▼
            ///               Interpreters::Join
            ///                       | (check build finished)
            ///                       ▼
            ///             HashProbeTransformExec
            PipelineExecBuilder build_builder;
            auto req_id = fmt::format("{} restore_build_{}", log->identifier(), restore_info->stream_index);
            build_builder.setSourceOp(
                std::make_unique<IOBlockInputStreamSourceOp>(exec_context, req_id, restore_info->build_stream));
            build_builder.setSinkOp(std::make_unique<HashJoinBuildSink>(
                exec_context,
                req_id,
                restore_info->join,
                restore_info->stream_index));
            exec_context.addOneTimeFuture(restore_info->join->wait_build_finished_future);
            exec_context.addOneTimeFuture(restore_info->join->wait_probe_finished_future);
            TaskScheduler::instance->submit(
                std::make_unique<SimplePipelineTask>(exec_context, req_id, build_builder.build()));

            return restore_probe_exec;
        }
        assert(join->hasPartitionToRestore() == false);
    }

    // current join has no more partition to restore, so check if previous join still has partition to restore
    return parent ? parent->tryGetRestoreExec() : HashProbeTransformExecPtr{};
}

void HashProbeTransformExec::startRestoreProbe()
{
    /// Trigger probe side restore task to get the block from probe_restore_stream.
    ///
    /// StreamRestoreTask [probe_restore_stream]
    ///                       | read and push
    ///                       ▼
    ///               probe_result_queue
    ///                       | pop and probe
    ///                       ▼
    ///             HashProbeTransformExec (probe restored hash partition)
    assert(!is_probe_restore_done && probe_restore_stream);
    // Use 1 as the queue_size to avoid accumulating too many blocks and causing the memory to exceed the limit.
    assert(!probe_result_queue);
    probe_result_queue = std::make_shared<ResultQueue>(1);
    TaskScheduler::instance->submit(
        std::make_unique<StreamRestoreTask>(exec_context, log->identifier(), probe_restore_stream, probe_result_queue));
    probe_restore_stream.reset();
}

bool HashProbeTransformExec::prepareProbeRestoredBlock()
{
    if (unlikely(is_probe_restore_done))
        return true;
    if (probe_restored_block)
        return true;
    assert(probe_result_queue);
    auto ret = probe_result_queue->tryPop(probe_restored_block);
    switch (ret)
    {
    case MPMCQueueResult::OK:
        return true;
    case MPMCQueueResult::EMPTY:
        return false;
    case MPMCQueueResult::FINISHED:
        is_probe_restore_done = true;
        return true;
    default:
        throw Exception(fmt::format("Unexpected result: {}", magic_enum::enum_name(ret)));
    }
}

Block HashProbeTransformExec::popProbeRestoredBlock()
{
    if (unlikely(is_probe_restore_done))
        return {};

    // `prepareProbeRestoredBlock` must have been called and return true before,
    // so `probe_restored_block` must not be empty here.
    Block ret;
    assert(probe_restored_block);
    std::swap(ret, probe_restored_block);
    assert(!probe_restored_block);
    return ret;
}

#define CONTINUE                             \
    if unlikely (exec_context.isCancelled()) \
        return OperatorStatus::CANCELLED;    \
    continue;

OperatorStatus HashProbeTransformExec::tryFillProcessInfoInRestoreProbeStage(ProbeProcessInfo & probe_process_info)
{
    while (true)
    {
        assert(probe_process_info.all_rows_joined_finish);
        if (!probe_partition_blocks.empty())
        {
            auto partition_block = std::move(probe_partition_blocks.front());
            probe_partition_blocks.pop_front();
            if (!partition_block.block || partition_block.block.rows() == 0)
            {
                CONTINUE;
            }
            join->checkTypes(partition_block.block);
            probe_process_info.resetBlock(std::move(partition_block.block), partition_block.partition_index);
            return OperatorStatus::HAS_OUTPUT;
        }
        else
        {
            if (!prepareProbeRestoredBlock())
                return OperatorStatus::WAITING;
            auto restore_ret = popProbeRestoredBlock();
            if (likely(restore_ret))
            {
                if (restore_ret.rows() == 0)
                {
                    CONTINUE;
                }
                /// Even if spill is enabled, if spill is not triggered during build,
                /// there is no need to dispatch probe block
                if (!join->isSpilled())
                {
                    join->checkTypes(restore_ret);
                    probe_process_info.resetBlock(std::move(restore_ret), 0);
                    return OperatorStatus::HAS_OUTPUT;
                }
                else
                {
                    join->dispatchProbeBlock(restore_ret, probe_partition_blocks, op_index);
                    if (hasMarkedSpillData())
                        return OperatorStatus::IO_OUT;
                }
            }
            else
            {
                return OperatorStatus::HAS_OUTPUT;
            }
        }
    }
}

OperatorStatus HashProbeTransformExec::tryFillProcessInfoInProbeStage(ProbeProcessInfo & probe_process_info)
{
    while (true)
    {
        assert(probe_process_info.all_rows_joined_finish);
        if (!probe_partition_blocks.empty())
        {
            auto partition_block = std::move(probe_partition_blocks.front());
            probe_partition_blocks.pop_front();
            if (!partition_block.block || partition_block.block.rows() == 0)
            {
                CONTINUE;
            }
            join->checkTypes(partition_block.block);
            probe_process_info.resetBlock(std::move(partition_block.block), partition_block.partition_index);
            return OperatorStatus::HAS_OUTPUT;
        }
        else
        {
            return OperatorStatus::NEED_INPUT;
        }
    }
}

#undef CONTINUE

OperatorStatus HashProbeTransformExec::tryFillProcessInfoInProbeStage(
    ProbeProcessInfo & probe_process_info,
    Block & input)
{
    assert(probe_process_info.all_rows_joined_finish);
    assert(probe_partition_blocks.empty());
    if (likely(input))
    {
        if (input.rows() == 0)
            return OperatorStatus::NEED_INPUT;
        /// Even if spill is enabled, if spill is not triggered during build,
        /// there is no need to dispatch probe block
        if (!join->isSpilled())
        {
            join->checkTypes(input);
            probe_process_info.resetBlock(std::move(input), 0);
            return OperatorStatus::HAS_OUTPUT;
        }
        else
        {
            join->dispatchProbeBlock(input, probe_partition_blocks, op_index);
            input.clear();
            if (hasMarkedSpillData())
                return OperatorStatus::IO_OUT;
            return tryFillProcessInfoInProbeStage(probe_process_info);
        }
    }
    else
    {
        return OperatorStatus::HAS_OUTPUT;
    }
}
} // namespace DB
