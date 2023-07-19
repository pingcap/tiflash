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

#include <Flash/Executor/PipelineExecutorContext.h>
#include <Operators/HashJoinProbeTransformOp.h>

#include <magic_enum.hpp>

#include "Operators/Operator.h"
#include "Operators/ProbeTransformExec.h"

namespace DB
{
HashJoinProbeTransformOp::HashJoinProbeTransformOp(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    const JoinPtr & join_,
    size_t op_index_,
    size_t max_block_size,
    const Block & input_header)
    : TransformOp(exec_context_, req_id)
    , origin_join(join_)
    , probe_process_info(max_block_size)
{
    RUNTIME_CHECK_MSG(origin_join != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(origin_join->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");

    BlockInputStreamPtr scan_hash_map_after_probe_stream;
    if (needScanHashMapAfterProbe(origin_join->getKind()))
        scan_hash_map_after_probe_stream = origin_join->createScanHashMapAfterProbeStream(input_header, op_index_, origin_join->getProbeConcurrency(), max_block_size);
    probe_transform = std::make_shared<ProbeTransformExec>(exec_context_, op_index_, origin_join, scan_hash_map_after_probe_stream, max_block_size);
}

void HashJoinProbeTransformOp::transformHeaderImpl(Block & header_)
{
    ProbeProcessInfo header_probe_process_info(0);
    header_probe_process_info.resetBlock(std::move(header_));
    header_ = origin_join->joinBlock(header_probe_process_info, true);
}

void HashJoinProbeTransformOp::operateSuffixImpl()
{
    LOG_DEBUG(log, "Finish join probe, total output rows {}, joined rows {}, scan hash map rows {}", joined_rows + scan_hash_map_rows, joined_rows, scan_hash_map_rows);
}

OperatorStatus HashJoinProbeTransformOp::onOutput(Block & block)
{
    while (true)
    {
        switch (status)
        {
        case ProbeStatus::PROBE:
            if unlikely (probe_process_info.all_rows_joined_finish)
            {
                if (probe_transform->finishOneProbe())
                {
                    if (probe_transform->hasMarkedSpillData())
                    {
                        status = ProbeStatus::PROBE_FINAL_SPILL;
                        break;
                    }
                    probe_transform->finalizeProbe();
                }
                status = probe_transform->needScanHashMapAfterProbe() && !probe_transform->isSpilled()
                    ? ProbeStatus::WAIT_PROBE_FINISH
                    : ProbeStatus::FINISHED;
                break;
            }
            block = probe_transform->joinBlock(probe_process_info);
            joined_rows += block.rows();
            return OperatorStatus::HAS_OUTPUT;
        case ProbeStatus::PROBE_FINAL_SPILL:
            return OperatorStatus::IO_OUT;
        case ProbeStatus::READ_SCAN_HASH_MAP_DATA:
            block = probe_transform->scanNonJoined();
            if unlikely (!block)
            {
                probe_transform->endNonJoined();
                status = probe_transform->isSpilled() ? ProbeStatus::GET_RESTORE_JOIN : ProbeStatus::FINISHED;
                break;
            }
            scan_hash_map_rows += block.rows();
            return OperatorStatus::HAS_OUTPUT;
        case ProbeStatus::WAIT_PROBE_FINISH:
            if (probe_transform->isAllProbeFinished())
            {
                if (probe_transform->needScanHashMapAfterProbe())
                {
                    probe_transform->startNonJoined();
                    status = ProbeStatus::READ_SCAN_HASH_MAP_DATA;
                }
                else if (probe_transform->isSpilled())
                {
                    status = ProbeStatus::GET_RESTORE_JOIN;
                }
                else
                {
                    status = ProbeStatus::FINISHED;
                }
                break;
            }
            return OperatorStatus::WAITING;
        case ProbeStatus::GET_RESTORE_JOIN:
            if (auto restore_exec = probe_transform->tryGetRestoreExec(); probe_transform)
            {
                probe_transform = restore_exec;
                status = ProbeStatus::RESTORE_BUILD;
            }
            else
            {
                status = ProbeStatus::FINISHED;
            }
            break;
        case ProbeStatus::RESTORE_BUILD:
            if (probe_transform->isAllBuildFinished())
            {
                status = ProbeStatus::PROBE;
                break;
            }
            return OperatorStatus::WAITING;
        case ProbeStatus::FINISHED:
            return OperatorStatus::HAS_OUTPUT;
        }
    }
}

bool HashJoinProbeTransformOp::fillProcessInfoFromPartitoinBlocks()
{
    if (!probe_partition_blocks.empty())
    {
        auto partition_block = std::move(probe_partition_blocks.front());
        probe_partition_blocks.pop_front();
        origin_join->checkTypes(partition_block.block);
        probe_process_info.resetBlock(std::move(partition_block.block), partition_block.partition_index);
        return true;
    }
    return false;
}

OperatorStatus HashJoinProbeTransformOp::transformImpl(Block & block)
{
    assert(status == ProbeStatus::PROBE);
    assert(probe_process_info.all_rows_joined_finish);
    if likely (block)
    {
        /// Even if spill is enabled, if spill is not triggered during build,
        /// there is no need to dispatch probe block
        if (!probe_transform->isSpilled())
        {
            origin_join->checkTypes(block);
            probe_process_info.resetBlock(std::move(block), 0);
        }
        else
        {
            assert(probe_partition_blocks.empty());
            probe_transform->dispatchBlock(block, probe_partition_blocks);
            auto fill_ret = fillProcessInfoFromPartitoinBlocks();
            if (probe_transform->hasMarkedSpillData())
                return OperatorStatus::IO_OUT;
            if (!fill_ret)
                return OperatorStatus::NEED_INPUT;
        }
    }

    return onOutput(block);
}

OperatorStatus HashJoinProbeTransformOp::tryOutputImpl(Block & block)
{
    if (status == ProbeStatus::PROBE && probe_process_info.all_rows_joined_finish)
    {
        if (!fillProcessInfoFromPartitoinBlocks())
            return OperatorStatus::NEED_INPUT;
    }

    return onOutput(block);
}

OperatorStatus HashJoinProbeTransformOp::awaitImpl()
{
    switch (status)
    {
    case ProbeStatus::WAIT_PROBE_FINISH:
        if (probe_transform->isAllProbeFinished())
        {
            status = ProbeStatus::READ_SCAN_HASH_MAP_DATA;
            probe_transform->startNonJoined();
            return OperatorStatus::HAS_OUTPUT;
        }
        else
        {
            return OperatorStatus::WAITING;
        }
    case ProbeStatus::RESTORE_BUILD:
        if (probe_transform->isAllBuildFinished())
        {
            status = ProbeStatus::PROBE;
            return OperatorStatus::HAS_OUTPUT;
        }
        else
        {
            return OperatorStatus::WAITING;
        }
    default:
        throw Exception(fmt::format("Unexpected status: {}", magic_enum::enum_name(status)));
    }
}

OperatorStatus HashJoinProbeTransformOp::executeIOImpl()
{
    switch (status)
    {
    case ProbeStatus::PROBE:
        probe_transform->flushMarkedSpillData();
        return OperatorStatus::NEED_INPUT;
    case ProbeStatus::PROBE_FINAL_SPILL:
        probe_transform->flushMarkedSpillData(/*is_the_last=*/true);
        probe_transform->finalizeProbe();
        status = ProbeStatus::WAIT_PROBE_FINISH;
        return OperatorStatus::HAS_OUTPUT;
    default:
        throw Exception(fmt::format("Unexpected status: {}", magic_enum::enum_name(status)));
    }
}
} // namespace DB
