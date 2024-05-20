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
#include <Operators/HashJoinProbeTransformOp.h>

#include <magic_enum.hpp>

namespace DB
{
#define BREAK                                \
    if unlikely (exec_context.isCancelled()) \
        return OperatorStatus::CANCELLED;    \
    break

HashJoinProbeTransformOp::HashJoinProbeTransformOp(
    PipelineExecutorContext & exec_context_,
    const String & req_id,
    const JoinPtr & join_,
    size_t op_index_,
    size_t max_block_size,
    const Block & input_header)
    : TransformOp(exec_context_, req_id)
    , origin_join(join_)
    , probe_process_info(max_block_size, join_->getProbeCacheColumnThreshold())
{
    RUNTIME_CHECK_MSG(origin_join != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(origin_join->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");

    BlockInputStreamPtr scan_hash_map_after_probe_stream;
    if (needScanHashMapAfterProbe(origin_join->getKind()))
        scan_hash_map_after_probe_stream = origin_join->createScanHashMapAfterProbeStream(
            input_header,
            op_index_,
            origin_join->getProbeConcurrency(),
            max_block_size);
    probe_transform = std::make_shared<HashProbeTransformExec>(
        req_id,
        exec_context_,
        op_index_,
        origin_join,
        scan_hash_map_after_probe_stream,
        max_block_size);
}

void HashJoinProbeTransformOp::transformHeaderImpl(Block & header_)
{
    ProbeProcessInfo header_probe_process_info(0, 0);
    header_probe_process_info.resetBlock(std::move(header_));
    header_ = origin_join->joinBlock(header_probe_process_info, true);
}

void HashJoinProbeTransformOp::operateSuffixImpl()
{
    LOG_DEBUG(
        log,
        "Finish join probe, total output rows {}, joined rows {}, scan hash map rows {}",
        joined_rows + scan_hash_map_rows,
        joined_rows,
        scan_hash_map_rows);
}

OperatorStatus HashJoinProbeTransformOp::onOutput(Block & block)
{
    while (true)
    {
        switch (status)
        {
        case ProbeStatus::RESTORE_PROBE:
            // fill probe_process_info for restore probe stage.
            // The subsequent logic is the same as the probe stage.
            if (probe_process_info.all_rows_joined_finish)
            {
                if (auto ret = probe_transform->tryFillProcessInfoInRestoreProbeStage(probe_process_info);
                    ret != OperatorStatus::HAS_OUTPUT)
                    return ret;
            }
        case ProbeStatus::PROBE:
            // For the probe/restore_probe phase, probe_process_info has been filled,
            // if all_rows_joined_finish is still true here, it means that there is no input block.
            if unlikely (probe_process_info.all_rows_joined_finish)
            {
                if (probe_transform->finishOneProbe())
                {
                    if (probe_transform->hasMarkedSpillData())
                    {
                        switchStatus(ProbeStatus::PROBE_FINAL_SPILL);
                        BREAK;
                    }
                    probe_transform->finalizeProbe();
                }
                auto next_status = (probe_transform->needScanHashMapAfterProbe() || probe_transform->shouldRestore())
                    ? ProbeStatus::WAIT_PROBE_FINISH
                    : ProbeStatus::FINISHED;
                switchStatus(next_status);
                BREAK;
            }
            block = probe_transform->joinBlock(probe_process_info);
            joined_rows += block.rows();
            return OperatorStatus::HAS_OUTPUT;
        case ProbeStatus::PROBE_FINAL_SPILL:
            return OperatorStatus::IO_OUT;
        case ProbeStatus::READ_SCAN_HASH_MAP_DATA:
            block = probe_transform->scanHashMapAfterProbe();
            if unlikely (!block)
            {
                probe_transform->endScanHashMapAfterProbe();
                auto next_status
                    = probe_transform->shouldRestore() ? ProbeStatus::GET_RESTORE_JOIN : ProbeStatus::FINISHED;
                switchStatus(next_status);
                BREAK;
            }
            scan_hash_map_rows += block.rows();
            return OperatorStatus::HAS_OUTPUT;
        case ProbeStatus::WAIT_PROBE_FINISH:
            if (probe_transform->quickCheckProbeFinished())
            {
                onWaitProbeFinishDone();
                BREAK;
            }
            return OperatorStatus::WAITING;
        case ProbeStatus::GET_RESTORE_JOIN:
            onGetRestoreJoin();
            BREAK;
        case ProbeStatus::RESTORE_BUILD:
            if (probe_transform->quickCheckBuildFinished())
            {
                onRestoreBuildFinish();
                BREAK;
            }
            return OperatorStatus::WAITING;
        case ProbeStatus::FINISHED:
            block = {};
            return OperatorStatus::HAS_OUTPUT;
        }
    }
}

OperatorStatus HashJoinProbeTransformOp::transformImpl(Block & block)
{
    assert(status == ProbeStatus::PROBE);
    assert(probe_process_info.all_rows_joined_finish);
    if (auto ret = probe_transform->tryFillProcessInfoInProbeStage(probe_process_info, block);
        ret != OperatorStatus::HAS_OUTPUT)
        return ret;

    return onOutput(block);
}

OperatorStatus HashJoinProbeTransformOp::tryOutputImpl(Block & block)
{
    if (status == ProbeStatus::PROBE && probe_process_info.all_rows_joined_finish)
    {
        if (auto ret = probe_transform->tryFillProcessInfoInProbeStage(probe_process_info);
            ret != OperatorStatus::HAS_OUTPUT)
            return ret;
    }

    return onOutput(block);
}

void HashJoinProbeTransformOp::onWaitProbeFinishDone()
{
    if (probe_transform->needScanHashMapAfterProbe())
    {
        probe_transform->startScanHashMapAfterProbe();
        switchStatus(ProbeStatus::READ_SCAN_HASH_MAP_DATA);
    }
    else if (probe_transform->shouldRestore())
    {
        switchStatus(ProbeStatus::GET_RESTORE_JOIN);
    }
    else
    {
        switchStatus(ProbeStatus::FINISHED);
    }
}

void HashJoinProbeTransformOp::onRestoreBuildFinish()
{
    probe_transform->startRestoreProbe();
    switchStatus(ProbeStatus::RESTORE_PROBE);
}

void HashJoinProbeTransformOp::onGetRestoreJoin()
{
    if (auto restore_exec = probe_transform->tryGetRestoreExec(); restore_exec)
    {
        probe_transform = restore_exec;
        switchStatus(ProbeStatus::RESTORE_BUILD);
    }
    else
    {
        switchStatus(ProbeStatus::FINISHED);
    }
}

OperatorStatus HashJoinProbeTransformOp::awaitImpl()
{
    while (true)
    {
        switch (status)
        {
        case ProbeStatus::WAIT_PROBE_FINISH:
            if (probe_transform->quickCheckProbeFinished())
            {
                onWaitProbeFinishDone();
                BREAK;
            }
            return OperatorStatus::WAITING;
        case ProbeStatus::GET_RESTORE_JOIN:
            onGetRestoreJoin();
            BREAK;
        case ProbeStatus::RESTORE_BUILD:
            if (probe_transform->quickCheckBuildFinished())
            {
                onRestoreBuildFinish();
                BREAK;
            }
            return OperatorStatus::WAITING;
        case ProbeStatus::READ_SCAN_HASH_MAP_DATA:
        case ProbeStatus::FINISHED:
            return OperatorStatus::HAS_OUTPUT;
        default:
            throw Exception(fmt::format("Unexpected status: {}", magic_enum::enum_name(status)));
        }
    }
}

OperatorStatus HashJoinProbeTransformOp::executeIOImpl()
{
    switch (status)
    {
    case ProbeStatus::PROBE:
    case ProbeStatus::RESTORE_PROBE:
        probe_transform->flushMarkedSpillData();
        return OperatorStatus::NEED_INPUT;
    case ProbeStatus::PROBE_FINAL_SPILL:
        probe_transform->flushMarkedSpillData();
        probe_transform->finalizeProbe();
        switchStatus(ProbeStatus::WAIT_PROBE_FINISH);
        return OperatorStatus::WAITING;
    default:
        throw Exception(fmt::format("Unexpected status: {}", magic_enum::enum_name(status)));
    }
}

#undef BREAK

void HashJoinProbeTransformOp::switchStatus(ProbeStatus to)
{
    LOG_TRACE(log, fmt::format("{} -> {}", magic_enum::enum_name(status), magic_enum::enum_name(to)));
    status = to;
}
} // namespace DB
