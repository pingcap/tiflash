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

#include <Operators/HashJoinProbeTransformOp.h>

#include <magic_enum.hpp>

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
    , join(join_)
    , probe_process_info(max_block_size)
    , op_index(op_index_)
{
    RUNTIME_CHECK_MSG(join != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(join->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");

    if (needScanHashMapAfterProbe(join->getKind()))
        scan_hash_map_after_probe_stream = join->createScanHashMapAfterProbeStream(input_header, op_index, join->getProbeConcurrency(), max_block_size);
}

void HashJoinProbeTransformOp::transformHeaderImpl(Block & header_)
{
    ProbeProcessInfo header_probe_process_info(0);
    header_probe_process_info.resetBlock(std::move(header_));
    header_ = join->joinBlock(header_probe_process_info, true);
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
                if (join->finishOneProbe(op_index))
                {
                    // TODO support spill.
                    RUNTIME_CHECK(!join->hasProbeSideMarkedSpillData(op_index));
                    join->finalizeProbe();
                }
                status = scan_hash_map_after_probe_stream
                    ? ProbeStatus::WAIT_PROBE_FINISH
                    : ProbeStatus::FINISHED;
                break;
            }
            block = join->joinBlock(probe_process_info);
            joined_rows += block.rows();
            return OperatorStatus::HAS_OUTPUT;
        case ProbeStatus::READ_SCAN_HASH_MAP_DATA:
            block = scan_hash_map_after_probe_stream->read();
            if unlikely (!block)
            {
                scan_hash_map_after_probe_stream->readSuffix();
                status = ProbeStatus::FINISHED;
                break;
            }
            scan_hash_map_rows += block.rows();
            return OperatorStatus::HAS_OUTPUT;
        case ProbeStatus::WAIT_PROBE_FINISH:
            if (join->isAllProbeFinished())
            {
                status = ProbeStatus::READ_SCAN_HASH_MAP_DATA;
                scan_hash_map_after_probe_stream->readPrefix();
                break;
            }
            return OperatorStatus::WAITING;
        case ProbeStatus::FINISHED:
            return OperatorStatus::HAS_OUTPUT;
        }
    }
}

OperatorStatus HashJoinProbeTransformOp::transformImpl(Block & block)
{
    assert(status == ProbeStatus::PROBE);
    assert(probe_process_info.all_rows_joined_finish);
    if likely (block)
    {
        join->checkTypes(block);
        probe_process_info.resetBlock(std::move(block), 0);
        assert(!probe_process_info.all_rows_joined_finish);
    }

    return onOutput(block);
}

OperatorStatus HashJoinProbeTransformOp::tryOutputImpl(Block & block)
{
    if (status == ProbeStatus::PROBE && probe_process_info.all_rows_joined_finish)
        return OperatorStatus::NEED_INPUT;

    return onOutput(block);
}

OperatorStatus HashJoinProbeTransformOp::awaitImpl()
{
    if likely (status == ProbeStatus::WAIT_PROBE_FINISH)
    {
        if (join->isAllProbeFinished())
        {
            status = ProbeStatus::READ_SCAN_HASH_MAP_DATA;
            scan_hash_map_after_probe_stream->readPrefix();
            return OperatorStatus::HAS_OUTPUT;
        }
        else
        {
            return OperatorStatus::WAITING;
        }
    }
    return OperatorStatus::NEED_INPUT;
}
} // namespace DB
