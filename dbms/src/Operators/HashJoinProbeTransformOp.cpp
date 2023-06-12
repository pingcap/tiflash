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

#include <Operators/HashJoinProbeTransformOp.h>

#include <magic_enum.hpp>

namespace DB
{
// TODO support spill.
HashJoinProbeTransformOp::HashJoinProbeTransformOp(
    PipelineExecutorStatus & exec_status_,
    const String & req_id,
    const JoinPtr & join_,
    size_t scan_hash_map_after_probe_stream_index_,
    size_t max_block_size,
    const Block & input_header)
    : TransformOp(exec_status_, req_id)
    , join(join_)
    , probe_process_info(max_block_size)
    , scan_hash_map_after_probe_stream_index(scan_hash_map_after_probe_stream_index_)
{
    RUNTIME_CHECK_MSG(join != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(join->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");

    if (needScanHashMapAfterProbe(join->getKind()))
        scan_hash_map_after_probe_stream = join->createScanHashMapAfterProbeStream(input_header, scan_hash_map_after_probe_stream_index, join->getProbeConcurrency(), max_block_size);
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

void HashJoinProbeTransformOp::probeOnTransform(Block & block)
{
    assert(status == ProbeStatus::PROBE);
    assert(probe_process_info.all_rows_joined_finish);
    if likely (block)
    {
        join->checkTypes(block);
        probe_process_info.resetBlock(std::move(block), 0);
        block = join->joinBlock(probe_process_info);
    }
}

OperatorStatus HashJoinProbeTransformOp::onProbeFinish(Block & block)
{
    assert(status == ProbeStatus::PROBE);
    join->finishOneProbe();
    if (scan_hash_map_after_probe_stream)
    {
        if (!join->isAllProbeFinished())
        {
            status = ProbeStatus::WAIT_PROBE_FINISH;
            return OperatorStatus::WAITING;
        }
        else
        {
            status = ProbeStatus::READ_SCAN_HASH_MAP_DATA;
            scan_hash_map_after_probe_stream->readPrefix();
            return scanHashMapData(block);
        }
    }
    else
    {
        status = ProbeStatus::FINISHED;
        return OperatorStatus::HAS_OUTPUT;
    }
}

OperatorStatus HashJoinProbeTransformOp::scanHashMapData(Block & block)
{
    assert(status == ProbeStatus::READ_SCAN_HASH_MAP_DATA);
    assert(scan_hash_map_after_probe_stream);
    block = scan_hash_map_after_probe_stream->read();
    if (!block)
    {
        scan_hash_map_after_probe_stream->readSuffix();
        status = ProbeStatus::FINISHED;
    }
    else
    {
        scan_hash_map_rows += block.rows();
    }
    return OperatorStatus::HAS_OUTPUT;
}

OperatorStatus HashJoinProbeTransformOp::handleProbedBlock(Block & block)
{
    assert(status == ProbeStatus::PROBE);
    if unlikely (!block)
        return onProbeFinish(block);

    joined_rows += block.rows();
    return OperatorStatus::HAS_OUTPUT;
}

OperatorStatus HashJoinProbeTransformOp::transformImpl(Block & block)
{
    assert(status == ProbeStatus::PROBE);
    probeOnTransform(block);
    return handleProbedBlock(block);
}

OperatorStatus HashJoinProbeTransformOp::tryOutputImpl(Block & block)
{
    switch (status)
    {
    case ProbeStatus::PROBE:
        if (probe_process_info.all_rows_joined_finish)
            return OperatorStatus::NEED_INPUT;

        block = join->joinBlock(probe_process_info);
        return handleProbedBlock(block);
    case ProbeStatus::READ_SCAN_HASH_MAP_DATA:
        return scanHashMapData(block);
    case ProbeStatus::FINISHED:
        return OperatorStatus::HAS_OUTPUT;
    default:
        throw Exception(fmt::format("Unexpected status: {}.", magic_enum::enum_name(status)));
    }
}

OperatorStatus HashJoinProbeTransformOp::awaitImpl()
{
    if likely (status == ProbeStatus::WAIT_PROBE_FINISH)
    {
        if (join->isAllProbeFinished())
        {
            status = ProbeStatus::READ_SCAN_HASH_MAP_DATA;
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
