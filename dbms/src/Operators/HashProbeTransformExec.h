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

#pragma once

#include <Flash/Executor/ResultQueue.h>
#include <Interpreters/Join.h>
#include <Operators/Operator.h>

namespace DB
{
class HashProbeTransformExec;
using HashProbeTransformExecPtr = std::shared_ptr<HashProbeTransformExec>;

class PipelineExecutorContext;

class HashProbeTransformExec : public std::enable_shared_from_this<HashProbeTransformExec>
{
public:
    HashProbeTransformExec(
        const String & req_id,
        PipelineExecutorContext & exec_context_,
        size_t op_index_,
        const JoinPtr & join_,
        BlockInputStreamPtr scan_hash_map_after_probe_stream_,
        UInt64 max_block_size_);

    // For NonJoined stage
    bool needScanHashMapAfterProbe() const { return scan_hash_map_after_probe_stream != nullptr; }
    void startNonJoined()
    {
        assert(scan_hash_map_after_probe_stream);
        scan_hash_map_after_probe_stream->readPrefix();
    }
    Block scanNonJoined()
    {
        assert(scan_hash_map_after_probe_stream);
        return scan_hash_map_after_probe_stream->read();
    }
    void endNonJoined()
    {
        assert(scan_hash_map_after_probe_stream);
        scan_hash_map_after_probe_stream->readSuffix();
        join->finishOneNonJoin(op_index);
    }

    // For probe stage
    Block joinBlock(ProbeProcessInfo & probe_process_info) { return join->joinBlock(probe_process_info); }
    void dispatchBlock(Block & block, PartitionBlocks & partition_blocks_list) { join->dispatchProbeBlock(block, partition_blocks_list, op_index); }
    bool finishOneProbe() { return join->finishOneProbe(op_index); }
    bool hasMarkedSpillData() const { return join->hasProbeSideMarkedSpillData(op_index); }
    bool isAllProbeFinished() const { return join->isAllProbeFinished(); }
    void finalizeProbe() { join->finalizeProbe(); }
    void flushMarkedSpillData(bool is_the_last = false) { join->flushProbeSideMarkedSpillData(op_index, is_the_last); }

    // For restore build stage
    bool isAllBuildFinished() const { return join->isAllBuildFinished(); }

    // For restore probe stage
    void startRestoreProbe();
    bool isProbeRestoreReady();

    bool shouldRestore() const { return join->isEnableSpill() && (join->isSpilled() || join->isRestoreJoin()); }

    HashProbeTransformExecPtr tryGetRestoreExec();

    OperatorStatus tryFillProcessInfoInRestoreProbeStage(ProbeProcessInfo & probe_process_info);

    OperatorStatus tryFillProcessInfoInProbeStage(ProbeProcessInfo & probe_process_info);
    OperatorStatus tryFillProcessInfoInProbeStage(ProbeProcessInfo & probe_process_info, Block & input);

private:
    // For restore probe stage
    Block popProbeRestoreBlock();

private:
    LoggerPtr log;

    PipelineExecutorContext & exec_context;

    size_t op_index;

    JoinPtr join;

    BlockInputStreamPtr scan_hash_map_after_probe_stream;

    PartitionBlocks probe_partition_blocks;

    const UInt64 max_block_size;

    HashProbeTransformExecPtr parent;

    // For restore probe.
    ResultQueuePtr probe_result_queue;
    BlockInputStreamPtr probe_restore_stream;
    Block probe_restore_block;
    bool is_probe_restore_done = false;
};
} // namespace DB
