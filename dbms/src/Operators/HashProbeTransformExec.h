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

#include <Interpreters/Join.h>
#include <Operators/Operator.h>

namespace DB
{
class HashProbeTransformExec;
using HashProbeTransformExecPtr = std::shared_ptr<HashProbeTransformExec>;

class PipelineExecutorContext;

class SharedQueueSourceHolder;
using SharedQueueSourceHolderPtr = std::shared_ptr<SharedQueueSourceHolder>;

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

    // For ScanHashMapAfterProbe stage
    bool needScanHashMapAfterProbe() const
    {
        return !scan_hashmap_after_probe_finished && scan_hash_map_after_probe_stream != nullptr;
    }
    void startScanHashMapAfterProbe()
    {
        assert(needScanHashMapAfterProbe());
        scan_hash_map_after_probe_stream->readPrefix();
    }
    Block scanHashMapAfterProbe()
    {
        assert(needScanHashMapAfterProbe());
        return scan_hash_map_after_probe_stream->read();
    }
    void endScanHashMapAfterProbe()
    {
        assert(needScanHashMapAfterProbe());
        scan_hash_map_after_probe_stream->readSuffix();
        join->finishOneNonJoin(op_index);
        scan_hashmap_after_probe_finished = true;
    }

    // For probe stage
    Block joinBlock(ProbeProcessInfo & probe_process_info) { return join->joinBlock(probe_process_info); }
    void dispatchBlock(Block & block, PartitionBlocks & partition_blocks_list)
    {
        join->dispatchProbeBlock(block, partition_blocks_list, op_index);
    }
    bool finishOneProbe() { return join->finishOneProbe(op_index); }
    bool hasMarkedSpillData() const { return join->hasProbeSideMarkedSpillData(op_index); }
    bool quickCheckProbeFinished() const { return join->quickCheckProbeFinished(); }
    void finalizeProbe() { join->finalizeProbe(); }
    void flushMarkedSpillData() { join->flushProbeSideMarkedSpillData(op_index); }

    // For restore build stage
    bool quickCheckBuildFinished() const { return join->quickCheckBuildFinished(); }

    // For restore probe stage
    void startRestoreProbe();

    bool shouldRestore() const { return join->isSpilled() || join->isRestoreJoin(); }

    HashProbeTransformExecPtr tryGetRestoreExec();

    OperatorStatus tryFillProcessInfoInRestoreProbeStage(ProbeProcessInfo & probe_process_info);

    OperatorStatus tryFillProcessInfoInProbeStage(ProbeProcessInfo & probe_process_info);
    OperatorStatus tryFillProcessInfoInProbeStage(ProbeProcessInfo & probe_process_info, Block & input);

private:
    // For restore build stage
    bool prepareProbeRestoredBlock();

    // For restore probe stage
    Block popProbeRestoredBlock();

private:
    LoggerPtr log;

    PipelineExecutorContext & exec_context;

    size_t op_index;

    JoinPtr join;

    BlockInputStreamPtr scan_hash_map_after_probe_stream;
    bool scan_hashmap_after_probe_finished = false;

    PartitionBlocks probe_partition_blocks;

    const UInt64 max_block_size;

    HashProbeTransformExecPtr parent;

    // For restore probe.
    SharedQueueSourceHolderPtr probe_source_holder;
    BlockInputStreamPtr probe_restore_stream;
    Block probe_restored_block;
    bool is_probe_restore_done = false;
};
} // namespace DB
