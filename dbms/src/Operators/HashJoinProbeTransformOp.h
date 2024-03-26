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
#include <Operators/HashProbeTransformExec.h>
#include <Operators/Operator.h>

namespace DB
{
class HashJoinProbeTransformOp : public TransformOp
{
public:
    HashJoinProbeTransformOp(
        PipelineExecutorContext & exec_context_,
        const String & req_id,
        const JoinPtr & join_,
        size_t op_index_,
        size_t max_block_size,
        const Block & input_header);

    String getName() const override { return "HashJoinProbeTransformOp"; }

protected:
    OperatorStatus transformImpl(Block & block) override;

    OperatorStatus tryOutputImpl(Block & block) override;

    OperatorStatus awaitImpl() override;

    OperatorStatus executeIOImpl() override;

    void transformHeaderImpl(Block & header_) override;

    void operateSuffixImpl() override;

private:
    OperatorStatus onOutput(Block & block);

    inline void onWaitProbeFinishDone();

    inline void onRestoreBuildFinish();

    inline void onGetRestoreJoin();

    /*
     *   spill not enabled:
     *                                        PROBE
     *                                          |
     *                                          ▼
     *                                  -----------------
     *        has scan_after_probe data |               | no scan_after_probe data
     *                                  ▼               ▼
     *                         WAIT_PROBE_FINISH     FINISHED
     *                                  |
     *                                  ▼
     *                        READ_SCAN_HASH_MAP_DATA
     *                                  |
     *                                  ▼
     *                               FINISHED
     *
     *   spill enabled:
     *                  |------------------->  PROBE/RESTORE_PROBE
     *                  |                              |
     *                  |                              ▼
     *                  |                     PROBE_FINAL_SPILL
     *                  |                              |
     *                  |                              ▼
     *                  |                     WAIT_PROBE_FINISH
     *                  |                              |
     *                  |                              ▼
     *                  |                       ---------------
     *                  |has scan_hash_map data |             | no scan_hash_map data
     *                  |                       ▼             |
     *                  |           READ_SCAN_HASH_MAP_DATA   |
     *                  |                       \             /
     *                  |                        \           /
     *                  |                         \         /
     *                  |                          \       /
     *                  |                           \     /
     *                  |                            \   /
     *                  |                             \ /
     *                  |                              ▼
     *                  |                       GET_RESTORE_JOIN
     *                  |                              |
     *                  |                              ▼
     *                  |                       ---------------
     *                  |     has restored join |             | no restored join
     *                  |                       ▼             ▼
     *                  |                 RESTORE_BUILD    FINISHED
     *                  |                       |
     *                  ------------------------|
     */
    enum class ProbeStatus
    {
        PROBE, /// probe data
        PROBE_FINAL_SPILL, /// final spill for probe data
        WAIT_PROBE_FINISH, /// wait probe finish
        READ_SCAN_HASH_MAP_DATA, /// output scan hash map after probe data
        GET_RESTORE_JOIN, /// try to get restore join
        RESTORE_BUILD, /// build for restore join
        RESTORE_PROBE, /// probe for restore join
        FINISHED, /// the final state
    };
    inline void switchStatus(ProbeStatus to);

private:
    JoinPtr origin_join;

    HashProbeTransformExecPtr probe_transform;

    ProbeProcessInfo probe_process_info;

    size_t joined_rows = 0;
    size_t scan_hash_map_rows = 0;

    ProbeStatus status{ProbeStatus::PROBE};
};
} // namespace DB
