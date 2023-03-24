// Copyright 2022 PingCAP, Ltd.
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

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/SquashingHashJoinBlockTransform.h>
#include <Interpreters/Join.h>

namespace DB
{
/** Executes a certain expression over the block.
  * Basically the same as ExpressionBlockInputStream,
  * but requires that there must be a join probe action in the Expression.
  *
  * The join probe action is different from the general expression
  * and needs to be executed after join hash map building.
  * We should separate it from the ExpressionBlockInputStream.
  */
class HashJoinProbeBlockInputStream : public IProfilingBlockInputStream
{
private:
    static constexpr auto name = "HashJoinProbe";

public:
    HashJoinProbeBlockInputStream(
        const BlockInputStreamPtr & input,
        const JoinPtr & join_,
        size_t non_joined_stream_index,
        const String & req_id,
        UInt64 max_block_size_);

    String getName() const override { return name; }
    Block getHeader() const override;
    void cancel(bool kill) override;

protected:
    Block readImpl() override;

private:
    /*
     *   spill not enabled:
     *                                  WAIT_BUILD_FINISH
     *                                          |
     *                                          ▼
     *                                        PROBE
     *                                          |
     *                                          ▼
     *                                  -----------------
     *              has non_joined data |               | no non_joined data
     *                                  ▼               ▼
     *                         WAIT_PROBE_FINISH     FINISHED
     *                                  |
     *                                  ▼
     *                        READ_NON_JOINED_DATA
     *                                  |
     *                                  ▼
     *                               FINISHED
     *
     *   spill enabled:
     *                  |-------------------> WAIT_BUILD_FINISH
     *                  |                             |
     *                  |                             ▼
     *                  |                           PROBE
     *                  |                             |
     *                  |                             ▼
     *                  |                    WAIT_PROBE_FINISH
     *                  |                             |
     *                  |                             ▼
     *                  |                      ---------------
     *                  |  has non_joined data |             | no non_joined data
     *                  |                      ▼             |
     *                  |             READ_NON_JOINED_DATA   |
     *                  |                      \             /
     *                  |                       \           /
     *                  |                        \         /
     *                  |                         \       /
     *                  |                          \     /
     *                  |                           \   /
     *                  |                            \ /
     *                  |                             ▼
     *                  |                      GET_RESTORE_JOIN
     *                  |                             |
     *                  |                             ▼
     *                  |                      ---------------
     *                  |    has restored join |             | no restored join
     *                  |                      ▼             ▼
     *                  |                RESTORE_BUILD    FINISHED
     *                  |                      |
     *                  -----------------------|
     *
     */
    enum class ProbeStatus
    {
        WAIT_BUILD_FINISH, /// wait build finish
        PROBE, /// probe for both init probe and restore probe
        WAIT_PROBE_FINISH, /// wait probe finish
        GET_RESTORE_JOIN, /// try to get restore join
        RESTORE_BUILD, /// build for restore join
        READ_NON_JOINED_DATA, /// output non joined data
        FINISHED, /// the final state
    };

    Block getOutputBlock();
    std::tuple<size_t, Block> getOneProbeBlock();
    void onCurrentProbeDone();
    void onAllProbeDone();
    void onCurrentReadNonJoinedDataDone();
    void tryGetRestoreJoin();
    void readSuffixImpl() override;
    const LoggerPtr log;
    /// join/non_joined_stream/restore_build_stream/restore_probe_stream can be modified during the runtime
    /// although read/write to those are almost only in 1 thread, but an exception is cancel thread will
    /// read them, so need to protect the multi-threads access
    std::mutex mutex;
    JoinPtr original_join;
    JoinPtr join;
    const bool need_output_non_joined_data;
    size_t current_non_joined_stream_index;
    BlockInputStreamPtr current_probe_stream;
    UInt64 max_block_size;
    ProbeProcessInfo probe_process_info;
    BlockInputStreamPtr non_joined_stream;
    BlockInputStreamPtr restore_build_stream;
    BlockInputStreamPtr restore_probe_stream;
    ProbeStatus status{ProbeStatus::WAIT_BUILD_FINISH};
    size_t joined_rows = 0;
    size_t non_joined_rows = 0;
    std::list<JoinPtr> parents;
    std::list<std::tuple<size_t, Block>> probe_partition_blocks;
};

} // namespace DB
