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

#include <DataStreams/HashJoinProbeExec.h>
#include <DataStreams/IProfilingBlockInputStream.h>
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
        size_t stream_index,
        const String & req_id,
        UInt64 max_block_size_);

    String getName() const override { return name; }
    Block getHeader() const override;
    void cancel(bool kill) override;

protected:
    Block readImpl() override;

    void readSuffixImpl() override;

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
     *                  |------------------->  WAIT_BUILD_FINISH
     *                  |                              |
     *                  |                              ▼
     *                  |                            PROBE
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
     *
     */
    enum class ProbeStatus
    {
        WAIT_BUILD_FINISH, /// wait build finish
        PROBE, /// probe for both init probe and restore probe
        WAIT_PROBE_FINISH, /// wait probe finish
        GET_RESTORE_JOIN, /// try to get restore join
        RESTORE_BUILD, /// build for restore join
        READ_SCAN_HASH_MAP_DATA, /// output scan hash map after probe data
        FINISHED, /// the final state
    };

    void switchStatus(ProbeStatus to);
    Block getOutputBlock();
    std::tuple<size_t, Block> getOneProbeBlock();
    void onCurrentProbeDone();
    void onAllProbeDone();
    void onCurrentScanHashMapDone();
    void tryGetRestoreJoin();

private:
    const LoggerPtr log;
    JoinPtr original_join;
    /// probe_exec can be modified during the runtime,
    /// although read/write to those are almost only in 1 thread,
    /// but an exception is cancel thread will read them,
    /// so need to use HashJoinProbeExecHolder protect the multi-threads access.
    HashJoinProbeExecHolder probe_exec;
    ProbeStatus status{ProbeStatus::WAIT_BUILD_FINISH};
    size_t joined_rows = 0;
    size_t scan_hash_map_rows = 0;

    Block header;
};

} // namespace DB
