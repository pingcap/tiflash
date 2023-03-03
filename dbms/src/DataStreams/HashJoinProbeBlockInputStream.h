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
        size_t probe_index,
        const String & req_id,
        UInt64 max_block_size);

    String getName() const override { return name; }
    Block getTotals() override;
    Block getHeader() const override;
    void cancel(bool kill) override;

protected:
    Block readImpl() override;
    Block getOutputBlock();

private:
    enum class ProbeStatus
    {
        PROBE,
        WAIT_FOR_READ_NON_JOINED_DATA,
        READ_NON_JOINED_DATA,
        FINISHED,
    };
    void readSuffixImpl() override;
    void finishOneProbe();

    const LoggerPtr log;
    JoinPtr join;
    size_t probe_index;
    ProbeProcessInfo probe_process_info;
    BlockInputStreamPtr non_joined_stream;
    ProbeStatus status{ProbeStatus::PROBE};
    size_t joined_rows = 0;
    size_t non_joined_rows = 0;
    std::atomic<bool> probe_finished = false;
};

} // namespace DB
