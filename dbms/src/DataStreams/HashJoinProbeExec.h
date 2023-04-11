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

#include <Common/PtrHolder.h>
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Join.h>

#include <memory>

#pragma once

namespace DB
{
class HashJoinProbeExec;
using HashJoinProbeExecPtr = std::shared_ptr<HashJoinProbeExec>;

class HashJoinProbeExec : public std::enable_shared_from_this<HashJoinProbeExec>
{
public:
    static HashJoinProbeExecPtr build(
        const JoinPtr & join,
        const BlockInputStreamPtr & probe_stream,
        size_t non_joined_stream_index,
        size_t max_block_size);

    using CancellationHook = std::function<bool()>;

    HashJoinProbeExec(
        const JoinPtr & join_,
        const BlockInputStreamPtr & restore_build_stream_,
        const BlockInputStreamPtr & probe_stream_,
        bool need_output_non_joined_data_,
        size_t non_joined_stream_index_,
        const BlockInputStreamPtr & non_joined_stream_,
        size_t max_block_size_);

    void waitUntilAllBuildFinished();

    void waitUntilAllProbeFinished();

    HashJoinProbeExecPtr tryGetRestoreExec();

    void cancel();

    void meetError(const String & error_message);

    void restoreBuild();

    void onProbeStart();
    // Returns empty block if probe finish.
    Block probe();
    // Returns true if the probe_exec ends.
    // Returns false if the probe_exec continues to execute.
    bool onProbeFinish();

    bool needOutputNonJoinedData() { return need_output_non_joined_data; }
    void onNonJoinedStart();
    Block fetchNonJoined();
    // Returns true if the probe_exec ends.
    // Returns false if the probe_exec continues to execute.
    bool onNonJoinedFinish();

    void setCancellationHook(CancellationHook cancellation_hook)
    {
        is_cancelled = std::move(cancellation_hook);
    }

private:
    PartitionBlock getProbeBlock();

    HashJoinProbeExecPtr doTryGetRestoreExec();

private:
    const JoinPtr join;

    const BlockInputStreamPtr restore_build_stream;

    const BlockInputStreamPtr probe_stream;

    const bool need_output_non_joined_data;
    const size_t non_joined_stream_index;
    const BlockInputStreamPtr non_joined_stream;

    const size_t max_block_size;

    CancellationHook is_cancelled{[]() {
        return false;
    }};

    ProbeProcessInfo probe_process_info;
    PartitionBlocks probe_partition_blocks;

    HashJoinProbeExecPtr parent;
};

using HashJoinProbeExecHolder = PtrHolder<HashJoinProbeExecPtr>;
} // namespace DB
