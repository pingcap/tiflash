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

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Interpreters/Join.h>

#include <memory>

#pragma once

namespace DB
{
class HashJoinProbeExec;
using HashJoinProbeExecPtr = std::shared_ptr<HashJoinProbeExec>;

class HashJoinProbeExec
{
public:
    JoinPtr join;

    BlockInputStreamPtr restore_build_stream;

    BlockInputStreamPtr probe_stream;

    bool need_output_non_joined_data;
    size_t non_joined_stream_index;
    BlockInputStreamPtr non_joined_stream;

    size_t max_block_size;

    std::list<std::tuple<size_t, Block>> probe_partition_blocks;

public:
    HashJoinProbeExec(
        const JoinPtr & join_,
        const BlockInputStreamPtr & restore_build_stream_,
        const BlockInputStreamPtr & probe_stream_,
        bool need_output_non_joined_data_,
        size_t non_joined_stream_index_,
        const BlockInputStreamPtr & non_joined_stream_,
        size_t max_block_size_);

    void restoreBuild();

    std::tuple<size_t, Block> getProbeBlock();

    std::optional<HashJoinProbeExecPtr> tryGetRestoreExec();

    void cancel();

    void meetError(const String & error_message);

    void onProbeStart();
    // Returns true if the probe_exec ends.
    // Returns false if the probe_exec continues to execute.
    bool onProbeFinish();

    // Returns true if the probe_exec ends.
    // Returns false if the probe_exec continues to execute.
    bool onNonJoinedFinish();
};

class HashJoinProbeExecHolder
{
public:
    const HashJoinProbeExecPtr & operator->()
    {
        std::lock_guard lock(mu);
        assert(exec);
        return exec;
    }

    const HashJoinProbeExecPtr & operator*()
    {
        std::lock_guard lock(mu);
        assert(exec);
        return exec;
    }

    void set(HashJoinProbeExecPtr && new_one)
    {
        assert(new_one);
        std::lock_guard lock(mu);
        exec = new_one;
    }

private:
    std::mutex mu;
    HashJoinProbeExecPtr exec;
};
} // namespace DB
