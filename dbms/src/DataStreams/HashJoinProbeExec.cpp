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

#include <DataStreams/HashJoinProbeExec.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/NonJoinedBlockInputStream.h>

namespace DB
{
HashJoinProbeExec::HashJoinProbeExec(
    const JoinPtr & join_,
    const BlockInputStreamPtr & restore_build_stream_,
    const BlockInputStreamPtr & probe_stream_,
    bool need_output_non_joined_data_,
    size_t non_joined_stream_index_,
    const BlockInputStreamPtr & non_joined_stream_,
    size_t max_block_size_)
    : join(join_)
    , restore_build_stream(restore_build_stream_)
    , probe_stream(probe_stream_)
    , need_output_non_joined_data(need_output_non_joined_data_)
    , non_joined_stream_index(non_joined_stream_index_)
    , non_joined_stream(non_joined_stream_)
    , max_block_size(max_block_size_)
{}

void HashJoinProbeExec::restoreBuild()
{
    restore_build_stream->readPrefix();
    while (restore_build_stream->read()) {};
    restore_build_stream->readSuffix();
}

std::tuple<size_t, Block> HashJoinProbeExec::getProbeBlock()
{
    size_t partition_index = 0;
    Block block;

    /// Even if spill is enabled, if spill is not triggered during build,
    /// there is no need to dispatch probe block
    if (!join->isSpilled())
    {
        block = probe_stream->read();
    }
    else
    {
        while (true)
        {
            if (!probe_partition_blocks.empty())
            {
                auto partition_block = probe_partition_blocks.front();
                probe_partition_blocks.pop_front();
                partition_index = std::get<0>(partition_block);
                block = std::get<1>(partition_block);
                break;
            }
            else
            {
                auto new_block = probe_stream->read();
                if (new_block)
                    join->dispatchProbeBlock(new_block, probe_partition_blocks);
                else
                    break;
            }
        }
    }
    return {partition_index, block};
}

std::optional<HashJoinProbeExecPtr> HashJoinProbeExec::tryGetRestoreExec()
{
    assert(join->isEnableSpill());
    /// first check if current join has a partition to restore
    if (join->hasPartitionSpilledWithLock())
    {
        auto restore_info = join->getOneRestoreStream(max_block_size);
        /// get a restore join
        if (restore_info.join)
        {
            /// restored join should always enable spill
            assert(restore_info.join->isEnableSpill());
            size_t non_joined_stream_index = 0;
            if (need_output_non_joined_data)
                non_joined_stream_index = dynamic_cast<NonJoinedBlockInputStream *>(restore_info.non_joined_stream.get())->getNonJoinedIndex();
            auto restore_probe_exec = std::make_shared<HashJoinProbeExec>(
                restore_info.join,
                restore_info.build_stream,
                restore_info.probe_stream,
                need_output_non_joined_data,
                non_joined_stream_index,
                restore_info.non_joined_stream,
                max_block_size);
            return {std::move(restore_probe_exec)};
        }
        assert(join->hasPartitionSpilledWithLock() == false);
    }
    return {};
}

void HashJoinProbeExec::cancel()
{
    join->cancel();
    if (non_joined_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(non_joined_stream.get());
        if (p_stream != nullptr)
            p_stream->cancel(false);
    }
    if (probe_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(probe_stream.get());
        if (p_stream != nullptr)
            p_stream->cancel(false);
    }
    if (restore_build_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(restore_build_stream.get());
        if (p_stream != nullptr)
            p_stream->cancel(false);
    }
}

void HashJoinProbeExec::meetError(const String & error_message)
{
    join->meetError(error_message);
}

void HashJoinProbeExec::onProbeStart()
{
    if (join->isRestoreJoin())
        probe_stream->readPrefix();
}

bool HashJoinProbeExec::onProbeFinish()
{
    if (join->isRestoreJoin())
        probe_stream->readSuffix();
    join->finishOneProbe();
    return !need_output_non_joined_data && !join->isEnableSpill();
}

bool HashJoinProbeExec::onNonJoinedFinish()
{
    non_joined_stream->readSuffix();
    if (!join->isEnableSpill())
    {
        return true;
    }
    else
    {
        join->finishOneNonJoin(non_joined_stream_index);
        return false;
    }
}
} // namespace DB
