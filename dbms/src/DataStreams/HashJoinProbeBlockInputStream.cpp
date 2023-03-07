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

#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <DataStreams/NonJoinedBlockInputStream.h>

namespace DB
{
HashJoinProbeBlockInputStream::HashJoinProbeBlockInputStream(
    const BlockInputStreamPtr & input,
    const JoinPtr & join_,
    size_t non_joined_stream_index,
    const String & req_id,
    UInt64 max_block_size_)
    : log(Logger::get(req_id))
    , original_join(join_)
    , join(original_join)
    , need_output_non_joined_data(join->needReturnNonJoinedData())
    , current_non_joined_stream_index(non_joined_stream_index)
    , max_block_size(max_block_size_)
    , probe_process_info(max_block_size_)
{
    children.push_back(input);
    current_probe_stream = children.back();

    RUNTIME_CHECK_MSG(join != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(join->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");
    if (need_output_non_joined_data)
        non_joined_stream = join->createStreamWithNonJoinedRows(input->getHeader(), current_non_joined_stream_index, join->getProbeConcurrency(), max_block_size);
}

Block HashJoinProbeBlockInputStream::getHeader() const
{
    Block res = children.back()->getHeader();
    assert(res.rows() == 0);
    ProbeProcessInfo header_probe_process_info(0);
    header_probe_process_info.resetBlock(std::move(res));
    /// use original_join here so we don't need add lock
    return original_join->joinBlock(header_probe_process_info);
}

void HashJoinProbeBlockInputStream::cancel(bool kill)
{
    IProfilingBlockInputStream::cancel(kill);
    /// When the probe stream quits probe by cancelling instead of normal finish, the Join operator might still produce meaningless blocks
    /// and expects these meaningless blocks won't be used to produce meaningful result.

    JoinPtr current_join;
    BlockInputStreamPtr current_non_joined_stream;
    BlockInputStreamPtr current_restore_build_stream;
    BlockInputStreamPtr current_restore_probe_stream;
    {
        std::lock_guard lock(mutex);
        current_join = join;
        current_non_joined_stream = non_joined_stream;
        current_restore_build_stream = restore_build_stream;
        current_restore_probe_stream = restore_probe_stream;
    }
    /// Cancel join just wake up all the threads waiting in Join::waitUntilAllBuildFinished/Join::waitUntilAllProbeFinished,
    /// the ongoing join process will not be interrupted
    /// There is a little bit hack here because cancel will be called in two cases:
    /// 1. the query is cancelled by the caller or meet error: in this case, wake up all waiting threads is safe
    /// 2. the query is executed normally, and one of the data stream has read an empty block, the the data stream and all its children
    ///    will call `cancel(false)`, in this case, there is two sub-cases
    ///    a. the data stream read an empty block because of EOF, then it means there must be no threads waiting in Join, so cancel the join is safe
    ///    b. the data stream read an empty block because of early exit of some executor(like limit), in this case, just wake the waiting
    ///       threads is not 100% safe because if the probe thread is wake up when build is not finished yet, it may produce wrong results, for now
    ///       it is safe because when any of the data stream read empty block because of early exit, the execution framework ensures that no further
    ///       data will be used.
    current_join->cancel();
    if (current_non_joined_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(current_non_joined_stream.get());
        if (p_stream != nullptr)
            p_stream->cancel(kill);
    }
    if (current_restore_probe_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(current_restore_probe_stream.get());
        if (p_stream != nullptr)
            p_stream->cancel(kill);
    }
    if (current_restore_build_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(current_restore_build_stream.get());
        if (p_stream != nullptr)
            p_stream->cancel(kill);
    }
}

Block HashJoinProbeBlockInputStream::readImpl()
{
    try
    {
        Block ret = getOutputBlock();
        return ret;
    }
    catch (...)
    {
        auto error_message = getCurrentExceptionMessage(false, true);
        join->meetError(error_message);
        throw Exception(error_message);
    }
}

void HashJoinProbeBlockInputStream::readSuffixImpl()
{
    LOG_DEBUG(log, "Finish join probe, total output rows {}, joined rows {}, non joined rows {}", joined_rows + non_joined_rows, joined_rows, non_joined_rows);
}

Block HashJoinProbeBlockInputStream::getOutputBlock()
{
    while (true)
    {
        switch (status)
        {
        case ProbeStatus::PROBE:
        {
            join->waitUntilAllBuildFinished();
            assert(current_probe_stream != nullptr);
            if (probe_process_info.all_rows_joined_finish)
            {
                size_t partition_index = 0;
                Block block;

                if (!join->isSpilled())
                {
                    block = current_probe_stream->read();
                }
                else
                {
                    auto partition_block = getOneProbeBlock();
                    partition_index = std::get<0>(partition_block);
                    block = std::get<1>(partition_block);
                }

                if (!block)
                {
                    if (join->isSpilled())
                    {
                        block = current_probe_stream->read();
                        if (block)
                        {
                            join->dispatchProbeBlock(block, probe_partition_blocks);
                            break;
                        }
                    }
                    join->finishOneProbe();

                    if (need_output_non_joined_data)
                    {
                        status = ProbeStatus::WAIT_FOR_READ_NON_JOINED_DATA;
                    }
                    else
                    {
                        status = ProbeStatus::JUDGE_WEATHER_HAVE_PARTITION_TO_RESTORE;
                    }
                    break;
                }
                else
                {
                    join->checkTypes(block);
                    probe_process_info.resetBlock(std::move(block), partition_index);
                }
            }
            auto ret = join->joinBlock(probe_process_info);
            joined_rows += ret.rows();
            return ret;
        }
        case ProbeStatus::WAIT_FOR_READ_NON_JOINED_DATA:
            join->waitUntilAllProbeFinished();
            status = ProbeStatus::READ_NON_JOINED_DATA;
            non_joined_stream->readPrefix();
            break;
        case ProbeStatus::READ_NON_JOINED_DATA:
        {
            auto block = non_joined_stream->read();
            non_joined_rows += block.rows();
            if (!block)
            {
                non_joined_stream->readSuffix();
                if (!join->isEnableSpill())
                {
                    status = ProbeStatus::FINISHED;
                    break;
                }
                join->finishOneNonJoin(current_non_joined_stream_index);
                status = ProbeStatus::JUDGE_WEATHER_HAVE_PARTITION_TO_RESTORE;
                break;
            }
            return block;
        }
        case ProbeStatus::BUILD_RESTORE_PARTITION:
        {
            auto [restore_join, build_stream, probe_stream, restore_non_joined_stream] = join->getOneRestoreStream(max_block_size);
            if (!restore_join)
            {
                status = ProbeStatus::JUDGE_WEATHER_HAVE_PARTITION_TO_RESTORE;
                break;
            }
            parents.push_back(join);
            {
                std::lock_guard lock(mutex);
                if (isCancelledOrThrowIfKilled())
                {
                    status = ProbeStatus::FINISHED;
                    return {};
                }
                join = restore_join;
                restore_build_stream = build_stream;
                restore_probe_stream = probe_stream;
                non_joined_stream = restore_non_joined_stream;
                current_probe_stream = restore_probe_stream;
                if (non_joined_stream != nullptr)
                    current_non_joined_stream_index = dynamic_cast<NonJoinedBlockInputStream *>(non_joined_stream.get())->getNonJoinedIndex();
            }
            probe_process_info.all_rows_joined_finish = true;
            while (restore_build_stream->read()) {};
            status = ProbeStatus::PROBE;
            break;
        }
        case ProbeStatus::JUDGE_WEATHER_HAVE_PARTITION_TO_RESTORE:
        {
            if (!join->isEnableSpill())
            {
                status = ProbeStatus::FINISHED;
                break;
            }
            join->waitUntilAllProbeFinished();
            if (join->hasPartitionSpilledWithLock())
            {
                status = ProbeStatus::BUILD_RESTORE_PARTITION;
            }
            else if (!parents.empty())
            {
                std::lock_guard lock(mutex);
                if (isCancelledOrThrowIfKilled())
                {
                    status = ProbeStatus::FINISHED;
                    return {};
                }
                join = parents.back();
                parents.pop_back();
                restore_probe_stream = nullptr;
                restore_build_stream = nullptr;
                current_probe_stream = nullptr;
                non_joined_stream = nullptr;
            }
            else
            {
                status = ProbeStatus::FINISHED;
            }
            break;
        }
        case ProbeStatus::FINISHED:
            return {};
        }
    }
}

std::tuple<size_t, Block> HashJoinProbeBlockInputStream::getOneProbeBlock()
{
    if (!probe_partition_blocks.empty())
    {
        auto partition_block = probe_partition_blocks.front();
        probe_partition_blocks.pop_front();
        return partition_block;
    }
    return {0, {}};
}

} // namespace DB
