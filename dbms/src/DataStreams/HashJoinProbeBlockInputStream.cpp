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
    auto input_header = input->getHeader();
    assert(input_header.rows() == 0);
    if (need_output_non_joined_data)
        non_joined_stream = join->createStreamWithNonJoinedRows(input_header, current_non_joined_stream_index, join->getProbeConcurrency(), max_block_size);
    ProbeProcessInfo header_probe_process_info(0);
    header_probe_process_info.resetBlock(std::move(input_header));
    header = original_join->joinBlock(header_probe_process_info, true);
}

Block HashJoinProbeBlockInputStream::getHeader() const
{
    return header;
}

void HashJoinProbeBlockInputStream::cancel(bool kill)
{
    IProfilingBlockInputStream::cancel(kill);
    JoinPtr current_join;
    RestoreInfo restore_info;
    {
        std::lock_guard lock(mutex);
        current_join = join;
        restore_info.non_joined_stream = non_joined_stream;
        restore_info.build_stream = restore_build_stream;
        restore_info.probe_stream = restore_probe_stream;
    }
    /// Join::wakeUpAllWaitingThreads wakes up all the threads waiting in Join::waitUntilAllBuildFinished/waitUntilAllProbeFinished,
    /// and once this function is called, all the subsequent call of Join::waitUntilAllBuildFinished/waitUntilAllProbeFinished will
    /// skip waiting directly.
    /// HashJoinProbeBlockInputStream::cancel will be called in two cases:
    /// 1. the query is cancelled by the caller or meet error: in this case, wake up all waiting threads is safe, because no data
    ///    will be used data anymore
    /// 2. the query is executed normally, and one of the data stream has read an empty block, the the data stream and all its
    ///    children will call `cancel(false)`, in this case, there is two sub-cases
    ///    a. the data stream read an empty block because of EOF, then it means there must be no threads waiting in Join, so wake
    ///       up all waiting threads is safe because actually there is no threads to be waken up
    ///    b. the data stream read an empty block because of early exit of some executor(like limit), in this case, waking up the
    ///       waiting threads is not 100% safe because if the probe thread is waken up when build is not finished yet, it may get
    ///       wrong result. Currently, the execution framework ensures that when any of the data stream read empty block because
    ///       of early exit, no further data will be used, and in order to make sure no wrong result is generated
    ///       - for threads reading joined data: will return empty block if build is not finished yet
    ///       - for threads reading non joined data: will return empty block if build or probe is not finished yet
    current_join->wakeUpAllWaitingThreads();
    if (restore_info.non_joined_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(restore_info.non_joined_stream.get());
        if (p_stream != nullptr)
            p_stream->cancel(kill);
    }
    if (restore_info.probe_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(restore_info.probe_stream.get());
        if (p_stream != nullptr)
            p_stream->cancel(kill);
    }
    if (restore_info.build_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(restore_info.build_stream.get());
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

void HashJoinProbeBlockInputStream::onCurrentProbeDone()
{
    if (join->isRestoreJoin())
        current_probe_stream->readSuffix();
    join->finishOneProbe();
    if (need_output_non_joined_data || join->isEnableSpill())
    {
        status = ProbeStatus::WAIT_PROBE_FINISH;
    }
    else
    {
        status = ProbeStatus::FINISHED;
    }
}

void HashJoinProbeBlockInputStream::onCurrentReadNonJoinedDataDone()
{
    non_joined_stream->readSuffix();
    if (!join->isEnableSpill())
    {
        status = ProbeStatus::FINISHED;
    }
    else
    {
        join->finishOneNonJoin(current_non_joined_stream_index);
        status = ProbeStatus::GET_RESTORE_JOIN;
    }
}

void HashJoinProbeBlockInputStream::tryGetRestoreJoin()
{
    /// find restore join in DFS way
    while (true)
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
                parents.push_back(join);
                {
                    std::lock_guard lock(mutex);
                    if (isCancelledOrThrowIfKilled())
                    {
                        status = ProbeStatus::FINISHED;
                        return;
                    }
                    join = restore_info.join;
                    restore_build_stream = restore_info.build_stream;
                    restore_probe_stream = restore_info.probe_stream;
                    non_joined_stream = restore_info.non_joined_stream;
                    current_probe_stream = restore_probe_stream;
                    if (non_joined_stream != nullptr)
                        current_non_joined_stream_index = dynamic_cast<NonJoinedBlockInputStream *>(non_joined_stream.get())->getNonJoinedIndex();
                }
                status = ProbeStatus::RESTORE_BUILD;
                return;
            }
            assert(join->hasPartitionSpilledWithLock() == false);
        }
        /// current join has no more partition to restore, so check if previous join still has partition to restore
        if (!parents.empty())
        {
            /// replace current join with previous join
            std::lock_guard lock(mutex);
            if (isCancelledOrThrowIfKilled())
            {
                status = ProbeStatus::FINISHED;
                return;
            }
            else
            {
                join = parents.back();
                parents.pop_back();
                restore_probe_stream = nullptr;
                restore_build_stream = nullptr;
                current_probe_stream = nullptr;
                non_joined_stream = nullptr;
            }
        }
        else
        {
            /// no previous join, set status to FINISHED
            status = ProbeStatus::FINISHED;
            return;
        }
    }
}

void HashJoinProbeBlockInputStream::onAllProbeDone()
{
    if (need_output_non_joined_data)
    {
        assert(non_joined_stream != nullptr);
        status = ProbeStatus::READ_NON_JOINED_DATA;
        non_joined_stream->readPrefix();
    }
    else
    {
        status = ProbeStatus::GET_RESTORE_JOIN;
    }
}

Block HashJoinProbeBlockInputStream::getOutputBlock()
{
    try
    {
        while (true)
        {
            switch (status)
            {
            case ProbeStatus::WAIT_BUILD_FINISH:
                join->waitUntilAllBuildFinished();
                /// after Build finish, always go to Probe stage
                if (join->isRestoreJoin())
                    current_probe_stream->readSuffix();
                status = ProbeStatus::PROBE;
                break;
            case ProbeStatus::PROBE:
            {
                assert(current_probe_stream != nullptr);
                if (probe_process_info.all_rows_joined_finish)
                {
                    auto [partition_index, block] = getOneProbeBlock();
                    if (!block)
                    {
                        onCurrentProbeDone();
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
            case ProbeStatus::WAIT_PROBE_FINISH:
                join->waitUntilAllProbeFinished();
                onAllProbeDone();
                break;
            case ProbeStatus::READ_NON_JOINED_DATA:
            {
                auto block = non_joined_stream->read();
                non_joined_rows += block.rows();
                if (!block)
                {
                    onCurrentReadNonJoinedDataDone();
                    break;
                }
                return block;
            }
            case ProbeStatus::GET_RESTORE_JOIN:
            {
                tryGetRestoreJoin();
                break;
            }
            case ProbeStatus::RESTORE_BUILD:
            {
                probe_process_info.all_rows_joined_finish = true;
                restore_build_stream->readPrefix();
                while (restore_build_stream->read()) {};
                restore_build_stream->readSuffix();
                status = ProbeStatus::WAIT_BUILD_FINISH;
                break;
            }
            case ProbeStatus::FINISHED:
                return {};
            }
        }
    }
    catch (...)
    {
        /// set status to finish if any exception happens
        status = ProbeStatus::FINISHED;
        throw;
    }
}

std::tuple<size_t, Block> HashJoinProbeBlockInputStream::getOneProbeBlock()
{
    size_t partition_index = 0;
    Block block;

    /// Even if spill is enabled, if spill is not triggered during build,
    /// there is no need to dispatch probe block
    if (!join->isSpilled())
    {
        block = current_probe_stream->read();
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
                auto new_block = current_probe_stream->read();
                if (new_block)
                    join->dispatchProbeBlock(new_block, probe_partition_blocks);
                else
                    break;
            }
        }
    }
    return {partition_index, block};
}

} // namespace DB
