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

#include <magic_enum.hpp>

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
    , probe_process_info(max_block_size_)
{
    children.push_back(input);

    RUNTIME_CHECK_MSG(original_join != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(original_join->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");

    bool need_output_non_joined_data = original_join->needReturnNonJoinedData();
    BlockInputStreamPtr non_joined_stream = nullptr;
    if (need_output_non_joined_data)
        non_joined_stream = original_join->createStreamWithNonJoinedRows(input->getHeader(), non_joined_stream_index, original_join->getProbeConcurrency(), max_block_size_);

    auto cur_probe_exec = std::make_shared<HashJoinProbeExec>(
        original_join,
        nullptr,
        input,
        need_output_non_joined_data,
        non_joined_stream_index,
        non_joined_stream,
        max_block_size_);
    probe_exec.set(std::move(cur_probe_exec));
}

void HashJoinProbeBlockInputStream::readSuffixImpl()
{
    LOG_DEBUG(log, "Finish join probe, total output rows {}, joined rows {}, non joined rows {}", joined_rows + non_joined_rows, joined_rows, non_joined_rows);
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
    probe_exec->cancel();
}

Block HashJoinProbeBlockInputStream::readImpl()
{
    return getOutputBlock();
}

void HashJoinProbeBlockInputStream::switchStatus(ProbeStatus to)
{
    LOG_TRACE(log, fmt::format("{} -> {}", magic_enum::enum_name(status), magic_enum::enum_name(to)));
    status = to;
}

void HashJoinProbeBlockInputStream::onCurrentProbeDone()
{
    switchStatus(probe_exec->onProbeFinish() ? ProbeStatus::FINISHED : ProbeStatus::WAIT_PROBE_FINISH);
}

void HashJoinProbeBlockInputStream::onCurrentReadNonJoinedDataDone()
{
    switchStatus(probe_exec->onNonJoinedFinish() ? ProbeStatus::FINISHED : ProbeStatus::GET_RESTORE_JOIN);
}

void HashJoinProbeBlockInputStream::tryGetRestoreJoin()
{
    /// find restore join in DFS way
    while (true)
    {
        if (isCancelledOrThrowIfKilled())
        {
            switchStatus(ProbeStatus::FINISHED);
            return;
        }

        auto cur_probe_exec = *probe_exec;
        auto restore_probe_exec = cur_probe_exec->tryGetRestoreExec();
        if (restore_probe_exec.has_value())
        {
            parents.push_back(std::move(cur_probe_exec));
            probe_exec.set(std::move(*restore_probe_exec));
            switchStatus(ProbeStatus::RESTORE_BUILD);
            return;
        }
        /// current join has no more partition to restore, so check if previous join still has partition to restore
        if (!parents.empty())
        {
            /// replace current join with previous join
            probe_exec.set(std::move(parents.back()));
            parents.pop_back();
        }
        else
        {
            /// no previous join, set status to FINISHED
            switchStatus(ProbeStatus::FINISHED);
            return;
        }
    }
}

void HashJoinProbeBlockInputStream::onAllProbeDone()
{
    if (probe_exec->need_output_non_joined_data)
    {
        assert(probe_exec->non_joined_stream != nullptr);
        probe_exec->non_joined_stream->readPrefix();
        switchStatus(ProbeStatus::READ_NON_JOINED_DATA);
    }
    else
    {
        switchStatus(ProbeStatus::GET_RESTORE_JOIN);
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
                probe_exec->join->waitUntilAllBuildFinished();
                /// after Build finish, always go to Probe stage
                probe_exec->onProbeStart();
                switchStatus(ProbeStatus::PROBE);
                break;
            case ProbeStatus::PROBE:
            {
                if (probe_process_info.all_rows_joined_finish)
                {
                    auto [partition_index, block] = probe_exec->getProbeBlock();
                    if (!block)
                    {
                        onCurrentProbeDone();
                        break;
                    }
                    else
                    {
                        probe_exec->join->checkTypes(block);
                        probe_process_info.resetBlock(std::move(block), partition_index);
                    }
                }
                auto ret = probe_exec->join->joinBlock(probe_process_info);
                joined_rows += ret.rows();
                return ret;
            }
            case ProbeStatus::WAIT_PROBE_FINISH:
                probe_exec->join->waitUntilAllProbeFinished();
                onAllProbeDone();
                break;
            case ProbeStatus::READ_NON_JOINED_DATA:
            {
                auto block = probe_exec->non_joined_stream->read();
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
                probe_exec->restoreBuild();
                switchStatus(ProbeStatus::WAIT_BUILD_FINISH);
                break;
            }
            case ProbeStatus::FINISHED:
                return {};
            }
        }
    }
    catch (...)
    {
        auto error_message = getCurrentExceptionMessage(true, true);
        probe_exec->meetError(error_message);
        switchStatus(ProbeStatus::FINISHED);
        throw Exception(error_message);
    }
}

} // namespace DB
