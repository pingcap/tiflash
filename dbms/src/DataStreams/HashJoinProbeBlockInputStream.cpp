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

#include <DataStreams/HashJoinBuildBlockInputStream.h>
#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <DataStreams/ScanHashMapAfterProbeBlockInputStream.h>

#include <magic_enum.hpp>

namespace DB
{
HashJoinProbeBlockInputStream::HashJoinProbeBlockInputStream(
    const BlockInputStreamPtr & input,
    const JoinPtr & join_,
    size_t stream_index,
    const String & req_id,
    UInt64 max_block_size_)
    : log(Logger::get(req_id))
    , original_join(join_)
{
    children.push_back(input);

    RUNTIME_CHECK_MSG(original_join != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(original_join->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");

    probe_exec.set(HashJoinProbeExec::build(req_id, original_join, stream_index, input, max_block_size_));
    probe_exec->setCancellationHook([&]() { return isCancelledOrThrowIfKilled(); });
    if (stream_index == 0)
    {
        original_join->setCancellationHook([&]() { return isCancelledOrThrowIfKilled(); });
    }

    ProbeProcessInfo header_probe_process_info(0, 0);
    header_probe_process_info.resetBlock(input->getHeader());
    header = original_join->joinBlock(header_probe_process_info, true);
}

void HashJoinProbeBlockInputStream::readSuffixImpl()
{
    LOG_DEBUG(
        log,
        "Finish join probe, total output rows {}, joined rows {}, scan hash map rows {}",
        joined_rows + scan_hash_map_rows,
        joined_rows,
        scan_hash_map_rows);
}

Block HashJoinProbeBlockInputStream::getHeader() const
{
    return header;
}

void HashJoinProbeBlockInputStream::cancel(bool kill)
{
    IProfilingBlockInputStream::cancel(kill);

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

void HashJoinProbeBlockInputStream::onCurrentScanHashMapDone()
{
    switchStatus(probe_exec->onScanHashMapAfterProbeFinish() ? ProbeStatus::FINISHED : ProbeStatus::GET_RESTORE_JOIN);
}

void HashJoinProbeBlockInputStream::tryGetRestoreJoin()
{
    if (auto restore_probe_exec = probe_exec->tryGetRestoreExec();
        restore_probe_exec && unlikely(!isCancelledOrThrowIfKilled()))
    {
        probe_exec.set(std::move(restore_probe_exec));
        switchStatus(ProbeStatus::RESTORE_BUILD);
    }
    else
    {
        switchStatus(ProbeStatus::FINISHED);
    }
}

void HashJoinProbeBlockInputStream::onAllProbeDone()
{
    auto & cur_probe_exec = *probe_exec;
    if (cur_probe_exec.needScanHashMap())
    {
        cur_probe_exec.onScanHashMapAfterProbeStart();
        switchStatus(ProbeStatus::READ_SCAN_HASH_MAP_DATA);
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
            if unlikely (isCancelledOrThrowIfKilled())
                return {};

            switch (status)
            {
            case ProbeStatus::WAIT_BUILD_FINISH:
            {
                auto & cur_probe_exec = *probe_exec;
                cur_probe_exec.waitUntilAllBuildFinished();
                /// after Build finish, always go to Probe stage
                cur_probe_exec.onProbeStart();
                switchStatus(ProbeStatus::PROBE);
                break;
            }
            case ProbeStatus::PROBE:
            {
                auto ret = probe_exec->probe();
                if (!ret)
                {
                    onCurrentProbeDone();
                    break;
                }
                else
                {
                    joined_rows += ret.rows();
                    return ret;
                }
            }
            case ProbeStatus::WAIT_PROBE_FINISH:
            {
                probe_exec->waitUntilAllProbeFinished();
                onAllProbeDone();
                break;
            }
            case ProbeStatus::READ_SCAN_HASH_MAP_DATA:
            {
                auto block = probe_exec->fetchScanHashMapData();
                scan_hash_map_rows += block.rows();
                if (!block)
                {
                    onCurrentScanHashMapDone();
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
