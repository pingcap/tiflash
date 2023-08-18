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

#include <DataStreams/HashJoinProbeBlockInputStream.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
HashJoinProbeBlockInputStream::HashJoinProbeBlockInputStream(
    const BlockInputStreamPtr & input,
    const ExpressionActionsPtr & join_probe_actions_,
    const String & req_id)
    : log(Logger::get(req_id))
    , join_probe_actions(join_probe_actions_)
{
    children.push_back(input);

    if (!join_probe_actions || join_probe_actions->getActions().size() != 1
        || join_probe_actions->getActions().back().type != ExpressionAction::Type::JOIN)
    {
        throw Exception("isn't valid join probe actions", ErrorCodes::LOGICAL_ERROR);
    }
}

Block HashJoinProbeBlockInputStream::getTotals()
{
<<<<<<< HEAD
    if (auto * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
    {
        totals = child->getTotals();
        join_probe_actions->executeOnTotals(totals);
    }

    return totals;
=======
    LOG_DEBUG(
        log,
        "Finish join probe, total output rows {}, joined rows {}, scan hash map rows {}",
        joined_rows + scan_hash_map_rows,
        joined_rows,
        scan_hash_map_rows);
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
}

Block HashJoinProbeBlockInputStream::getHeader() const
{
    Block res = children.back()->getHeader();
    join_probe_actions->execute(res);
    return res;
}

Block HashJoinProbeBlockInputStream::readImpl()
{
    Block res = children.back()->read();
    if (!res)
        return res;

    join_probe_actions->execute(res);

    // TODO split block if block.size() > settings.max_block_size
    // https://github.com/pingcap/tiflash/issues/3436

<<<<<<< HEAD
    return res;
=======
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
>>>>>>> 6638f2067b (Fix license and format coding style (#7962))
}

} // namespace DB
