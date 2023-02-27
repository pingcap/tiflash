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

namespace DB
{
HashJoinProbeBlockInputStream::HashJoinProbeBlockInputStream(
    const BlockInputStreamPtr & input,
    const JoinPtr & join_,
    size_t probe_index_,
    const String & req_id,
    UInt64 max_block_size_)
    : log(Logger::get(req_id))
    , join(join_)
    , probe_index(probe_index_)
    , max_block_size(max_block_size_)
    , probe_process_info(max_block_size_)
{
    children.push_back(input);

    RUNTIME_CHECK_MSG(join != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(join->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");
    if (join->needReturnNonJoinedData())
        non_joined_stream = join->createStreamWithNonJoinedRows(input->getHeader(), probe_index, join->getProbeConcurrency(), max_block_size);
}

Block HashJoinProbeBlockInputStream::getTotals()
{
    if (auto * child = !join->isRestoreJoin() ? dynamic_cast<IProfilingBlockInputStream *>(&*children.back()) : dynamic_cast<IProfilingBlockInputStream *>(&*restore_stream))
    {
        totals = child->getTotals();
        if (!totals)
        {
            if (join->hasTotals())
            {
                for (const auto & name_and_type : child->getHeader().getColumnsWithTypeAndName())
                {
                    auto column = name_and_type.type->createColumn();
                    column->insertDefault();
                    totals.insert(ColumnWithTypeAndName(std::move(column), name_and_type.type, name_and_type.name));
                }
            }
            else
                return totals; /// There's nothing to JOIN.
        }
        join->joinTotals(totals);
    }

    return totals;
}

Block HashJoinProbeBlockInputStream::getHeader() const
{
    Block res = children.back()->getHeader();
    assert(res.rows() == 0);
    ProbeProcessInfo header_probe_process_info(0);
    header_probe_process_info.resetBlock(std::move(res));
    return join->joinBlock(header_probe_process_info);
}

void HashJoinProbeBlockInputStream::finishOneProbe()
{
    bool expect = false;
    if likely (probe_finished.compare_exchange_strong(expect, true))
        join->finishOneProbe();
}

void HashJoinProbeBlockInputStream::finishOneNonJoin()
{
    bool expect = false;
    if likely (non_join_finished.compare_exchange_strong(expect, true))
        join->finishOneNonJoin();
}

void HashJoinProbeBlockInputStream::cancel(bool kill)
{
    IProfilingBlockInputStream::cancel(kill);
    /// When the probe stream quits probe by cancelling instead of normal finish, the Join operator might still produce meaningless blocks
    /// and expects these meaningless blocks won't be used to produce meaningful result.

    join->is_canceled = true;

    if (non_joined_stream != nullptr)
    {
        auto * p_non_joined_stream = dynamic_cast<IProfilingBlockInputStream *>(non_joined_stream.get());
        if (p_non_joined_stream != nullptr)
            p_non_joined_stream->cancel(kill);
    }
    if (restore_stream != nullptr)
    {
        auto * p_restore_stream = dynamic_cast<IProfilingBlockInputStream *>(restore_stream.get());
        if (p_restore_stream != nullptr)
            p_restore_stream->cancel(kill);
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
            if (probe_process_info.all_rows_joined_finish)
            {
                size_t partition_index = 0;
                Block block;

                if (!join->isEnableSpill())
                {
                    block = children.back()->read();
                }
                else
                {
                    auto partition_block = getOneProbeBlock();
                    partition_index = std::get<0>(partition_block);
                    block = std::get<1>(partition_block);
                }

                if (!block)
                {
                    if (join->isEnableSpill())
                    {
                        block = !join->isRestoreJoin() ? children.back()->read() : restore_stream->read();
                        if (block)
                        {
                            join->dispatchProbeBlock(block, probe_partition_blocks);
                            break;
                        }
                    }
                    finishOneProbe();

                    if (join->needReturnNonJoinedData())
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
                finishOneNonJoin();
                join->waitUntilAllNonJoinFinished();
                status = ProbeStatus::JUDGE_WEATHER_HAVE_PARTITION_TO_RESTORE;
                break;
            }
            return block;
        }
        case ProbeStatus::BUILD_RESTORE_PARTITION:
        {
            auto [restore_join, build_stream, probe_stream] = join->getOneRestoreStream();
            if (!restore_join)
            {
                status = ProbeStatus::JUDGE_WEATHER_HAVE_PARTITION_TO_RESTORE;
                break;
            }
            parents.push_back(join);
            join = restore_join;
            probe_finished = false;
            if (join->needReturnNonJoinedData())
            {
                non_join_finished = false;
            }
            build_stream = std::make_shared<HashJoinBuildBlockInputStream>(build_stream, restore_join, probe_index, log->identifier());
            restore_stream.reset();
            restore_stream = probe_stream;
            if (join->needReturnNonJoinedData())
            {
                non_joined_stream = join->createStreamWithNonJoinedRows(probe_stream->getHeader(), probe_index, join->getProbeConcurrency(), max_block_size);
            }
            probe_process_info.all_rows_joined_finish = true;
            while (build_stream->read()) {};
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
                join = parents.back();
                parents.pop_back();
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
