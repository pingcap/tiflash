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

#include <DataStreams/HashJoinProbeBlockInputStream.h>

namespace DB
{
HashJoinProbeBlockInputStream::HashJoinProbeBlockInputStream(
    const BlockInputStreamPtr & input,
    const JoinPtr & join_,
    size_t probe_index_,
    const String & req_id,
    UInt64 max_block_size)
    : log(Logger::get(req_id))
    , join(join_)
    , probe_index(probe_index_)
    , probe_process_info(max_block_size)
{
    children.push_back(input);

    RUNTIME_CHECK_MSG(join != nullptr, "join ptr should not be null.");
    RUNTIME_CHECK_MSG(join->getProbeConcurrency() > 0, "Join probe concurrency must be greater than 0");
    if (join->needReturnNonJoinedData())
        non_joined_stream = join->createStreamWithNonJoinedRows(input->getHeader(), probe_index, join->getProbeConcurrency(), max_block_size);
}

Block HashJoinProbeBlockInputStream::getTotals()
{
    if (auto * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
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

void HashJoinProbeBlockInputStream::cancel(bool kill)
{
    IProfilingBlockInputStream::cancel(kill);
    /// When the probe stream quits probe by cancelling instead of normal finish, the Join operator might still produce meaningless blocks
    /// and expects these meaningless blocks won't be used to produce meaningful result.
    try
    {
        finishOneProbe();
    }
    catch (...)
    {
        tryLogCurrentException(log, "finishOneProbe failed in cancel() ");
        join->meetError();
    }
    if (non_joined_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(non_joined_stream.get());
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
        join->meetError();
        throw;
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
            if (probe_process_info.all_rows_joined_finish)
            {
                Block block = children.back()->read();
                if (!block)
                {
                    finishOneProbe();
                    if (join->needReturnNonJoinedData())
                        status = ProbeStatus::WAIT_FOR_READ_NON_JOINED_DATA;
                    else
                        status = ProbeStatus::FINISHED;
                    break;
                }
                else
                {
                    join->checkTypes(block);
                    probe_process_info.resetBlock(std::move(block));
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
                status = ProbeStatus::FINISHED;
            }
            return block;
        }
        case ProbeStatus::FINISHED:
            return {};
        }
    }
}

} // namespace DB
