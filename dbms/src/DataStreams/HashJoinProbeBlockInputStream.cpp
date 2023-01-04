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
    bool has_non_joined_data_,
    const String & req_id,
    UInt64 max_block_size_)
    : log(Logger::get(req_id))
    , join(join_)
    , probe_index(probe_index_)
    , max_block_size(max_block_size_)
    , has_non_joined_data(has_non_joined_data_)
    , probe_process_info(max_block_size)
    , squashing_transform(max_block_size)
{
    children.push_back(input);

    RUNTIME_CHECK_MSG(join != nullptr, "join ptr should not be null.");
    if (has_non_joined_data)
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

void HashJoinProbeBlockInputStream::cancel(bool kill)
{
    IProfilingBlockInputStream::cancel(kill);
    if (non_joined_stream != nullptr)
    {
        auto * p_stream = dynamic_cast<IProfilingBlockInputStream *>(non_joined_stream.get());
        if (p_stream != nullptr)
            p_stream->cancel(kill);
    }
}

Block HashJoinProbeBlockInputStream::readImpl()
{
    // if join finished, return {} directly.
    if (squashing_transform.isJoinFinished())
    {
        return Block{};
    }

    while (squashing_transform.needAppendBlock())
    {
        Block result_block = getOutputBlock();
        squashing_transform.appendBlock(result_block);
    }
    return squashing_transform.getFinalOutputBlock();
}

Block HashJoinProbeBlockInputStream::getOutputBlock()
{
    if (reading_non_joined_data)
    {
        return non_joined_stream->read();
    }
    if (probe_process_info.all_rows_joined_finish)
    {
        Block block = children.back()->read();
        if (!block)
        {
            /// probe is done, append non-joined data if needed
            join->finishOneProbe();
            if (has_non_joined_data)
            {
                join->waitUntilAllProbeFinished();
                reading_non_joined_data = true;
                non_joined_stream->readPrefix();
                return non_joined_stream->read();
            }
            else
                return block;
        }
        else
        {
            join->checkTypes(block);
            probe_process_info.resetBlock(std::move(block));
        }
    }
    return join->joinBlock(probe_process_info);
}

} // namespace DB
