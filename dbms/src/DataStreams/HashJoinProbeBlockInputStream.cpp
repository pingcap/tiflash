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
    const String & req_id,
    UInt64 max_block_size)
    : log(Logger::get(req_id))
    , join(join_)
    , probe_process_info(max_block_size)
    , squashing_transform(max_block_size)
{
    children.push_back(input);

    RUNTIME_CHECK_MSG(join != nullptr, "join ptr should not be null.");
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

Block HashJoinProbeBlockInputStream::readImpl()
{
    // if join finished, return {} directly.
    if (squashing_transform.isJoinFinished())
    {
        return Block{};
    }

    while (squashing_transform.needAppendBlock())
    {
        Block result_block = getOutputBlock(probe_process_info);
        squashing_transform.appendBlock(result_block);
    }
    return squashing_transform.getFinalOutputBlock();
}

Block HashJoinProbeBlockInputStream::getOutputBlock(ProbeProcessInfo & probe_process_info_) const
{
    if (probe_process_info_.all_rows_joined_finish)
    {
        Block block = children.back()->read();
        if (!block)
        {
            return block;
        }
        join->checkTypes(block);
        probe_process_info_.resetBlock(std::move(block));
    }

    return join->joinBlock(probe_process_info_);
}

} // namespace DB
