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
#include <Interpreters/ExpressionActions.h>

namespace DB
{
HashJoinProbeBlockInputStream::HashJoinProbeBlockInputStream(
    const BlockInputStreamPtr & input,
    const JoinPtr & join_,
    const String & req_id)
    : log(Logger::get(req_id))
    , join(join_)
{
    children.push_back(input);
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
    join->joinBlock(res);
    return res;
}

Block HashJoinProbeBlockInputStream::readImpl()
{
    Block res = children.back()->read();
    if (!res)
        return res;

    join->joinBlock(res);

    // TODO split block if block.size() > settings.max_block_size
    // https://github.com/pingcap/tiflash/issues/3436

    return res;
}

} // namespace DB
