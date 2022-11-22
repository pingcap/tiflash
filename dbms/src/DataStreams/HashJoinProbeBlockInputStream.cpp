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
    const ExpressionActionsPtr & join_probe_actions_,
    const String & req_id,
    size_t concurrency_probe_index_)
    : log(Logger::get(req_id))
    , join_probe_actions(join_probe_actions_)
    , concurrency_probe_index(concurrency_probe_index_)
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
    if (auto * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
    {
        totals = child->getTotals();
        join_probe_actions->executeOnTotals(totals);
    }

    return totals;
}

Block HashJoinProbeBlockInputStream::getHeader() const
{
    Block res = children.back()->getHeader();
    if (res.rows() != 0)
        res = res.cloneEmpty();
    join_probe_actions->executeForHashJoinProbeSide(res);
    return res;
}

Block HashJoinProbeBlockInputStream::readImpl()
{
    if (join_probe_actions->needGetBlockForHashJoinProbe(concurrency_probe_index))
    {
        Block block = children.back()->read();
        std::cout<<"read block row : "<<block.rows()<<"----"<<concurrency_probe_index<<std::endl;
        if (!block)
            return block;
        join_probe_actions->updateBlockForHashJoinProbe(block, concurrency_probe_index);
    }

    Block res;
    join_probe_actions->executeForHashJoinProbeSide(res, concurrency_probe_index);
    std::cout<<"out block row : "<<res.rows()<<"----"<<concurrency_probe_index<<std::endl;
    return res;
}

} // namespace DB
