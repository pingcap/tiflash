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

    return res;
}

} // namespace DB
