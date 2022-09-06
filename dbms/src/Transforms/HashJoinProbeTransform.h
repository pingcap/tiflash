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

#pragma once

#include <Interpreters/ExpressionActions.h>
#include <Transforms/Transforms.h>

namespace DB
{
class HashJoinProbeTransform : public Transform
{
public:
    explicit HashJoinProbeTransform(
        const ExpressionActionsPtr & join_probe_actions_)
        : join_probe_actions(join_probe_actions_)
    {
        if (!join_probe_actions || join_probe_actions->getActions().size() != 1
            || join_probe_actions->getActions().back().type != ExpressionAction::Type::JOIN)
        {
            throw Exception("isn't valid join probe actions", ErrorCodes::LOGICAL_ERROR);
        }
    }

    bool transform(Block & block) override
    {
        if (block)
            join_probe_actions->execute(block);
        return true;
    }

private:
    ExpressionActionsPtr join_probe_actions;
};
} // namespace DB
