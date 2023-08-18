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

#pragma once

#include <Common/Exception.h>
#include <Flash/Planner/PhysicalPlanNode.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{
/**
 * A physical plan node with single child.
 */
class PhysicalUnary : public PhysicalPlanNode
{
public:
    PhysicalUnary(
        const String & executor_id_,
        const PlanType & type_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id,
        const PhysicalPlanNodePtr & child_)
        : PhysicalPlanNode(executor_id_, type_, schema_, fine_grained_shuffle_, req_id)
        , child(child_)
    {
        RUNTIME_ASSERT(child, log, "children(0) shouldn't be nullptr");
    }

    PhysicalPlanNodePtr children(size_t i) const override
    {
        RUNTIME_ASSERT(i == 0, log, "child_index({}) shouldn't >= childrenSize({})", i, childrenSize());
        return child;
    }

    size_t childrenSize() const override { return 1; };

protected:
    PhysicalPlanNodePtr child;
};
} // namespace DB
