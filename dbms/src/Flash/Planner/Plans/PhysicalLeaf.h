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

#include <Common/TiFlashException.h>
#include <Flash/Planner/PhysicalPlanNode.h>

namespace DB
{
/**
 * A physical plan node with no children.
 */
class PhysicalLeaf : public PhysicalPlanNode
{
public:
    PhysicalLeaf(
        const String & executor_id_,
        const PlanType & type_,
        const NamesAndTypes & schema_,
        const FineGrainedShuffle & fine_grained_shuffle_,
        const String & req_id)
        : PhysicalPlanNode(executor_id_, type_, schema_, fine_grained_shuffle_, req_id)
    {}

    PhysicalPlanNodePtr children(size_t) const override
    {
        throw TiFlashException("the children size of PhysicalLeaf is zero", Errors::Planner::Internal);
    }

    size_t childrenSize() const override { return 0; };
};
} // namespace DB