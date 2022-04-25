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

#include <Common/Exception.h>
#include <Flash/Planner/PhysicalPlan.h>

namespace DB
{
/**
 * A physical plan node with no children.
 */
class PhysicalLeaf : public PhysicalPlan
{
public:
    PhysicalLeaf(const String & executor_id_, const PlanType & type_, const NamesAndTypes & schema_, const String & req_id)
        : PhysicalPlan(executor_id_, type_, schema_, req_id)
    {}

    PhysicalPlanPtr children(size_t) const override
    {
        throw Exception("the children size of PhysicalLeaf is zero");
    }

    void setChild(size_t, const PhysicalPlanPtr &) override
    {
        throw Exception("the children size of PhysicalLeaf is zero");
    }

    void appendChild(const PhysicalPlanPtr &) override
    {
        throw Exception("the children size of PhysicalLeaf is zero");
    }

    size_t childrenSize() const override { return 0; };
};
} // namespace DB