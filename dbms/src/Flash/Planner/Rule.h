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

#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/plans/PhysicalProjection.h>
#include <Interpreters/Context.h>

#include <memory>

namespace DB
{
class Rule
{
public:
    virtual PhysicalPlanPtr apply(const Context & context, PhysicalPlanPtr plan) = 0;

    virtual ~Rule() = default;
};
using RulePtr = std::shared_ptr<Rule>;

class FinalizeRule : public Rule
{
public:
    PhysicalPlanPtr apply(const Context &, PhysicalPlanPtr plan) override
    {
        plan->finalize();
        return plan;
    }
};

class AddFinalProjectionForNonMpp : public Rule
{
public:
    PhysicalPlanPtr apply(const Context & context, PhysicalPlanPtr plan) override
    {
        if (plan->tp() != PlanType::ExchangeSender)
        {
            const auto & dag_context = *context.getDAGContext();
            return PhysicalProjection::buildRootFinal(
                context,
                dag_context.outputFieldTypes(),
                dag_context.outputOffsets(),
                plan->execId(),
                dag_context.keep_session_timezone_info,
                plan);
        }
        return plan;
    }
};

void optimize(const Context & context, PhysicalPlanPtr plan)
{
    static std::vector<RulePtr> rules{std::make_shared<AddFinalProjectionForNonMpp>(), std::make_shared<FinalizeRule>()};
    for (const auto & rule : rules)
        plan = rule->apply(context, plan);
}
} // namespace DB