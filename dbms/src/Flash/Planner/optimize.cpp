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

#include <Flash/Planner/Plans/PhysicalAggregation.h>
#include <Flash/Planner/Plans/PhysicalTiCIScan.h>
#include <Flash/Planner/optimize.h>
#include <Interpreters/Context.h>

namespace DB
{
class Rule
{
public:
    virtual PhysicalPlanNodePtr apply(const Context & context, PhysicalPlanNodePtr plan, const LoggerPtr & log) = 0;

    virtual ~Rule() = default;
};
using RulePtr = std::shared_ptr<Rule>;

class FinalizeRule : public Rule
{
public:
    PhysicalPlanNodePtr apply(const Context &, PhysicalPlanNodePtr plan, const LoggerPtr &) override
    {
        plan->finalize(toNames(plan->getSchema()));
        return plan;
    }

    static RulePtr create() { return std::make_shared<FinalizeRule>(); }
};

class TiCICountAggOptimizeRule : public Rule
{
public:
    PhysicalPlanNodePtr apply(const Context & context, PhysicalPlanNodePtr plan, const LoggerPtr & logger) override
    {
        if (plan->tp() == PlanType::Aggregation)
        {
            auto agg = std::static_pointer_cast<PhysicalAggregation>(plan);
            if (agg->isCountNotNullableColumnWithoutGroupbyKey())
            {
                auto child = agg->children(0);
                if (child->tp() == PlanType::TiCiScan)
                {
                    auto tici_scan = std::static_pointer_cast<PhysicalTiCIScan>(child);
                    tici_scan->setIsCountAgg(true);
                }
            }
        }
        if (plan->childrenSize() != 0)
        {
            for (size_t i = 0; i < plan->childrenSize(); ++i)
            {
                apply(context, plan->children(i), logger);
            }
        }

        return plan;
    }

    static RulePtr create() { return std::make_shared<TiCICountAggOptimizeRule>(); }
};

PhysicalPlanNodePtr optimize(const Context & context, PhysicalPlanNodePtr plan, const LoggerPtr & log)
{
    RUNTIME_CHECK(plan);
    static std::vector<RulePtr> rules{TiCICountAggOptimizeRule::create(), FinalizeRule::create()};
    for (const auto & rule : rules)
    {
        plan = rule->apply(context, plan, log);
        RUNTIME_CHECK(plan);
    }
    return plan;
}
} // namespace DB
