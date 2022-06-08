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

#include <Flash/Planner/optimize.h>
#include <Interpreters/Context.h>

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

    static RulePtr create() { return std::make_shared<FinalizeRule>(); }
};

PhysicalPlanPtr optimize(const Context & context, PhysicalPlanPtr plan)
{
    assert(plan);
    static std::vector<RulePtr> rules{FinalizeRule::create()};
    for (const auto & rule : rules)
        plan = rule->apply(context, plan);
    return plan;
}
} // namespace DB
