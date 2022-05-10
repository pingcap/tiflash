//
// Created by root on 5/10/22.
//

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

void optimize(const Context & context, PhysicalPlanPtr plan)
{
    assert(plan);
    static std::vector<RulePtr> rules{FinalizeRule::create()};
    for (const auto & rule : rules)
        plan = rule->apply(context, plan);
}
} // namespace DB