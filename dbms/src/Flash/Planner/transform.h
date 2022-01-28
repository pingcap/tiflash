#pragma once

#include <Flash/Planner/PhysicalPlan.h>

namespace DB
{
/// f: (const PhysicalPlanPtr &) -> PhysicalPlanPtr
template <typename FF>
PhysicalPlanPtr transformDown(PhysicalPlanPtr plan, FF && f)
{
    plan = f(plan);
    for (size_t i = 0; i < plan->childrenSize(); ++i)
    {
        plan->setChild(i, transformDown(plan->children(i), std::forward<FF>(f)));
    }
    return plan;
}

/// f: (const PhysicalPlanPtr &) -> PhysicalPlanPtr
template <typename FF>
PhysicalPlanPtr transformUp(PhysicalPlanPtr plan, FF && f)
{
    std::vector<PhysicalPlanPtr> after_children;
    after_children.reserve(plan->childrenSize());
    for (size_t i = 0; i < plan->childrenSize(); ++i)
    {
        after_children.push_back(transformDown(plan->children(i), std::forward<FF>(f)));
    }
    plan = f(plan);
    for (size_t i = 0; i < plan->childrenSize(); ++i)
    {
        plan->setChild(i, after_children[i]);
    }
    return plan;
}
} // namespace DB