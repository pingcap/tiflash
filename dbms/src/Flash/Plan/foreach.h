#pragma once

#include <Flash/Plan/Plan.h>

namespace DB
{
template <typename FF>
void foreachUp(const PlanPtr & plan, FF && ff)
{
    for (size_t i = 0; i < plan->childrenSize(); ++i)
    {
        foreachUp(plan->children(i), std::forward<FF>(ff));
    }
    ff(plan);
}

template <typename FF>
void foreachDown(const PlanPtr & plan, FF && ff)
{
    ff(plan);
    for (size_t i = 0; i < plan->childrenSize(); ++i)
    {
        foreachDown(plan->children(i), std::forward<FF>(ff));
    }
}
} // namespace DB