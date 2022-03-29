#pragma once

#include <Flash/Planner/PhysicalPlan.h>

namespace DB
{
/**
 * A physical plan node with single child.
 */
class PhysicalUnary : public PhysicalPlan
{
public:
    PhysicalUnary(const String & executor_id_, const PlanType & type_, const NamesAndTypes & schema_)
        : PhysicalPlan(executor_id_, type_, schema_)
    {}

    PhysicalPlanPtr children(size_t i) const override
    {
        assert(i == 0);
        assert(child);
        return child;
    }

    void setChild(size_t i, const PhysicalPlanPtr & new_child) override
    {
        assert(i == 0);
        assert(new_child);
        child = new_child;
    }

    void appendChild(const PhysicalPlanPtr & new_child) override
    {
        assert(!child);
        assert(new_child);
        child = new_child;
    }

    size_t childrenSize() const override { return 1; };

protected:
    PhysicalPlanPtr child;
};
} // namespace DB