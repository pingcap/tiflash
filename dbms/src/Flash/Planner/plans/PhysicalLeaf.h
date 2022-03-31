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
    PhysicalLeaf(const String & executor_id_, const PlanType & type_, const NamesAndTypes & schema_)
        : PhysicalPlan(executor_id_, type_, schema_)
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