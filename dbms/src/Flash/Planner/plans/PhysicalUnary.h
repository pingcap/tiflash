#pragma once

#include <Common/Exception.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

namespace DB
{
/**
 * A physical plan node with single child.
 */
class PhysicalUnary : public PhysicalPlan
{
public:
    PhysicalUnary(const String & executor_id_, const PlanType & type_, const NamesAndTypes & schema_, const String & req_id)
        : PhysicalPlan(executor_id_, type_, schema_, req_id)
    {}

    PhysicalPlanPtr children(size_t i) const override
    {
        RUNTIME_ASSERT(i == 0, log, fmt::format("child_index({}) should not >= childrenSize({})", i, childrenSize()));
        assert(child);
        return child;
    }

    void setChild(size_t i, const PhysicalPlanPtr & new_child) override
    {
        RUNTIME_ASSERT(i == 0, log, fmt::format("child_index({}) should not >= childrenSize({})", i, childrenSize()));
        assert(new_child);
        assert(new_child.get() != this);
        child = new_child;
    }

    void appendChild(const PhysicalPlanPtr & new_child) override
    {
        RUNTIME_ASSERT(!child, log, fmt::format("the actual children size had be the max size({}), don't append child again", childrenSize()));
        assert(new_child);
        assert(new_child.get() != this);
        child = new_child;
    }

    size_t childrenSize() const override { return 1; };

protected:
    PhysicalPlanPtr child;
};
} // namespace DB
