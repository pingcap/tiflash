#pragma once

#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
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
        if (i != 0)
            throw TiFlashException(
                fmt::format("child_index({}) should not >= childrenSize({})", i, childrenSize()),
                Errors::Coprocessor::Internal);
        assert(child);
        return child;
    }

    void setChild(size_t i, const PhysicalPlanPtr & new_child) override
    {
        if (i != 0)
            throw TiFlashException(
                fmt::format("child_index({}) should not >= childrenSize({})", i, childrenSize()),
                Errors::Coprocessor::Internal);
        assert(new_child);
        assert(new_child.get() != this);
        child = new_child;
    }

    void appendChild(const PhysicalPlanPtr & new_child) override
    {
        if (child)
            throw TiFlashException(
                fmt::format("the actual children size had be the max size({}), don't append child again", childrenSize()),
                Errors::Coprocessor::Internal);
        assert(new_child);
        assert(new_child.get() != this);
        child = new_child;
    }

    size_t childrenSize() const override { return 1; };

protected:
    PhysicalPlanPtr child;
};
} // namespace DB