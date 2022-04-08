#pragma once

#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Flash/Planner/PhysicalPlan.h>

namespace DB
{
/**
 * A physical plan node with a left and right child.
 */
class PhysicalBinary : public PhysicalPlan
{
public:
    PhysicalBinary(const String & executor_id_, const PlanType & type_, const NamesAndTypes & schema_)
        : PhysicalPlan(executor_id_, type_, schema_)
    {}

    PhysicalPlanPtr children(size_t i) const override
    {
        if (i > 1)
            throw TiFlashException(
                fmt::format("child_index({}) should not >= childrenSize({})", i, childrenSize()),
                Errors::Coprocessor::Internal);
        if (i == 0)
        {
            assert(left);
            return left;
        }
        else
        {
            assert(right);
            return right;
        }
    }

    void setChild(size_t i, const PhysicalPlanPtr & new_child) override
    {
        if (i > 1)
            throw TiFlashException(
                fmt::format("child_index({}) should not >= childrenSize({})", i, childrenSize()),
                Errors::Coprocessor::Internal);
        assert(new_child);
        assert(new_child.get() != this);
        if (i == 0)
        {
            left = new_child;
        }
        else
        {
            right = new_child;
        }
    }

    void appendChild(const PhysicalPlanPtr & new_child) override
    {
        assert(new_child);
        assert(new_child.get() != this);
        if (!left)
        {
            assert(!right);
            left = new_child;
        }
        else
        {
            if (right)
                throw TiFlashException(
                    fmt::format("the actual children size had be the max size({}), don't append child again", childrenSize()),
                    Errors::Coprocessor::Internal);
            right = new_child;
        }
    }

    size_t childrenSize() const override { return 2; };

protected:
    PhysicalPlanPtr left;
    PhysicalPlanPtr right;
};
} // namespace DB