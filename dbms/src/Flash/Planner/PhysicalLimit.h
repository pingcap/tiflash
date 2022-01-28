#pragma once

#include <Flash/Planner/PhysicalPlan.h>

namespace DB
{
class PhysicalLimit : public PhysicalPlan
{
public:
    PhysicalLimit(
        const String & executor_id_,
        const Names & schema_,
        size_t limit_)
        : PhysicalPlan(executor_id_, PlanType::Limit, schema_)
        , limit(limit_)
    {}

    PhysicalPlanPtr children(size_t) const override
    {
        return child;
    }

    void setChild(size_t i, const PhysicalPlanPtr & new_child) override
    {
        assert(i == 0);
        child = new_child;
    }

    void appendChild(const PhysicalPlanPtr & new_child) override
    {
        assert(!child);
        child = new_child;
    }

    size_t childrenSize() const override { return 1; };

    void transform(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    bool finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    PhysicalPlanPtr child;
    size_t limit;
};
} // namespace DB