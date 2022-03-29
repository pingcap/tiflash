#pragma once

#include <Flash/Planner/PhysicalPlan.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
class PhysicalProjection : public PhysicalPlan
{
public:
    PhysicalProjection(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const ExpressionActionsPtr & project_actions_)
        : PhysicalPlan(executor_id_, PlanType::Projection, schema_)
        , project_actions(project_actions_)
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
        child = new_child;
    }

    void appendChild(const PhysicalPlanPtr & new_child) override
    {
        assert(!child);
        assert(new_child);
        child = new_child;
    }

    size_t childrenSize() const override { return 1; };

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams) override;

    PhysicalPlanPtr child;
    ExpressionActionsPtr project_actions;
};
} // namespace DB