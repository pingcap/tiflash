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
        const Names & schema_,
        const ExpressionActionsPtr & project_actions_)
        : PhysicalPlan(executor_id_, PlanType::Projection, schema_)
        , project_actions(project_actions_)
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

    bool isOnlyProject() const;

private:
    PhysicalPlanPtr child;
    ExpressionActionsPtr project_actions;
};
} // namespace DB