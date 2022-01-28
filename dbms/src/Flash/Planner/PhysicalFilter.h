#pragma once

#include <Flash/Planner/PhysicalPlan.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
class PhysicalFilter : public PhysicalPlan
{
public:
    PhysicalFilter(
        const String & executor_id_,
        const Names & schema_,
        const String & filter_column_,
        const ExpressionActionsPtr & before_filter_actions_)
        : PhysicalPlan(executor_id_, PlanType::Selection, schema_)
        , filter_column(filter_column_)
        , before_filter_actions(before_filter_actions_)
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
    String filter_column;
    ExpressionActionsPtr before_filter_actions;
};
} // namespace DB