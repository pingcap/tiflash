#pragma once

#include <Flash/Planner/PhysicalPlan.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
class PhysicalAggregation : public PhysicalPlan
{
public:
    PhysicalAggregation(
        const String & executor_id_,
        const Names & schema_,
        const ExpressionActionsPtr & before_agg_actions_,
        const Names & aggregation_keys_,
        const TiDB::TiDBCollators & aggregation_collators_,
        const AggregateDescriptions & aggregate_descriptions_,
        const ExpressionActionsPtr & castAfterAgg_)
        : PhysicalPlan(executor_id_, PlanType::Aggregation, schema_)
        , before_agg_actions(before_agg_actions_)
        , aggregation_keys(aggregation_keys_)
        , aggregation_collators(aggregation_collators_)
        , aggregate_descriptions(aggregate_descriptions_)
        , cast_after_agg(castAfterAgg_)
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

    ExpressionActionsPtr before_agg_actions;
    Names aggregation_keys;
    TiDB::TiDBCollators aggregation_collators;
    AggregateDescriptions aggregate_descriptions;
    ExpressionActionsPtr cast_after_agg;
};
} // namespace DB