#pragma once

#include <Flash/Planner/plans/PhysicalUnary.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalAggregation : public PhysicalUnary
{
public:
    static PhysicalPlanPtr build(
        const Context & context,
        const String & executor_id,
        const tipb::Aggregation & aggregation,
        PhysicalPlanPtr child);

    PhysicalAggregation(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const ExpressionActionsPtr & before_agg_actions_,
        const Names & aggregation_keys_,
        const TiDB::TiDBCollators & aggregation_collators_,
        bool is_final_agg_,
        const AggregateDescriptions & aggregate_descriptions_,
        const ExpressionActionsPtr & castAfterAgg_)
        : PhysicalUnary(executor_id_, PlanType::Aggregation, schema_)
        , before_agg_actions(before_agg_actions_)
        , aggregation_keys(aggregation_keys_)
        , aggregation_collators(aggregation_collators_)
        , is_final_agg(is_final_agg_)
        , aggregate_descriptions(aggregate_descriptions_)
        , cast_after_agg(castAfterAgg_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams) override;

    ExpressionActionsPtr before_agg_actions;
    Names aggregation_keys;
    TiDB::TiDBCollators aggregation_collators;
    bool is_final_agg;
    AggregateDescriptions aggregate_descriptions;
    ExpressionActionsPtr cast_after_agg;
};
} // namespace DB