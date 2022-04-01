#pragma once

#include <Flash/Planner/plans/PhysicalUnary.h>
#include <Interpreters/ExpressionActions.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalFilter : public PhysicalUnary
{
public:
    static PhysicalPlanPtr build(
        const Context & context,
        const String & executor_id,
        const tipb::Selection & selection,
        PhysicalPlanPtr child);

    PhysicalFilter(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const String & filter_column_,
        const ExpressionActionsPtr & before_filter_actions_)
        : PhysicalUnary(executor_id_, PlanType::Selection, schema_)
        , filter_column(filter_column_)
        , before_filter_actions(before_filter_actions_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, Context & context, size_t max_streams) override;

    String filter_column;
    ExpressionActionsPtr before_filter_actions;
};
} // namespace DB