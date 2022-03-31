#pragma once

#include <Core/SortDescription.h>
#include <Flash/Planner/plans/PhysicalUnary.h>
#include <Interpreters/ExpressionActions.h>
#include <tipb/executor.pb.h>

namespace DB
{
class PhysicalTopN : public PhysicalUnary
{
public:
    static PhysicalPlanPtr build(
        const Context & context,
        const String & executor_id,
        const tipb::TopN & top_n,
        PhysicalPlanPtr child);

    PhysicalTopN(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const SortDescription & order_descr_,
        const ExpressionActionsPtr & before_sort_actions_,
        size_t limit_)
        : PhysicalUnary(executor_id_, PlanType::TopN, schema_)
        , order_descr(order_descr_)
        , before_sort_actions(before_sort_actions_)
        , limit(limit_)
    {}

    void finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    void transformImpl(DAGPipeline & pipeline, const Context & context, size_t max_streams) override;

    SortDescription order_descr;
    ExpressionActionsPtr before_sort_actions;
    size_t limit;
};
} // namespace DB