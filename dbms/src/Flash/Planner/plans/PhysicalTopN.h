#pragma once

#include <Common/LogWithPrefix.h>
#include <DataStreams/MergeSortingBlockInputStream.h>
#include <DataStreams/PartialSortingBlockInputStream.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Flash/Coprocessor/InterpreterUtils.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
class PhysicalTopN : public PhysicalPlan
{
public:
    PhysicalTopN(
        const String & executor_id_,
        const NamesAndTypes & schema_,
        const SortDescription & order_descr_,
        const ExpressionActionsPtr & before_sort_actions_,
        size_t limit_)
        : PhysicalPlan(executor_id_, PlanType::TopN, schema_)
        , order_descr(order_descr_)
        , before_sort_actions(before_sort_actions_)
        , limit(limit_)
    {}

    PhysicalPlanPtr children(size_t) const override
    {
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

    void transform(DAGPipeline & pipeline, const Context & context, size_t max_streams) override;

    bool finalize(const Names & parent_require) override;

    const Block & getSampleBlock() const override;

private:
    PhysicalPlanPtr child;
    SortDescription order_descr;
    ExpressionActionsPtr before_sort_actions;
    size_t limit;
};
} // namespace DB