#pragma once

#include <Core/Block.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGSet.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Planner/PhysicalAggregation.h>
#include <Flash/Planner/PhysicalFilter.h>
#include <Flash/Planner/PhysicalLimit.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/PhysicalProjection.h>
#include <Flash/Planner/PhysicalSource.h>
#include <Flash/Planner/PhysicalTopN.h>
#include <Flash/Planner/toPhysicalPlan.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Transaction/TMTStorages.h>
#include <tipb/executor.pb.h>

namespace DB
{
class ToPhysicalPlanBuilder
{
public:
    ToPhysicalPlanBuilder(Context & context_)
        : context(context_)
        , settings(context.getSettingsRef())
    {}

    std::shared_ptr<PhysicalAggregation> toPhysicalPlan(const String & executor_id, const tipb::Aggregation & aggregation);
    std::shared_ptr<PhysicalFilter> toPhysicalPlan(const String & executor_id, const tipb::Selection & selection);
    std::shared_ptr<PhysicalLimit> toPhysicalPlan(const String & executor_id, const tipb::Limit & limit);
    std::shared_ptr<PhysicalProjection> toPhysicalPlan(const String & executor_id, const tipb::Projection & projection);
    std::shared_ptr<PhysicalTopN> toPhysicalPlan(const String & executor_id, const tipb::TopN & top_n);
    std::shared_ptr<PhysicalSource> toPhysicalPlan(const String & executor_id, const Names & source_schema, const Block & source_sample_block);

private:
    Context & context;
    Settings settings;
    Names schema;
};
} // namespace DB