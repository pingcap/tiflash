#pragma once

#include <Core/Block.h>
#include <Flash/Coprocessor/DAGContext.h>
#include <Flash/Coprocessor/DAGSet.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Planner/plans/PhysicalAggregation.h>
#include <Flash/Planner/plans/PhysicalFilter.h>
#include <Flash/Planner/plans/PhysicalLimit.h>
#include <Flash/Planner/PhysicalPlan.h>
#include <Flash/Planner/plans/PhysicalProjection.h>
#include <Flash/Planner/plans/PhysicalSource.h>
#include <Flash/Planner/plans/PhysicalTopN.h>
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

    void toPhysicalPlan(const String & executor_id, const tipb::Aggregation & aggregation);
    void toPhysicalPlan(const String & executor_id, const tipb::Selection & selection);
    void toPhysicalPlan(const String & executor_id, const tipb::Limit & limit);
    void toPhysicalPlan(const String & executor_id, const tipb::Projection & projection);
    void toPhysicalPlan(const String & executor_id, const tipb::TopN & top_n);
    void toPhysicalPlan(const String & executor_id, const NamesAndTypes & source_schema, const Block & source_sample_block);

    void appendNonRootFinalProjection(const String & column_prefix);
    void appendRootFinalProjection(
        const std::vector<tipb::FieldType> & require_schema,
        const std::vector<Int32> & output_offsets,
        const String & column_prefix,
        bool keep_session_timezone_info);
private:
    ExpressionActionsPtr newActionsForNewPlan();

    PhysicalPlanPtr cur_plan;

    Context & context;
    Settings settings;
    NamesAndTypes schema;
};
} // namespace DB