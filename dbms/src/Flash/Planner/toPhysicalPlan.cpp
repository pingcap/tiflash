#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Planner/toPhysicalPlan.h>

namespace DB
{
std::shared_ptr<PhysicalAggregation> ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const tipb::Aggregation & aggregation)
{
    return nullptr;
}

std::shared_ptr<PhysicalFilter> ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const tipb::Selection & selection)
{
    return nullptr;
}

std::shared_ptr<PhysicalLimit> ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const tipb::Limit & limit)
{
    return std::make_shared<PhysicalLimit>(executor_id, schema, limit.limit());
}

std::shared_ptr<PhysicalProjection> ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const tipb::Projection & projection)
{
    return nullptr;
}

std::shared_ptr<PhysicalTopN> ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const tipb::TopN & top_n)
{
    return nullptr;
}

std::shared_ptr<PhysicalSource> ToPhysicalPlanBuilder::toPhysicalPlan(const String & executor_id, const Names & source_schema, const Block & source_sample_block)
{
    assert(schema.empty());
    auto plan = std::make_shared<PhysicalSource>(executor_id, source_schema, source_sample_block);
    schema = source_schema;
    return plan;
}
} // namespace DB