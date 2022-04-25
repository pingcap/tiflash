#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Flash/Planner/PhysicalPlanBuilder.h>
#include <Flash/Planner/plans/PhysicalAggregation.h>
#include <Flash/Planner/plans/PhysicalExchangeReceiver.h>
#include <Flash/Planner/plans/PhysicalExchangeSender.h>
#include <Flash/Planner/plans/PhysicalFilter.h>
#include <Flash/Planner/plans/PhysicalJoin.h>
#include <Flash/Planner/plans/PhysicalLimit.h>
#include <Flash/Planner/plans/PhysicalProjection.h>
#include <Flash/Planner/plans/PhysicalSource.h>
#include <Flash/Planner/plans/PhysicalTableScan.h>
#include <Flash/Planner/plans/PhysicalTopN.h>

namespace DB
{
namespace
{
PhysicalPlanPtr popBack(std::vector<PhysicalPlanPtr> vec)
{
    assert(!vec.empty());
    PhysicalPlanPtr back = vec.back();
    vec.pop_back();
    return back;
}
} // namespace

void PhysicalPlanBuilder::buildSource(const Block & sample_block)
{
    cur_plans.push_back(PhysicalSource::build(source_sample_block, log.identifier()));
}
} // namespace DB
