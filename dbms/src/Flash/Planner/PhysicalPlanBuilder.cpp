#include <Flash/Planner/PhysicalPlanBuilder.h>
#include <Flash/Planner/plans/PhysicalSource.h>

namespace DB
{
void PhysicalPlanBuilder::buildSource(const Block & sample_block)
{
    cur_plans.push_back(PhysicalSource::build(source_sample_block, log.identifier()));
}
} // namespace DB
