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
void PhysicalPlanBuilder::build(const String & executor_id, const tipb::Executor * executor)
{
    assert(executor);
    switch (executor->tp())
    {
    case tipb::ExecType::TypeJoin:
    {
        auto right = popBack(cur_plans);
        auto left = popBack(cur_plans);
        cur_plans.push_back(PhysicalJoin::build(context, executor_id, executor->join(), left, right));
        break;
    }
    case tipb::ExecType::TypeSelection:
        cur_plans.push_back(PhysicalFilter::build(context, executor_id, executor->selection(), popBack(cur_plans)));
        break;
    case tipb::ExecType::TypeAggregation:
    case tipb::ExecType::TypeStreamAgg:
        cur_plans.push_back(PhysicalAggregation::build(context, executor_id, executor->aggregation(), popBack(cur_plans)));
        break;
    case tipb::ExecType::TypeTopN:
        cur_plans.push_back(PhysicalTopN::build(context, executor_id, executor->topn(), popBack(cur_plans)));
        break;
    case tipb::ExecType::TypeLimit:
        cur_plans.push_back(PhysicalLimit::build(executor_id, executor->limit(), popBack(cur_plans)));
        break;
    case tipb::ExecType::TypeProjection:
        cur_plans.push_back(PhysicalProjection::build(context, executor_id, executor->projection(), popBack(cur_plans)));
        break;
    case tipb::ExecType::TypeExchangeSender:
        cur_plans.push_back(PhysicalExchangeSender::build(executor_id, executor->exchange_sender(), popBack(cur_plans)));
        break;
    case tipb::ExecType::TypeExchangeReceiver:
        cur_plans.push_back(PhysicalExchangeReceiver::build(context, executor_id));
        break;
    case tipb::ExecType::TypeTableScan:
    case tipb::ExecType::TypePartitionTableScan:
        cur_plans.push_back(PhysicalTableScan::build(context, executor, executor_id));
        break;
    default:
        throw TiFlashException(fmt::format("{} executor is not supported", executor->tp()), Errors::Coprocessor::Unimplemented);
    }
}

void PhysicalPlanBuilder::buildSource(
    const String & executor_id,
    const NamesAndTypes & source_schema,
    const Block & source_sample_block)
{
    cur_plans.push_back(PhysicalSource::build(executor_id, source_schema, source_sample_block));
}

void PhysicalPlanBuilder::buildNonRootFinalProjection(const String & column_prefix)
{
    cur_plans.push_back(PhysicalProjection::buildNonRootFinal(context, column_prefix, popBack(cur_plans)));
}

void PhysicalPlanBuilder::buildRootFinalProjection(
    const std::vector<tipb::FieldType> & require_schema,
    const std::vector<Int32> & output_offsets,
    const String & column_prefix,
    bool keep_session_timezone_info)
{
    cur_plans.push_back(PhysicalProjection::buildRootFinal(
        context,
        require_schema,
        output_offsets,
        column_prefix,
        keep_session_timezone_info,
        popBack(cur_plans)));
}
} // namespace DB