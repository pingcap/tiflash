#include <Common/FmtUtils.h>
#include <Common/TiFlashException.h>
#include <Flash/Planner/PhysicalPlanBuilder.h>
#include <Flash/Planner/plans/PhysicalAggregation.h>
#include <Flash/Planner/plans/PhysicalExchangeSender.h>
#include <Flash/Planner/plans/PhysicalFilter.h>
#include <Flash/Planner/plans/PhysicalLimit.h>
#include <Flash/Planner/plans/PhysicalProjection.h>
#include <Flash/Planner/plans/PhysicalSource.h>
#include <Flash/Planner/plans/PhysicalTopN.h>

namespace DB
{
void PhysicalPlanBuilder::build(const String & executor_id, const tipb::Executor * executor)
{
    assert(executor);
    assert(cur_plan);
    switch (executor->tp())
    {
    case tipb::ExecType::TypeSelection:
        cur_plan = PhysicalFilter::build(context, executor_id, executor->selection(), cur_plan);
    case tipb::ExecType::TypeAggregation:
    case tipb::ExecType::TypeStreamAgg:
        cur_plan = PhysicalAggregation::build(context, executor_id, executor->aggregation(), cur_plan);
    case tipb::ExecType::TypeTopN:
        cur_plan = PhysicalTopN::build(context, executor_id, executor->topn(), cur_plan);
    case tipb::ExecType::TypeLimit:
        cur_plan = PhysicalLimit::build(executor_id, executor->limit(), cur_plan);
    case tipb::ExecType::TypeExchangeSender:
        cur_plan = PhysicalExchangeSender::build(executor_id, executor->exchange_sender(), cur_plan);
    default:
        throw TiFlashException(fmt::format("{} executor is not supported", executor->tp()), Errors::Coprocessor::Unimplemented);
    }
}

void PhysicalPlanBuilder::buildSource(
    const String & executor_id,
    const NamesAndTypes & source_schema,
    const Block & source_sample_block)
{
    assert(!cur_plan);
    cur_plan = PhysicalSource::build(executor_id, source_schema, source_sample_block);
}

void PhysicalPlanBuilder::buildNonRootFinalProjection(const String & column_prefix)
{
    assert(cur_plan);
    cur_plan = PhysicalProjection::buildNonRootFinal(context, column_prefix, cur_plan);
}

void PhysicalPlanBuilder::buildRootFinalProjection(
    const std::vector<tipb::FieldType> & require_schema,
    const std::vector<Int32> & output_offsets,
    const String & column_prefix,
    bool keep_session_timezone_info)
{
    assert(cur_plan);
    cur_plan = PhysicalProjection::buildRootFinal(
        context,
        require_schema,
        output_offsets,
        column_prefix,
        keep_session_timezone_info,
        cur_plan);
}
} // namespace DB